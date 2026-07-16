package storage

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// newTestStorage returns a SecretStorage backed by an in-memory fake
// kubernetes clientset, so tests exercise the real storage logic without a
// cluster.
func newTestStorage(t *testing.T) *SecretStorage {
	t.Helper()
	return &SecretStorage{
		Namespace:  "caddy-system",
		LeaseID:    "test-instance",
		kubeClient: fake.NewClientset(),
		logger:     zaptest.NewLogger(t),
	}
}

// newQuietTestStorage is for tests that spawn the background lock-refresh
// goroutine (Lock/TryLock success paths): the goroutine may log after the
// test returns, which the zaptest logger treats as a fatal error, so these
// tests use a no-op logger instead.
func newQuietTestStorage(t *testing.T) *SecretStorage {
	t.Helper()
	s := newTestStorage(t)
	s.logger = zap.NewNop()
	return s
}

func TestStoreLoadRoundtrip(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	key := "certificates/acme-v02.api.letsencrypt.org-directory/example.com/example.com.crt"

	if err := s.Store(ctx, key, []byte("first")); err != nil {
		t.Fatalf("Store: %v", err)
	}
	got, err := s.Load(ctx, key)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != "first" {
		t.Fatalf("Load = %q, want %q", got, "first")
	}

	// Storing the same key again must overwrite, not fail.
	if err := s.Store(ctx, key, []byte("second")); err != nil {
		t.Fatalf("Store (update): %v", err)
	}
	got, err = s.Load(ctx, key)
	if err != nil {
		t.Fatalf("Load after update: %v", err)
	}
	if string(got) != "second" {
		t.Fatalf("Load after update = %q, want %q", got, "second")
	}
}

func TestLoadMissingKeyIsErrNotExist(t *testing.T) {
	s := newTestStorage(t)
	_, err := s.Load(context.Background(), "certificates/missing/missing.crt")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Load of missing key: err = %v, want fs.ErrNotExist", err)
	}
}

func TestExists(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	key := "certificates/issuer/example.com/example.com.json"

	if s.Exists(ctx, key) {
		t.Fatal("Exists = true before Store")
	}
	if err := s.Store(ctx, key, []byte("meta")); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if !s.Exists(ctx, key) {
		t.Fatal("Exists = false after Store")
	}
}

func TestDelete(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	key := "certificates/issuer/example.com/example.com.key"

	if err := s.Store(ctx, key, []byte("key")); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Load(ctx, key); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Load after delete: err = %v, want fs.ErrNotExist", err)
	}
}

func TestListByPrefix(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	keys := []string{
		"certificates/issuer/a.example.com/a.example.com.crt",
		"certificates/issuer/a.example.com/a.example.com.key",
		"acme/issuer/users/user@example.com/user@example.com.json",
	}
	for _, k := range keys {
		if err := s.Store(ctx, k, []byte("v")); err != nil {
			t.Fatalf("Store(%q): %v", k, err)
		}
	}

	got, err := s.List(ctx, "certificates", true)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List(certificates) returned %d keys (%v), want 2", len(got), got)
	}
}

// otherHolderLease returns a lease for leaseName held by a different
// instance, whose renewTime makes it expired or not as requested.
func otherHolderLease(leaseName, namespace string, expired bool) *coordinationv1.Lease {
	renewTime := metav1.NewMicroTime(time.Now())
	if expired {
		renewTime = metav1.NewMicroTime(time.Now().Add(-time.Minute))
	}
	holder := "other-instance"
	seconds := int32(5)
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: leaseName, Namespace: namespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &seconds,
			AcquireTime:          &renewTime,
			RenewTime:            &renewTime,
		},
	}
}

func TestTryAcquireCreatesLease(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	if _, err := s.tryAcquireOrRenew(ctx, "caddy-lock-issue.cert.example.com", false); err != nil {
		t.Fatalf("tryAcquireOrRenew on free lock: %v", err)
	}
	lease, err := s.kubeClient.CoordinationV1().Leases(s.Namespace).Get(ctx, "caddy-lock-issue.cert.example.com", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("lease not created: %v", err)
	}
	if *lease.Spec.HolderIdentity != s.LeaseID {
		t.Fatalf("lease holder = %q, want %q", *lease.Spec.HolderIdentity, s.LeaseID)
	}
}

func TestTryAcquireHeldLockReturnsErrLockHeld(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	leaseName := "caddy-lock-issue.cert.example.com"

	_, err := s.kubeClient.CoordinationV1().Leases(s.Namespace).Create(ctx, otherHolderLease(leaseName, s.Namespace, false), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating fixture lease: %v", err)
	}

	if _, err := s.tryAcquireOrRenew(ctx, leaseName, false); !errors.Is(err, errLockHeld) {
		t.Fatalf("tryAcquireOrRenew on held lock: err = %v, want errLockHeld", err)
	}
}

func TestTryAcquireTakesOverExpiredLease(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	leaseName := "caddy-lock-issue.cert.example.com"

	_, err := s.kubeClient.CoordinationV1().Leases(s.Namespace).Create(ctx, otherHolderLease(leaseName, s.Namespace, true), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating fixture lease: %v", err)
	}

	if _, err := s.tryAcquireOrRenew(ctx, leaseName, false); err != nil {
		t.Fatalf("tryAcquireOrRenew on expired lock: %v", err)
	}
	lease, err := s.kubeClient.CoordinationV1().Leases(s.Namespace).Get(ctx, leaseName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get lease: %v", err)
	}
	if *lease.Spec.HolderIdentity != s.LeaseID {
		t.Fatalf("lease holder after takeover = %q, want %q", *lease.Spec.HolderIdentity, s.LeaseID)
	}
}

func TestKeepLockUpdatedSurvivesTransientError(t *testing.T) {
	old := leaseRenewInterval
	leaseRenewInterval = 5 * time.Millisecond

	s := newTestStorage(t)
	ctx, cancel := context.WithCancel(context.Background())
	leaseName := "caddy-lock-issue.cert.example.com"

	if _, err := s.tryAcquireOrRenew(ctx, leaseName, false); err != nil {
		t.Fatalf("acquiring lock: %v", err)
	}

	// Fail the next single lease GET with a server error; afterwards the
	// refresh loop must keep renewing rather than exit.
	failures := 1
	fakeClient := s.kubeClient.(*fake.Clientset)
	fakeClient.PrependReactor("get", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if failures > 0 {
			failures--
			return true, nil, apierrors.NewInternalError(errors.New("transient"))
		}
		return false, nil, nil
	})

	refreshStopped := make(chan struct{})
	go func() {
		s.keepLockUpdated(ctx, leaseName)
		close(refreshStopped)
	}()
	defer func() {
		// The refresh goroutine must be stopped before the interval is
		// restored, or the write races with the goroutine's reads.
		cancel()
		<-refreshStopped
		leaseRenewInterval = old
	}()

	// Wait for several refresh intervals, then verify the lease renewTime
	// advanced past the acquisition time (the loop outlived the error). The
	// polling below uses List rather than Get so it does not trip the
	// injected reactor, which only the refresh loop should consume.
	deadline := time.Now().Add(2 * time.Second)
	acquired := time.Now()
	for time.Now().Before(deadline) {
		leases, err := s.kubeClient.CoordinationV1().Leases(s.Namespace).List(context.Background(), metav1.ListOptions{})
		if err == nil {
			for _, lease := range leases.Items {
				if lease.Name == leaseName && lease.Spec.RenewTime != nil && lease.Spec.RenewTime.After(acquired) {
					return // renewed after the transient failure
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("lease was never renewed after a transient error; refresh loop stopped")
}

func TestKeepLockUpdatedStopsWhenLockLost(t *testing.T) {
	old := leaseRenewInterval
	leaseRenewInterval = 5 * time.Millisecond

	s := newTestStorage(t)
	ctx, cancel := context.WithCancel(context.Background())
	leaseName := "caddy-lock-issue.cert.example.com"

	// The lease is held by another, unexpired instance: refresh must stop.
	_, err := s.kubeClient.CoordinationV1().Leases(s.Namespace).Create(ctx, otherHolderLease(leaseName, s.Namespace, false), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating fixture lease: %v", err)
	}

	done := make(chan struct{})
	go func() {
		s.keepLockUpdated(ctx, leaseName)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
		leaseRenewInterval = old
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("keepLockUpdated did not stop after losing the lock")
	}
}

func TestUnlockDeletesLeaseEvenWithCanceledContext(t *testing.T) {
	s := newTestStorage(t)
	key := "issue_cert_example.com"
	leaseName := cleanKey(key, leasePrefix)

	if _, err := s.tryAcquireOrRenew(context.Background(), leaseName, false); err != nil {
		t.Fatalf("acquiring lock: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the operation holding the lock was canceled
	if err := s.Unlock(ctx, key); err != nil {
		t.Fatalf("Unlock with canceled context: %v", err)
	}

	_, err := s.kubeClient.CoordinationV1().Leases(s.Namespace).Get(context.Background(), leaseName, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("lease still exists after Unlock: err = %v", err)
	}
}

func TestLockAcquiresFreeLock(t *testing.T) {
	s := newQuietTestStorage(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key := "issue_cert_example.com"

	if err := s.Lock(ctx, key); err != nil {
		t.Fatalf("Lock on free lock: %v", err)
	}
	if err := s.Unlock(ctx, key); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

func TestLockTimesOutOnHeldLock(t *testing.T) {
	old := lockAcquireTimeout
	lockAcquireTimeout = 0 // give up after the first failed attempt
	defer func() { lockAcquireTimeout = old }()

	s := newTestStorage(t)
	ctx := context.Background()
	key := "issue_cert_example.com"
	leaseName := cleanKey(key, leasePrefix)

	_, err := s.kubeClient.CoordinationV1().Leases(s.Namespace).Create(ctx, otherHolderLease(leaseName, s.Namespace, false), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating fixture lease: %v", err)
	}

	err = s.Lock(ctx, key)
	if !errors.Is(err, errLockHeld) {
		t.Fatalf("Lock on held lock: err = %v, want errLockHeld after timeout", err)
	}
}

func TestExistsReportsFalseButLogsOnAPIError(t *testing.T) {
	s := newTestStorage(t)
	fakeClient := s.kubeClient.(*fake.Clientset)
	fakeClient.PrependReactor("get", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("apiserver unavailable"))
	})

	if s.Exists(context.Background(), "certificates/issuer/example.com/example.com.crt") {
		t.Fatal("Exists = true when the API call failed")
	}
}

func TestStoreUpdatesWhenCreateConflicts(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	key := "certificates/issuer/example.com/example.com.crt"

	// Seed the secret out-of-band so Store's Create hits AlreadyExists.
	if err := s.Store(ctx, key, []byte("old")); err != nil {
		t.Fatalf("seed Store: %v", err)
	}
	if err := s.Store(ctx, key, []byte("new")); err != nil {
		t.Fatalf("Store over existing secret: %v", err)
	}
	got, err := s.Load(ctx, key)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("Load = %q, want %q", got, "new")
	}
}

func TestCleanKeyProducesValidKubernetesNames(t *testing.T) {
	cases := []struct {
		key    string
		prefix string
		want   string
	}{
		// An ARI lock key with the same shape as the one behind the July
		// renewal outage (the ID here is synthetic): certmagic locks ARI
		// refreshes with "ari_" + the ARI certificate ID, which is
		// mixed-case base64url. The apiserver rejected the uppercase lease
		// name, and renewal maintenance hung forever retrying it.
		{
			key:    "ari_aA1aA1aA1aA1aA1_Aa1Aa1Aa1Aa.A1aA1aA1aA1aA1aA1aA1aA1a",
			prefix: leasePrefix,
			want:   "caddy-lock-ari.aa1aa1aa1aa1aa1.aa1aa1aa1aa.a1aa1aa1aa1aa1aa1aa1aa1a",
		},
		// Empty ARI ID must not leave a trailing separator.
		{key: "ari_", prefix: leasePrefix, want: "caddy-lock-ari"},
		// Ordinary issuance lock and cert resource keys are unchanged.
		{
			key:    "issue_cert_example-1.cloud",
			prefix: leasePrefix,
			want:   "caddy-lock-issue.cert.example-1.cloud",
		},
		{
			key:    "certificates/acme-v02.api.letsencrypt.org-directory/example-2.com/example-2.com.crt",
			prefix: keyPrefix,
			want:   "caddy.ingress--certificates.acme-v02.api.letsencrypt.org-directory.example-2.com.example-2.com.crt",
		},
		// Wildcard storage keys ("*." becomes "wildcard_." in certmagic).
		{
			key:    "issue_cert_*.example.com",
			prefix: leasePrefix,
			want:   "caddy-lock-issue.cert.example.com",
		},
		// A dash adjacent to a replaced character must not produce a label
		// that starts or ends with '-' (base64url IDs can contain "_-").
		{key: "ari_ab-_cd_-ef", prefix: leasePrefix, want: "caddy-lock-ari.ab.cd.ef"},
	}

	for _, tc := range cases {
		got := cleanKey(tc.key, tc.prefix)
		if got != tc.want {
			t.Errorf("cleanKey(%q, %q) = %q, want %q", tc.key, tc.prefix, got, tc.want)
		}
		if errs := validation.IsDNS1123Subdomain(got); len(errs) != 0 {
			t.Errorf("cleanKey(%q, %q) = %q is not a valid kubernetes name: %v", tc.key, tc.prefix, got, errs)
		}
	}
}

func TestLockFailsFastOnValidationError(t *testing.T) {
	s := newTestStorage(t)
	fakeClient := s.kubeClient.(*fake.Clientset)
	// The fake clientset does not validate object names, so simulate the
	// apiserver's RFC 1123 rejection (the July outage failure mode).
	fakeClient.PrependReactor("create", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInvalid(
			schema.GroupKind{Group: "coordination.k8s.io", Kind: "Lease"},
			"caddy-lock-ari.MixedCase", nil)
	})

	start := time.Now()
	err := s.Lock(context.Background(), "ari_MixedCaseID")
	if err == nil {
		t.Fatal("Lock succeeded despite validation rejection")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("Lock error = %v, want an Invalid apierror", err)
	}
	// Must fail on the first attempt, not after retries/timeouts.
	if elapsed := time.Since(start); elapsed > leasePollInterval {
		t.Fatalf("Lock took %v to fail; expected immediate failure", elapsed)
	}
}

func TestTryLock(t *testing.T) {
	s := newQuietTestStorage(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key := "ari_someCertificateID"

	ok, err := s.TryLock(ctx, key)
	if err != nil || !ok {
		t.Fatalf("TryLock on free lock = (%v, %v), want (true, nil)", ok, err)
	}
	if err := s.Unlock(ctx, key); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	// Held by another instance: must report busy without error and without
	// blocking.
	leaseName := cleanKey(key, leasePrefix)
	_, err = s.kubeClient.CoordinationV1().Leases(s.Namespace).Create(ctx, otherHolderLease(leaseName, s.Namespace, false), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating fixture lease: %v", err)
	}
	ok, err = s.TryLock(ctx, key)
	if err != nil || ok {
		t.Fatalf("TryLock on held lock = (%v, %v), want (false, nil)", ok, err)
	}
}
