package storage

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	s := newTestStorage(t)
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
