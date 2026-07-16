package storage

import (
	"context"
	"errors"
	"io/fs"
	"testing"

	"go.uber.org/zap/zaptest"
	"k8s.io/client-go/kubernetes/fake"
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
