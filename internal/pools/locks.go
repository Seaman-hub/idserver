package pools

import (
	"context"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// DistLocker is a context aware distributed locker (interface is similar to sync.Locker).
type DistLocker interface {
	// Lock locks the locker.
	Lock(ctx context.Context) error
	// Unlock unlocks the previously successfully locked lock.
	Unlock() error
}

// LockEtcd implements the DistLocker interface using etcd.
//
// See https://github.com/etcd-io/etcd/blob/master/Documentation/learning/why.md#using-etcd-for-distributed-coordination
type LockEtcd struct {
	cli     *clientv3.Client
	prefix  string
	logger  Logger
	mu      *concurrency.Mutex
	session *concurrency.Session
}

// NewLockEtcd creates a new instance of LockEtcd.
func NewLockEtcd(cli *clientv3.Client, prefix string, logger Logger) *LockEtcd {
	return &LockEtcd{cli: cli, prefix: prefix, logger: logger}
}

// Lock creates a new session-based lock in etcd and locks it.
func (l *LockEtcd) Lock(ctx context.Context) error {
	var err error
	l.session, err = concurrency.NewSession(l.cli, concurrency.WithTTL(1))
	if err != nil {
		return errors.Wrap(err, "failed to create an etcd session")
	}
	l.mu = concurrency.NewMutex(l.session, l.prefix)
	err = l.mu.Lock(ctx)

	return errors.Wrap(err, "failed to lock a mutex in etcd")
}

// Unlock unlocks the previously locked lock.
func (l *LockEtcd) Unlock() error {
	defer func() {
		if err := l.session.Close(); err != nil {
			l.logger.Log(err)
		}
	}()
	return errors.Wrap(l.mu.Unlock(l.cli.Ctx()), "failed to unlock a mutex in etcd")
}
