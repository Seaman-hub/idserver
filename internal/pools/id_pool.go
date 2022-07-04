package pools

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	etcdKeyIDMaxUsed  = "maxused"
	etcdKeyIDReleased = "released"
	// the same as client side application
	etcdKeyIDUsed = "/nat/meters/"
)

// IDPoolStateBackend interface encapsulates the logic of retrieving and persisting the state of a IDPool.
type IDPoolStateBackend interface {
	GetUsed(ctx context.Context) ([]int, error)
	GetMaxUsed(ctx context.Context) (int, error)
	SetMaxUsed(ctx context.Context, id int) error
	GetReleased(ctx context.Context) ([]int, error)
	SetReleased(ctx context.Context, id []int) error
}

type IDPool struct {
	locker   DistLocker
	backend  IDPoolStateBackend
	released []int
	logger   Logger
	max_id   int
	mtx      sync.Mutex
}

// NewIDPool creates and initializes an IDPool.
func NewIDPool(initValue int, locker DistLocker, IDPoolStateBackend IDPoolStateBackend) *IDPool {
	return &IDPool{
		locker:  locker,
		backend: IDPoolStateBackend,
		max_id:  initValue,
	}
}

// Fill recycled number hole
func (p *IDPool) Init(ctx context.Context) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if err := p.locker.Lock(ctx); err != nil {
		return err
	}
	defer func() {
		if err := p.locker.Unlock(); err != nil {
			p.logger.Log(err)
		}
	}()

	p.max_id, _ = p.backend.GetMaxUsed(ctx)

	var i int
	used := make(map[int]bool)
	for i = 1; i <= p.max_id; i++ {
		used[i] = true
	}
	usedlist, _ := p.backend.GetUsed(ctx)
	if len(usedlist) > 0 {
		for i := 0; i < len(usedlist); i++ {
			if _, ok := used[usedlist[i]]; ok {
				delete(used, usedlist[i])
			}
		}
	}
	for k, _ := range used {
		p.released = append(p.released, k)
	}
	return nil
}

func (p *IDPool) Acquire(ctx context.Context) (id int, err error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	// fmt.Println("Acquire()", time.Now().UnixNano()/int64(time.Millisecond))
	if err := p.locker.Lock(ctx); err != nil {
		return 0, err
	}
	defer func() {
		if err := p.locker.Unlock(); err != nil {
			p.logger.Log(err)
		}
	}()

	released, _ := p.backend.GetReleased(ctx)
	p.released = released

	// Pick a value that's been returned, if any.
	if len(p.released) > 0 {
		id = p.released[len(p.released)-1]
		p.released = p.released[:len(p.released)-1]
		p.backend.SetReleased(ctx, p.released)
		return id, nil
	}
	p.max_id, _ = p.backend.GetMaxUsed(ctx)

	p.max_id++
	p.backend.SetMaxUsed(ctx, p.max_id)

	return p.max_id, nil
}

func (p *IDPool) Release(ctx context.Context, id int) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if err := p.locker.Lock(ctx); err != nil {
		return err
	}
	defer func() {
		if err := p.locker.Unlock(); err != nil {
			p.logger.Log(err)
		}
	}()

	v, _ := p.backend.GetMaxUsed(ctx)
	p.max_id = v
	if id > v {
		return nil
	}
	// If we're recycling maxUsed, just shrink the pool.
	if id == v {
		p.max_id = id - 1
		p.backend.SetMaxUsed(ctx, p.max_id)
		return nil
	}

	released, _ := p.backend.GetReleased(ctx)

	for _, v := range released {
		if v == id {
			return nil
		}
	}

	p.released = released
	// Add it to the set of recycled IDs.
	p.released = append(p.released, id)
	p.backend.SetReleased(ctx, p.released)

	return nil
}

// IDPoolEtcd is an etcd implementation of a IDPoolStateBackend.
type IDPoolEtcd struct {
	// prefix is the etcd key prefix.
	prefix string
	cli    *clientv3.Client
}

func NewIDPoolEtcd(cli *clientv3.Client, prefix string) *IDPoolEtcd {
	return &IDPoolEtcd{
		prefix: prefix,
		cli:    cli,
	}
}

func (t *IDPoolEtcd) GetUsed(ctx context.Context) ([]int, error) {
	r, _ := t.cli.Get(ctx, etcdKeyIDUsed, clientv3.WithPrefix())
	if len(r.Kvs) == 0 {
		return nil, nil
	}
	used := []int{}
	for _, kv := range r.Kvs {
		v, err := strconv.Atoi(string(kv.Key)[12:])
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse value '%s' as int32", string((kv.Key)[11:]))
		}
		used = append(used, int(v))
	}
	return used, nil
}

func (t *IDPoolEtcd) GetReleased(ctx context.Context) ([]int, error) {
	r, _ := t.cli.Get(ctx, t.prefix+etcdKeyIDReleased)
	if len(r.Kvs) == 0 {
		return nil, nil
	}

	for _, kv := range r.Kvs {
		if len(string(kv.Value)) == 0 {
			return nil, nil
		}
		released := strings.Split(string(kv.Value), ",")
		if len(released) < 1 {
			return nil, nil
		}

		idlist := make([]int, len(released))

		for index, val := range released {
			idlist[index], _ = strconv.Atoi(val)
		}
		return idlist, nil
	}
	return nil, nil
}

func (t *IDPoolEtcd) SetReleased(ctx context.Context, id []int) error {
	v := strings.Replace(strings.Trim(fmt.Sprint(id), "[]"), " ", ",", -1)
	_, err := t.cli.Put(ctx, t.prefix+etcdKeyIDReleased, v)
	if err != nil {
		return err
	}
	return nil
}

func (t *IDPoolEtcd) GetMaxUsed(ctx context.Context) (int, error) {
	r, _ := t.cli.Get(ctx, t.prefix+etcdKeyIDMaxUsed)
	if len(r.Kvs) == 0 {
		return 0, nil
	}
	for _, kv := range r.Kvs {
		v, err := strconv.ParseInt(string(kv.Value), 10, 32)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to parse value '%s' as int32", string(kv.Value))
		}
		return int(v), nil
	}
	return 0, nil
}

func (t *IDPoolEtcd) SetMaxUsed(ctx context.Context, id int) error {
	v := fmt.Sprintf("%d", id)
	_, err := t.cli.Put(ctx, t.prefix+etcdKeyIDMaxUsed, v)
	if err != nil {
		return err
	}
	return nil
}
