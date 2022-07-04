package api

import (
	"github.com/Seaman-hub/idserver/api/ns"
	"github.com/Seaman-hub/idserver/internal/pools"
	"github.com/Seaman-hub/idserver/internal/storage"

	"golang.org/x/net/context"
)

var (
	numberPool *pools.IDPool
)

// NumberServerAPI defines the number-server API.
type NumberServerAPI struct {
}

// NewNumberServerAPI returns a new NumberServerAPI.
func NewNumberServerAPI(cli *storage.Etcd, prefix, dataprefix string, initValue int) *NumberServerAPI {
	numberPool = pools.NewIDPool(
		initValue,
		pools.NewLockEtcd(cli.Client, prefix, pools.NewStdLogger()),
		pools.NewIDPoolEtcd(cli.Client, dataprefix),
	)
	numberPool.Init(context.Background())
	return &NumberServerAPI{}
}

// GetSequenceNum returns a sequence number.
func (n *NumberServerAPI) GetSequenceNum(ctx context.Context, req *ns.GetSequenceNumRequest) (*ns.GetSequenceNumResponse, error) {
	mid, _ := numberPool.Acquire(ctx)
	return &ns.GetSequenceNumResponse{
		Number: uint32(mid),
	}, nil
}

// PutSequenceNum puts a sequence number back to pool.
func (n *NumberServerAPI) PutSequenceNum(ctx context.Context, req *ns.PutSequenceNumRequest) (*ns.PutSequenceNumResponse, error) {
	numberPool.Release(ctx, int(req.Number))
	return &ns.PutSequenceNumResponse{}, nil
}
