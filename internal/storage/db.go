package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	etcdTimeoutSec = 5
)

type Etcd struct {
	Client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

// Query key
func Get(db *Etcd, key string) (*clientv3.GetResponse, error) {
	resp, err := db.kv.Get(context.TODO(), key)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return resp, err
}

// Query keys with Prefix
func GetWithPrefix(db *Etcd, prefix string) (*clientv3.GetResponse, error) {
	resp, err := db.kv.Get(context.TODO(), prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		return nil, err
	}

	return resp, err
}

// Query keys with Prefix
func WatchWithPrefix(db *Etcd, prefix string) *clientv3.WatchChan {
	watchRespChan := db.watcher.Watch(context.TODO(), prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())

	return &watchRespChan
}

// OpenDatabase opens the database
func OpenDatabase(c *cli.Context) (*Etcd, error) {
	log.WithFields(log.Fields{
		"storage-server": c.String("storage-server"),
	}).Info("connecting to storage-server")

	members := strings.SplitN(c.String("storage-server"), ",", 3)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   members,
		DialTimeout: etcdTimeoutSec * time.Second,
	})
	if err != nil {
		fmt.Println("client new error\n", err)
		return nil, err
	}

	kv := clientv3.NewKV(cli)
	lease := clientv3.NewLease(cli)
	watcher := clientv3.NewWatcher(cli)

	return &Etcd{
		Client:  cli,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}, nil
}

// CloseDatabase close the database
func CloseDatabase(db *Etcd) {
	db.Client.Close()
}
