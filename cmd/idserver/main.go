package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"

	"github.com/Seaman-hub/idserver/api/ns"
	"github.com/Seaman-hub/idserver/internal/api"
	"github.com/Seaman-hub/idserver/internal/common"
	"github.com/Seaman-hub/idserver/internal/storage"
)

var version string // set by the compiler
var nsapi *api.NumberServerAPI

func run(c *cli.Context) error {
	tasks := []func(*cli.Context) error{
		printStartMessage,
		setEtcdConnection,
		startAPIServer,
	}

	for _, t := range tasks {
		if err := t(c); err != nil {
			log.Fatal(err)
		}
	}

	sigChan := make(chan os.Signal)
	exitChan := make(chan struct{})
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	log.WithField("signal", <-sigChan).Info("signal received")
	go func() {
		log.Warning("stopping idserver")
		exitChan <- struct{}{}
	}()
	select {
	case <-exitChan:
	case s := <-sigChan:
		log.WithField("signal", s).Info("signal received, stopping immediately")
	}

	return nil
}

func printStartMessage(c *cli.Context) error {
	log.WithFields(log.Fields{
		"version": version,
		"docs":    "not implemented",
	}).Info("starting Number Server")

	return nil
}

func setEtcdConnection(c *cli.Context) error {
	log.Info("connecting to etcd")
	db, err := storage.OpenDatabase(c)
	if err != nil {
		log.Fatalln("Failed to open storage:", err)
	}
	common.DB = db
	return nil
}

func startAPIServer(c *cli.Context) error {
	log.WithFields(log.Fields{
		"bind": c.String("bind"),
	}).Info("starting Number api server")

	var opts []grpc.ServerOption
	gs := grpc.NewServer(opts...)
	nsapi = api.NewNumberServerAPI(common.DB, "/idpool_lock/simple/", "/idpool/simple/", c.Int("id-initial"))
	ns.RegisterNumberServerServer(gs, nsapi)

	ln, err := net.Listen("tcp", c.String("bind"))
	if err != nil {
		return errors.Wrap(err, "start api listener error")
	}

	go gs.Serve(ln)
	go Watch()

	return nil
}
func Watch() {
	log.Info("watching etcd connection")
	wc := storage.WatchWithPrefix(common.DB, "/nat/meters/")
	go func() {
		for watchResp := range *wc {
			for _, e := range watchResp.Events {
				switch e.Type {
				// case mvccpb.PUT:

				case mvccpb.DELETE:
					id := string(e.PrevKv.Key)[12:]
					var req ns.PutSequenceNumRequest
					num, _ := strconv.Atoi(id)
					req.Number = uint32(num)
					log.Info("delete watched")
					nsapi.PutSequenceNum(context.Background(), &req)

				}
			}
		}
	}()
}

func main() {
	app := cli.NewApp()
	app.Name = "idserver"
	app.Usage = "number-server for SDN networks"
	app.Version = version
	app.Copyright = "See http://github.com/Seaman-hub/idserver for copyright information"
	app.Action = run
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "bind",
			Usage:  "ip:port to bind the api server",
			Value:  "0.0.0.0:8000",
			EnvVar: "BIND",
		},
		cli.StringFlag{
			Name:   "storage-server",
			Value:  "localhost:2379",
			Usage:  "connect to storage server",
			EnvVar: "STORAGE_SERVER",
		},
		cli.IntFlag{
			Name:   "id-initial",
			Value:  1,
			Usage:  "initial id value to initialize ID pool",
			EnvVar: "ID_INITIAL",
		},
	}

	app.Run(os.Args)
}
