package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/lance6716/plan-change-capturer/cmd"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	var sig os.Signal
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		select {
		case <-ctx.Done():
			return
		case sig = <-sigCh:
			cancel()
		}
	}()

	err := cmd.Execute(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Printf("cancel pcc by user signal %s\n", sig.String())
		} else {
			panic(err)
		}
	}
}
