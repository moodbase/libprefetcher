package main

import (
	"context"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/moodbase/libprefetcher"
)

var endpoint = "http://replace.with.a.rpc.endpoint"

// define what you want to fetch
func fetchBlock(height int64) (any, error) {
	client, err := ethclient.Dial(endpoint)
	if err != nil {
		return nil, err
	}
	block, err := client.BlockByNumber(context.Background(), big.NewInt(height))
	return block, err
}

func main() {
	client, err := ethclient.Dial(endpoint)
	if err != nil {
		slog.Error("failed to connect to the Ethereum client", "err", err)
		return
	}
	currentHeight, err := client.BlockNumber(context.Background())
	if err != nil {
		slog.Error("failed to get the current block number", "err", err)
		return
	}
	processed := int64(currentHeight) - 98
	// init pre-fetcher
	preFetcher := libprefetcher.New(endpoint, fetchBlock, processed, 0, 32, 8)
	// start it
	preFetcher.Start()
	for i := processed + 1; i < processed+100; i++ {
		// get the data prefetched one by one
		get := preFetcher.Get(i)
		if get == nil {
			slog.Info("wait for block", "height", i)
			time.Sleep(1 * time.Second)
			i--
		} else {
			block := get.Data.(*types.Block)
			slog.Info(
				"fetched block",
				"specified height", get.BlockNumber,
				"block height", block.Number().Int64(),
				"size", len(block.Transactions()),
				"hash", block.Hash(),
			)
		}
	}
	preFetcher.Stop()
}
