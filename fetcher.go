package libprefetcher

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

type FetchFunc func(height int64) (any, error)

type PreFetcher struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	m      map[int64]*Payload
	mLock  sync.RWMutex

	endpoint string
	fetch    FetchFunc

	size      int64 // buffer size
	lag       int64 // blocks set to lag behind the chain head to avoid fork
	processed int64 // block already processed

	scheduled int64 // height scheduled to fetch
	chainHead int64 // block height of the latest block

	fetchConcurrency int
	chFetchData      chan int64
	chFetchRetry     chan int64
	chPayload        chan *Payload
}

type Payload struct {
	BlockNumber int64
	Data        any
}

// New creates a new PreFetcher instance.
//
//	endpoint: the endpoint of the ethereum node
//	fetchFunc: the function to fetch some Data you want
//	processed: the block number already processed
//	lag: the blocks set to lag behind the chain head to avoid fork
//	size: the buffer size
//	fetchConcurrency: the number of concurrent fetchers
func New(endpoint string, fetchFunc FetchFunc, processed, lag, size int64, fetchConcurrency int) *PreFetcher {
	ctx, cancelFunc := context.WithCancel(context.Background())
	b := &PreFetcher{
		ctx:    ctx,
		cancel: cancelFunc,
		wg:     sync.WaitGroup{},
		m:      make(map[int64]*Payload),
		mLock:  sync.RWMutex{},

		fetch:    fetchFunc,
		endpoint: endpoint,

		size:      size,
		lag:       lag,
		processed: processed,

		scheduled: processed,

		fetchConcurrency: fetchConcurrency,
		chFetchData:      make(chan int64, fetchConcurrency),
		chFetchRetry:     make(chan int64, fetchConcurrency),
		chPayload:        make(chan *Payload, fetchConcurrency),
	}
	return b
}

func (p *PreFetcher) Stop() {
	slog.Info("[PreFetcher] stopping")
	p.cancel()
	p.wg.Wait()
	slog.Info("[PreFetcher] stopped")
}

func (p *PreFetcher) Start() {
	p.updateChainHead()
	slog.Info("[PreFetcher] starting")
	go p.mapWriteLoop()
	go p.feedTask()
	for i := 0; i < p.fetchConcurrency; i++ {
		go p.dataFetcher(i)
	}
}

func (p *PreFetcher) updateChainHead() {
	client, err := ethclient.Dial(p.endpoint)
	defer client.Close()
	if err != nil {
		slog.Error("[PreFetcher.updateChainHead] failed to create eth client", "error", err)
		return
	}
	number, err := client.BlockNumber(p.ctx)
	if err != nil {
		slog.Error("[PreFetcher.updateChainHead] failed to get block number", "error", err)
		return
	}
	if number == 0 {
		return
	}
	if p.chainHead != int64(number) {
		slog.Debug("[PreFetcher.updateChainHead] chain head updated", "block BlockNumber", number)
	}
	p.chainHead = int64(number)
}

func (p *PreFetcher) mapWriteLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer func() {
		slog.Info("[PreFetcher] map write loop stopped")
		p.wg.Done()
		ticker.Stop()
	}()
	slog.Info("[PreFetcher] map write loop started")
	p.wg.Add(1)
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.updateChainHead()
			record := slog.NewRecord(time.Now(), slog.LevelInfo, "[PreFetcher] status", 0)
			p.mLock.RLock()
			record.Add("chain head", p.chainHead, "scheduled", p.scheduled, "buff length", len(p.m))
			p.mLock.RUnlock()
			slog.Default().Handler().Handle(context.Background(), record)
		case payload := <-p.chPayload:
			p.mLock.Lock()
			p.m[payload.BlockNumber] = payload
			p.mLock.Unlock()
		}
	}
}

// Get returns the block Data of the given BlockNumber. Not thread safe.
func (p *PreFetcher) Get(height int64) *Payload {
	p.processed = height - 1

	p.mLock.RLock()
	data, ok := p.m[height]
	p.mLock.RUnlock()

	if ok {
		p.mLock.Lock()
		delete(p.m, p.processed)
		p.mLock.Unlock()
		return data
	}

	slog.Info("[PreFetcher.Get] Data not exist", "BlockNumber", height)
	return nil
}

func (p *PreFetcher) dataFetcher(fetcherIndex int) {
	defer func() {
		slog.Info(fmt.Sprintf("[PreFetcher %d] stopped", fetcherIndex))
		p.wg.Done()
	}()
	slog.Info(fmt.Sprintf("[PreFetcher %d] started", fetcherIndex))
	p.wg.Add(1)

	var doFetch = func(i int64) {
		data, err := p.fetch(i)
		if err == nil {
			select {
			case <-p.ctx.Done():
			case p.chPayload <- &Payload{BlockNumber: i, Data: data}:
			}
		} else {
			slog.Error(fmt.Sprintf("[PreFetcher %d] failed to fetch block", fetcherIndex), "BlockNumber", i, "error", err)
			select {
			case <-p.ctx.Done():
			case p.chFetchRetry <- i:
			}

		}
	}

	for {
		// retry failed tasks first
		select {
		case <-p.ctx.Done():
			return
		case i := <-p.chFetchRetry:
			slog.Info(fmt.Sprintf("[PreFetcher %d] retry", fetcherIndex), "BlockNumber", i)
			doFetch(i)
			continue
		default:
		}

		select {
		case <-p.ctx.Done():
			return
		case i := <-p.chFetchData:
			doFetch(i)
		}
	}
}

func (p *PreFetcher) feedTask() {
	slog.Info("[PreFetcher] feed task started")
	p.wg.Add(1)
	defer func() {
		slog.Info("[PreFetcher] feed task stopped")
		p.wg.Done()
	}()
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}
		next := p.scheduled + 1
		// wait for chain head
		if p.chainHead-next < p.lag {
			d := 2 * time.Second
			slog.Debug("[PreFetcher] wait for chain head", "chain head", p.chainHead, "BlockNumber", next, "wait duration", d)
			time.Sleep(d)
			continue
		}
		// wait for execution
		if next > p.processed+p.size {
			time.Sleep(1 * time.Second)
			continue
		}
		select {
		case <-p.ctx.Done():
			return
		case p.chFetchData <- next:
			p.scheduled = next
		}
	}
}
