package fnf_ch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/slices"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var asyncDups = [10]atomic.Uint64{}

func goMonitor(wg *sync.WaitGroup, ctx context.Context, cwsStats []*DbStats, expRate uint64) {
	defer wg.Done()

	p := message.NewPrinter(language.English)

	cResMon := &ResMonitor{}
	err := cResMon.Init()
	if err != nil {
		panic("resmon init failed: " + err.Error())
	}

	startUnixMilli := time.Now().UnixMilli()
	const failOnLowRateIters = 15
	lessThanExpIters := 0 // if rate is less than 2/3 of exp for `failOnLowRateIters` iterations => fail
	for {
		select {
		case <-ctx.Done():
			fmt.Println("monitor done")
			return
		default:
		}

		time.Sleep(time.Second)
		nowUnixMilli := time.Now().UnixMilli()

		recsDiff := uint64(0)
		writeErrDiff := uint64(0)
		for _, stats := range cwsStats {
			recsDiff += stats.writeRecs.Swap(0)
			writeErrDiff += stats.writeErr.Swap(0)
		}
		elapsedSec := (nowUnixMilli - startUnixMilli) / 1e3
		cCpu, cMem, err := cResMon.GetUsageRelative()
		if err != nil {
			panic("resmon query err: " + err.Error())
		}
		if recsDiff < expRate {
			lessThanExpIters++
			if lessThanExpIters > failOnLowRateIters {
				if err != nil {
					panic(fmt.Errorf("too low rate for %d iters", failOnLowRateIters))
				}
			}
		} else {
			lessThanExpIters = 0
		}
		var asyncDupsSnap [10]uint64
		for i := range asyncDups {
			asyncDupsSnap[i] = asyncDups[i].Swap(0)
		}
		var reqDursSnap [8]uint64
		for i := range reqDurHist {
			reqDursSnap[i] = reqDurHist[i].Swap(0)
		}
		p.Printf("%5d (sec), cpu: %15.0f%%, rss: %15d, rows: %15d, errs: %5d, dups: %v, durs: %v\n",
			elapsedSec, cCpu, cMem, recsDiff, writeErrDiff, asyncDupsSnap, reqDursSnap)

	}
}

var reqDurHist = [8]atomic.Uint64{}

var reqDurHistBuckets = []time.Duration{
	0,
	time.Second,
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	15 * time.Second,
	30 * time.Second,
	1 * time.Minute,
}

func addReqDurTime(t time.Duration) {
	index, found := slices.BinarySearch(reqDurHistBuckets, t)
	if found { // in start of needed bucket
		reqDurHist[index].Add(1)
	} else { // went ahead
		reqDurHist[index-1].Add(1)
	}
}
