package fnf_ch

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/compress"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func readBulk(t testing.TB) *DbManualBulk {
	dataZipped, err := os.ReadFile(filepath.Join(envInputDir, "input.json.gz"))
	require.NoError(t, err)
	reader, err := gzip.NewReader(bytes.NewReader(dataZipped))
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)

	bulk := DbManualBulk{}
	err = json.Unmarshal(data, &bulk)
	require.NoError(t, err)
	return &bulk
}

func clickhouseGoAsyncIngest(wg *sync.WaitGroup, ctx context.Context, cw *DbWriterManualAsync) {
	defer wg.Done()
	t := time.Now()
	dups := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		start := time.Now()
		cw.writeBatch(dups)
		addReqDurTime(time.Since(start))
		t = t.Add(envTenantBulkDelay)
		dups = 0
		for time.Until(t) < 0 {
			t = t.Add(envTenantBulkDelay)
			dups++
		}
		if dups > 9 {
			panic("too high deficit")
		}
		asyncDups[dups].Add(1)
		time.Sleep(time.Until(t))
	}
}

func TestBulkRead(t *testing.T) {
	envInputFill()
	bulk := readBulk(t)
	fmt.Println("Read recs:", len(bulk.Recs))
}

func bulkSize(b []string) int {
	ret := 0
	for i := range b {
		ret += len(b[i])
	}
	return ret
}

func ClickhouseGoAsyncCommon(t testing.TB, cws []*DbWriterManualAsync) {
	require.Equal(t, envTenants, len(cws))
	ctx, cancelCtx := context.WithCancel(context.Background())

	bulk := readBulk(t)
	require.GreaterOrEqual(t, len(bulk.Recs), envBulkSize)
	bulk.Recs = slices.Clip(bulk.Recs[:envBulkSize])

	cwsStats := []*DbStats{}
	for _, cw := range cws {
		cwsStats = append(cwsStats, &cw.stats)
		expInfo := DevInfo{
			deviceIp: randIp().String(),
			siteId:   uuid.New().String(),
			tenantId: uuid.New().String(),
		}
		err := cw.Init(bulk, expInfo)
		require.NoError(t, err)
	}
	runtime.GC()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go goMonitor(wg, ctx, cwsStats, uint64(envTenants*envTenantRate))

	fmt.Println("start ingesting")

	for _, cw := range cws {
		wg.Add(1)
		go clickhouseGoAsyncIngest(wg, ctx, cw)
		time.Sleep(envTenantBulkDelay / time.Duration(envTenants))
	}

	time.Sleep(envDur)
	cancelCtx()
	wg.Wait()

	fmt.Println("done ingesting")
}

func TestClickManualAsyncCompr(t *testing.T) {
	fmt.Println("Creating conns per tenant")
	envFill()
	envPrint()
	var writers []*DbWriterManualAsync
	for i := 0; i < envTenants; i++ {
		cw, err := CreateDbWriterManualAsync(compress.LZ4)
		require.NoError(t, err)
		writers = append(writers, cw)
	}
	ClickhouseGoAsyncCommon(t, writers)
}

func TestRecreateDb(t *testing.T) {
	envDbFill()
	fmt.Println("Creating client")
	cw, err := CreateDbWriterManualAsync(compress.NONE)
	require.NoError(t, err)
	fmt.Println("Removing DB")
	err = cw.removeDb()
	require.NoError(t, err)
	fmt.Println("Creating DB")
	err = cw.createDb()
	require.NoError(t, err)
}
