package fnf_ch

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/compress"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type DbManualBulk struct {
	Recs []string `json:"recs"`
}

type DevInfo struct {
	deviceIp string
	siteId   string
	tenantId string
}

type DbStats struct {
	writeErr  atomic.Uint64
	writeOk   atomic.Uint64
	writeRecs atomic.Uint64
}

type DbWriterManualAsync struct {
	cmd      strings.Builder
	compress compress.Method
	conn     driver.Conn
	stats    DbStats

	// init vars
	b         *DbManualBulk
	devInfo   DevInfo
	prefix    string
	tableName string
}

func (o *DbWriterManualAsync) connect() (err error) {
	addr := fmt.Sprintf("%s:%d", envDbHost, envDbPort)
	var tlsConf *tls.Config
	if envDbSecure {
		tlsConf = &tls.Config{}
	}
	o.conn, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Username: envDbUser,
			Password: envDbPass,
		},
		Compression: &clickhouse.Compression{
			Method: o.compress,
		},
		DialTimeout: 5 * time.Second,
		TLS:         tlsConf,
	})
	return
}

func CreateDbWriterManualAsync(compress compress.Method) (*DbWriterManualAsync, error) {
	db := new(DbWriterManualAsync)
	db.compress = compress
	err := db.connect()
	if err != nil {
		return nil, fmt.Errorf("could connect to DB. Probably it's down/unreachable. Error: %w", err)
	}
	err = db.conn.Ping(context.Background())
	if err != nil {
		return nil, fmt.Errorf("ping to DB failed: %w", err)
	}
	return db, nil
}

func (o *DbWriterManualAsync) Init(b *DbManualBulk, tntInfo DevInfo) error {
	o.b = b
	o.devInfo = tntInfo
	o.tableName = "sink_tcp"
	o.prefix = fmt.Sprintf("INSERT INTO sdflow.%s VALUES", o.tableName)
	return o.createTable()
}

func (o *DbWriterManualAsync) Close() {
	o.conn.Close()
}

func (o *DbWriterManualAsync) exec(msg string) error {
	return o.conn.Exec(context.Background(), msg)
}

func (o *DbWriterManualAsync) createDb() error {
	return o.exec("CREATE DATABASE IF NOT EXISTS sdflow")
}

func (o *DbWriterManualAsync) removeDb() error {
	return o.exec("DROP DATABASE IF EXISTS sdflow")
}

func (o *DbWriterManualAsync) createTable() (err error) {
	wait := 0
	if envAsyncWait {
		wait = 1
	}
	q := fmt.Sprintf(`ALTER USER %s
		SETTINGS async_insert = 1,
		async_insert_busy_timeout_ms = %d,
		async_insert_max_data_size = %d,
		async_insert_threads = %d,
		wait_for_async_insert = %d`,
		envDbUser, envAsyncMaxTime*1000, envAsyncMaxMem, envAsyncThreads, wait)
	if !envDedup {
		q += ", async_insert_deduplicate = 0"

	}
	err = o.exec(q)
	if err != nil {
		return fmt.Errorf("db settings error: %w", err)
	}
	ddl := getCreateTableDdl(o.tableName)
	err = o.exec(ddl)
	if err != nil {
		return fmt.Errorf("db create table error: %w", err)
	}
	return nil
}

func (o *DbWriterManualAsync) fillWriteCmd() {
	o.cmd.Reset()
	o.cmd.WriteString(o.prefix)
	now := time.Now()
	ts := strconv.FormatInt(now.Unix(), 10)

	for recIndex := range o.b.Recs {
		o.cmd.WriteByte('(')
		o.cmd.WriteString(ts)
		o.cmd.WriteString(",'")
		o.cmd.WriteString(o.devInfo.tenantId)
		o.cmd.WriteString("','")
		o.cmd.WriteString(o.devInfo.siteId)
		o.cmd.WriteString("','")
		o.cmd.WriteString(o.devInfo.deviceIp)
		o.cmd.WriteString("',")
		o.cmd.WriteString(o.b.Recs[recIndex])
		o.cmd.WriteByte(')')
	}
}

func (o *DbWriterManualAsync) writeBatchSend(wg *sync.WaitGroup) {
	defer wg.Done()

	try := 0
	cmdString := o.cmd.String()
	for {
		err := o.exec(cmdString)
		if err != nil {
			try++
			if try > 50 {
				panic("too many tries")
			}
			o.stats.writeErr.Add(1)
			slowPrinters[0].printf("Error sending to DB (tnt %s): %s\n", o.devInfo.tenantId, err.Error())
			time.Sleep(100 * time.Millisecond)
		} else {
			o.stats.writeOk.Add(1)
			o.stats.writeRecs.Add(uint64(len(o.b.Recs)))
			return
		}
	}
}

func (o *DbWriterManualAsync) writeBatch(dups int) {
	o.fillWriteCmd()

	wg := &sync.WaitGroup{}
	for i := 0; i <= dups; i++ {
		wg.Add(1)
		go o.writeBatchSend(wg)
	}
	wg.Wait()
}
