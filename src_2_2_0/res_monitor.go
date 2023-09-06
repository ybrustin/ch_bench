package fnf_ch

import (
	"fmt"
	"os"
	"time"

	"github.com/prometheus/procfs"
)

type ResMonitorIf interface {
	// CPU percentage since last call, current RSS, error
	GetUsageRelative() (float64, int64, error)
	// CPU usage in seconds, Wall clock elapsed seconds, current RSS, error
	GetUsageTotal() (float64, float64, int64, error)
}

type ResMonitor struct {
	prevCpuSec  float64
	prevTime    time.Time
	startCpuSec float64
	startTime   time.Time
	proc        procfs.Proc
}

func (r *ResMonitor) Init() error {
	pid := os.Getpid()
	return r.InitPid(pid)
}

func (r *ResMonitor) InitPid(pid int) error {
	var err error
	r.proc, err = procfs.NewProc(pid)
	if err != nil {
		return err
	}
	cpuSec, _, err := r.readStat()
	if err != nil {
		return err
	}
	now := time.Now()
	r.startTime = now
	r.prevTime = now
	r.startCpuSec = cpuSec
	r.prevCpuSec = cpuSec
	return nil
}

func (r *ResMonitor) readStat() (float64, int64, error) {
	stats, err := r.proc.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("could not get process stats, error: %w", err)
	}
	return stats.CPUTime(), int64(stats.ResidentMemory()), nil
}

func (r *ResMonitor) GetUsageTotal() (float64, float64, int64, error) {
	curSec, rss, err := r.readStat()
	if err != nil {
		return 0, 0, 0, err
	}
	elapsedWallSec := time.Since(r.startTime).Seconds()
	elapsedCpuSec := curSec - r.startCpuSec
	return elapsedCpuSec, elapsedWallSec, rss, nil
}

func (r *ResMonitor) GetUsageRelative() (float64, int64, error) {
	curSec, rss, err := r.readStat()
	if err != nil {
		return 0, 0, err
	}
	now := time.Now()
	elapsedWallSec := now.Sub(r.prevTime).Seconds()
	if elapsedWallSec <= 0 {
		return 0, 0, nil
	}
	elapsedCpuSec := curSec - r.prevCpuSec
	r.prevCpuSec = curSec
	r.prevTime = now
	return 100 * elapsedCpuSec / elapsedWallSec, rss, nil
}
