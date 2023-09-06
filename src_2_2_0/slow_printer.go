package fnf_ch

import (
	"fmt"
	"time"
)

type SlowPrinter struct {
	lastPrint time.Time
	minDiff   time.Duration
	skipped   int
}

func (o *SlowPrinter) printf(format string, a ...any) {
	now := time.Now()
	if now.Sub(o.lastPrint) > o.minDiff {
		if o.skipped > 0 {
			skippedStr := fmt.Sprintf("(skipped %d) ", o.skipped)
			fmt.Printf(skippedStr+format, a...)
			o.skipped = 0
		} else {
			fmt.Printf(format, a...)
		}
		o.lastPrint = now
	} else {
		o.skipped++
	}
}

var slowPrinters = [2]SlowPrinter{
	{minDiff: time.Second},
	{minDiff: time.Second},
}
