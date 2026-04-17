package util

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

const (
	snowflakeWorkerIDBits = 10
	snowflakeSequenceBits = 12

	snowflakeMaxWorkerID  = (1 << snowflakeWorkerIDBits) - 1
	snowflakeSequenceMask = (1 << snowflakeSequenceBits) - 1

	snowflakeWorkerIDShift  = snowflakeSequenceBits
	snowflakeTimestampShift = snowflakeWorkerIDBits + snowflakeSequenceBits
)

var snowflakeEpoch = time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

type IDGenerator struct {
	mu sync.Mutex

	workerID       int64
	lastTimestamp  int64
	sequence       int64
	clockError     string
	unhealthyUntil int64
}

func NewIDGenerator(workerID int) (*IDGenerator, error) {
	if workerID < 0 || workerID > snowflakeMaxWorkerID {
		return nil, fmt.Errorf("WORKER_ID must be between 0 and %d", snowflakeMaxWorkerID)
	}

	return &IDGenerator{
		workerID: int64(workerID),
	}, nil
}

func (g *IDGenerator) Generate() (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now().UTC().UnixMilli()
	if now < g.lastTimestamp {
		g.unhealthyUntil = g.lastTimestamp
		g.clockError = fmt.Sprintf(
			"clock moved backwards by %dms: refusing to generate id until clock recovers",
			g.lastTimestamp-now,
		)
		return "", fmt.Errorf(g.clockError)
	}

	g.clearClockErrorIfRecovered(now)

	if now == g.lastTimestamp {
		g.sequence = (g.sequence + 1) & snowflakeSequenceMask
		if g.sequence == 0 {
			now = waitNextMillisecond(g.lastTimestamp)
		}
	} else {
		g.sequence = 0
	}

	g.lastTimestamp = now

	id := ((now - snowflakeEpoch) << snowflakeTimestampShift) |
		(g.workerID << snowflakeWorkerIDShift) |
		g.sequence

	return strconv.FormatInt(id, 10), nil
}

func (g *IDGenerator) HealthError() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now().UTC().UnixMilli()
	g.clearClockErrorIfRecovered(now)
	if g.clockError == "" {
		return nil
	}

	return fmt.Errorf(g.clockError)
}

func (g *IDGenerator) clearClockErrorIfRecovered(now int64) {
	if g.clockError == "" {
		return
	}
	if now < g.unhealthyUntil {
		return
	}

	g.clockError = ""
	g.unhealthyUntil = 0
}

func waitNextMillisecond(lastTimestamp int64) int64 {
	for {
		now := time.Now().UTC().UnixMilli()
		if now > lastTimestamp {
			return now
		}
		time.Sleep(time.Millisecond)
	}
}
