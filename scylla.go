package gocql

import (
	"fmt"
	"math"
	"strconv"
	"sync"
)

// scyllaSupported represents Scylla connection options as sent in SUPPORTED
// frame.
type scyllaSupported struct {
	shard     int
	nrShards  int
	msbIgnore uint64
}

func parseSupported(supported map[string][]string) scyllaSupported {
	const (
		scyllaShard             = "SCYLLA_SHARD"
		scyllaNrShards          = "SCYLLA_NR_SHARDS"
		scyllaPartitioner       = "SCYLLA_PARTITIONER"
		scyllaShardingAlgorithm = "SCYLLA_SHARDING_ALGORITHM"
		scyllaShardingIgnoreMSB = "SCYLLA_SHARDING_IGNORE_MSB"
	)

	var (
		si  scyllaSupported
		err error
	)

	if s, ok := supported[scyllaShard]; ok {
		if si.shard, err = strconv.Atoi(s[0]); err != nil {
			if gocqlDebug {
				Logger.Printf("scylla: failed to parse %s value %v: %s", scyllaShard, s, err)
			}
		}
	}
	if s, ok := supported[scyllaNrShards]; ok {
		if si.nrShards, err = strconv.Atoi(s[0]); err != nil {
			if gocqlDebug {
				Logger.Printf("scylla: failed to parse %s value %v: %s", scyllaNrShards, s, err)
			}
		}
	}
	if s, ok := supported[scyllaShardingIgnoreMSB]; ok {
		if si.msbIgnore, err = strconv.ParseUint(s[0], 10, 64); err != nil {
			if gocqlDebug {
				Logger.Printf("scylla: failed to parse %s value %v: %s", scyllaShardingIgnoreMSB, s, err)
			}
		}
	}

	var (
		partitioner string
		algorithm   string
	)
	if s, ok := supported[scyllaPartitioner]; ok {
		partitioner = s[0]
	}
	if s, ok := supported[scyllaShardingAlgorithm]; ok {
		algorithm = s[0]
	}

	if partitioner != "org.apache.cassandra.dht.Murmur3Partitioner" || algorithm != "biased-token-round-robin" || si.nrShards == 0 || si.msbIgnore == 0 {
		if gocqlDebug {
			Logger.Printf("scylla: unsupported sharding configuration")
		}
		return scyllaSupported{}
	}

	return si
}

// isScyllaConn checks if conn is suitable for scyllaConnPicker.
func isScyllaConn(conn *Conn) bool {
	s := parseSupported(conn.supported)
	return s.nrShards != 0
}

// scyllaConnPicker is a specialised ConnPicker that selects connections based
// on token trying to get connection to a shard containing the given token.
type scyllaConnPicker struct {
	conns     []*Conn
	nrConns   int
	nrShards  int
	msbIgnore uint64
	pos       int
	mu        sync.RWMutex
}

func newScyllaConnPicker(conn *Conn) *scyllaConnPicker {
	s := parseSupported(conn.supported)
	if s.nrShards == 0 {
		panic(fmt.Sprintf("scylla: %s not a sharded connection", conn.Address()))
	}

	if gocqlDebug {
		Logger.Printf("scylla: %s sharding options %+v", conn.Address(), s)
	}

	return &scyllaConnPicker{
		nrShards:  s.nrShards,
		msbIgnore: s.msbIgnore,
	}
}

func (p *scyllaConnPicker) Remove(conn *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := parseSupported(conn.supported)
	if s.nrShards == 0 {
		panic(fmt.Sprintf("scylla: %s not a sharded connection", conn.Address()))
	}
	if gocqlDebug {
		Logger.Printf("scylla: %s remove shard %d connection", conn.Address(), s.shard)
	}
	p.conns[s.shard] = nil
}

func (p *scyllaConnPicker) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	conns := p.conns
	p.conns = nil
	for _, conn := range conns {
		if conn != nil {
			conn.Close()
		}
	}
}

func (p *scyllaConnPicker) Size() (int, int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nrConns, p.nrShards - p.nrConns
}

func (p *scyllaConnPicker) Pick(t token) *Conn {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.conns) == 0 {
		return nil
	}

	if t == nil {
		end := p.pos // avoid endless loops
		for {
			p.pos = (p.pos + 1) % len(p.conns)
			if conn := p.conns[p.pos]; conn != nil {
				return conn
			}
			if p.pos == end {
				return nil
			}
		}
	}

	mmt, ok := t.(murmur3Token)
	// double check if that's murmur3 token
	if !ok {
		return nil
	}

	idx := p.shardOf(mmt)

	return p.conns[idx]
}

func (p *scyllaConnPicker) shardOf(token murmur3Token) int {
	shards := uint64(p.nrShards)
	z := uint64(token+math.MinInt64) << p.msbIgnore
	lo := z & 0xffffffff
	hi := (z >> 32) & 0xffffffff
	mul1 := lo * shards
	mul2 := hi * shards
	sum := (mul1 >> 32) + mul2
	return int(sum >> 32)
}

func (p *scyllaConnPicker) Put(conn *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := parseSupported(conn.supported)
	if s.nrShards == 0 {
		panic(fmt.Sprintf("scylla: %s not a sharded connection", conn.Address()))
	}

	if s.nrShards != len(p.conns) {
		if s.nrShards != p.nrShards {
			panic(fmt.Sprintf("scylla: %s invalid number of shards", conn.Address()))
		}
		conns := p.conns
		p.conns = make([]*Conn, s.nrShards, s.nrShards)
		copy(p.conns, conns)
	}
	if c := p.conns[s.shard]; c != nil {
		conn.Close()
		return
	}
	p.conns[s.shard] = conn
	p.nrConns++
	if gocqlDebug {
		Logger.Printf("scylla: %s put shard %d connection total: %d missing: %d", conn.Address(), s.shard, p.nrConns, p.nrShards-p.nrConns)
	}
}
