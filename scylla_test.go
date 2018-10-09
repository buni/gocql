package gocql

import (
	"testing"
)

func TestScyllaConnPickerRoundRobinForNilToken(t *testing.T) {
	s := scyllaConnPicker{
		nrShards:  4,
		msbIgnore: 12,
	}

	t.Run("no conns", func(t *testing.T) {
		s.conns = []*Conn{{}}
		if s.Pick(token(nil)) != s.conns[0] {
			t.Fatal("expected connection")
		}
	})

	t.Run("one shard", func(t *testing.T) {
		s.conns = []*Conn{{}}
		if s.Pick(token(nil)) != s.conns[0] {
			t.Fatal("expected connection")
		}
	})

	t.Run("multiple shards", func(t *testing.T) {
		s.conns = []*Conn{nil, {}}
		if s.Pick(token(nil)) != s.conns[1] {
			t.Fatal("expected connection")
		}
		if s.Pick(token(nil)) != s.conns[1] {
			t.Fatal("expected connection")
		}
	})

	t.Run("multiple shards no conns", func(t *testing.T) {
		s.conns = []*Conn{nil, nil}
		if s.Pick(token(nil)) != nil {
			t.Fatal("expected nil")
		}
		if s.Pick(token(nil)) != nil {
			t.Fatal("expected nil")
		}
	})

}

func TestScyllaConnPickerShardOf(t *testing.T) {
	s := scyllaConnPicker{
		nrShards:  4,
		msbIgnore: 12,
	}
	for _, test := range scyllaShardOfTests {
		if shard := s.shardOf(murmur3Token(test.token)); shard != test.shard {
			t.Errorf("wrong scylla shard calculated for token %d, expected %d, got %d", test.token, test.shard, shard)
		}
	}
}
