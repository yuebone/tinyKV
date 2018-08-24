package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

// store meta infomation of Store
type Metas []Region

// Implement Sort.Interface.
func (m Metas) Len() int      { return len(m) }
func (m Metas) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m Metas) Less(i, j int) bool {
	return bytes.Compare(m[i].StartKey, m[j].StartKey) < 0
}

type StoreCluster struct {
	// Key of stores is ID of regions.
	stores map[int]*memdb.DB
}

func (sc *StoreCluster) InsertData(regionID int, k, v []byte) error {
	db, ok := sc.stores[regionID]
	if !ok {
		db = memdb.New(comparer.DefaultComparer, 10240)
		sc.stores[regionID] = db
	}

	return db.Put(k, v)
}

// Please implement this function.
func (sc *StoreCluster) Seek(key []byte) (iterator.Iterator, error) {
	return nil, nil
}

type Region struct {
	ID       int
	StartKey []byte
	EndKey   []byte
}

// LocateKey will returns the Region contains key.
func LocateKey(metas Metas, key []byte) Region {
	idx := sort.Search(len(metas), func(i int) bool {
		return bytes.Compare(metas[i].StartKey, key) > 0
	})
	if idx == 0 {
		return metas[idx]
	}
	return metas[idx-1]
}

const (
	metaCount = 128
	keyLen    = 64
)

func initMeta() Metas {
	rand.Seed(time.Now().Unix())

	metas := Metas{}
	metas = append(metas, Region{ID: 1, StartKey: nil})
	for i := 2; i <= metaCount; i++ {
		nextKey := make([]byte, keyLen)
		rand.Read(nextKey)
		metas = append(metas, Region{ID: i, StartKey: nextKey})
	}
	sort.Sort(metas)

	// set meta.EndKey to the next Region's StartKey.
	for i, _ := range metas {
		if i == len(metas)-1 {
			break
		}
		metas[i].EndKey = metas[i+1].StartKey
	}
	return metas
}

func insertPrefixData(storeCluster *StoreCluster, metas Metas) {
	prefix := []byte("table:id:1")
	key := make([]byte, keyLen)
	prefixDataCount := 100000
	for i := 0; i < prefixDataCount; i++ {
		copy(key, prefix)
		rand.Read(key[len(prefix):])

		mockValue := []byte("1")
		region := LocateKey(metas, key)

		storeCluster.InsertData(region.ID, key, mockValue)
	}
}

func insertRandomData(storeCluster *StoreCluster, metas Metas) {
	key := make([]byte, keyLen)
	prefixDataCount := 500000
	for i := 0; i < prefixDataCount; i++ {
		rand.Read(key)
		mockValue := []byte("1")
		region := LocateKey(metas, key)

		storeCluster.InsertData(region.ID, key, mockValue)
	}
}

func initStorage(metas Metas) *StoreCluster {
	storeCluster := &StoreCluster{
		stores: make(map[int]*memdb.DB),
	}

	// Insert prefix data.
	insertPrefixData(storeCluster, metas)
	// Insert random data.
	insertRandomData(storeCluster, metas)
	return storeCluster
}

func main() {
	metas := initMeta()
	storeCluster := initStorage(metas)

	// You can remove below code.
	var sum int
	for id, store := range storeCluster.stores {
		sum += store.Len()
		fmt.Printf("id:%v len:%v\n", id, store.Len())
	}
	fmt.Printf("sum:%v\n", sum)
}
