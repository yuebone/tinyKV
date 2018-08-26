package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/pingcap/goleveldb/leveldb/iterator"
	"github.com/pingcap/goleveldb/leveldb/util"
	"sync"
)

const (
	metaCount = 128
	keyLen    = 64
	concurrentCount = 4
)

type Region struct {
	ID       int
	StartKey []byte
	EndKey   []byte
}

// store meta infomation of Store
type Metas []Region

// Implement Sort.Interface.
func (m Metas) Len() int      { return len(m) }
func (m Metas) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m Metas) Less(i, j int) bool {
	return bytes.Compare(m[i].StartKey, m[j].StartKey) < 0
}

func LocateKey(metas Metas, key []byte) (Region, int) {
	idx := sort.Search(len(metas), func(i int) bool {
		return bytes.Compare(metas[i].StartKey, key) > 0
	})
	if idx == 0 {
		return metas[idx], idx
	}
	return metas[idx-1], idx-1
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

// Seek the key in one region, the iterator returned can only end at region.EndKey
func (sc *StoreCluster) Seek(metas Metas, key []byte) (iterator.Iterator, error) {
	region, _ := LocateKey(metas, key)
	db, ok := sc.stores[region.ID]
	if !ok {
		return nil, memdb.ErrNotFound
	}

	it := db.NewIterator(&util.Range{key, region.EndKey})
	return it, nil
}

func appendForValue(db *memdb.DB, begKey, endKey []byte, appendByte byte){
	it := db.NewIterator(&util.Range{begKey, endKey})
	defer it.Release()

	for ok := it.First(); ok; ok = it.Next(){
		newV := make([]byte, len(it.Value()), len(it.Value())+1)
		copy(newV, it.Value())
		newV = append(newV, appendByte)

		db.Put(it.Key(), newV)
	}
}

func memsetLoop(a []byte, v byte) {
	for i := range a { a[i] = v }
}

// for each key with the keyPrefix, append a byte to it's value
func (sc *StoreCluster) appendForPrefixData(metas Metas, keyPrefix []byte, appendByte byte) {
	begKey := make([]byte, keyLen)
	endKey := make([]byte, keyLen)
	memsetLoop(begKey, 0x00)
	memsetLoop(endKey, 0xff)
	copy(begKey, keyPrefix)
	copy(endKey, keyPrefix)

	_, begIdx := LocateKey(metas, begKey)
	_, endIdx := LocateKey(metas, endKey)

	ch := make(chan int)
	wg := sync.WaitGroup{}
	wg.Add(concurrentCount)

	for i := 0; i < concurrentCount; i++ {
		go func(pWg *sync.WaitGroup) {
			for {
				if idx, ok := <-ch; ok {
					appendForValue(sc.stores[metas[idx].ID], begKey, endKey, appendByte)
				} else {
					break
				}
			}
			pWg.Done()
		}(&wg)
	}

	for i := begIdx; i <= endIdx; i++ {
		ch <- i
	}
	close(ch)
	wg.Wait()

	lastDb := sc.stores[metas[endIdx].ID]
	if v, ok := lastDb.Get(endKey); ok == nil {
		newV := make([]byte, len(v), len(v)+1)
		newV = append(newV, appendByte)
		lastDb.Put(endKey, newV)
	}
}

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
		region, _ := LocateKey(metas, key)

		storeCluster.InsertData(region.ID, key, mockValue)
	}
}

func insertRandomData(storeCluster *StoreCluster, metas Metas) {
	key := make([]byte, keyLen)
	prefixDataCount := 500000
	for i := 0; i < prefixDataCount; i++ {
		rand.Read(key)
		mockValue := []byte("1")
		region, _ := LocateKey(metas, key)

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
	prefix := []byte("table:id:1")

	begKey := make([]byte, keyLen)
	endKey := make([]byte, keyLen)
	memsetLoop(begKey, 0x00)
	memsetLoop(endKey, 0xff)
	copy(begKey, prefix)
	copy(endKey, prefix)

	_, begIdx := LocateKey(metas, begKey)
	_, endIdx := LocateKey(metas, endKey)

	// before update
	fmt.Printf("before update\n")

	for i := begIdx; i <= endIdx; i++ {
		region := metas[i]
		db := storeCluster.stores[region.ID]
		it := db.NewIterator(&util.Range{begKey, endKey})
		num := 0
		for ok := it.First(); ok; ok = it.Next(){
			num++
		}

		fmt.Printf("id:%v num:%v\n", region.ID, num)
		it.Release()
	}

	// update
	storeCluster.appendForPrefixData(metas, prefix, '0')

	// after update
	fmt.Printf("after update\n")
	updatedValue := []byte("10")

	for i := begIdx; i <= endIdx; i++ {
		region := metas[i]
		db := storeCluster.stores[region.ID]
		it := db.NewIterator(&util.Range{begKey, endKey})

		num := 0
		for ok := it.First(); ok; ok = it.Next(){
			num++
			if bytes.Compare(it.Value(), updatedValue) != 0 {
				fmt.Printf("expected_value:%v\n", updatedValue)
				fmt.Printf("id:%v unexpected_value:(%v, %v)\n", region.ID, it.Key(), it.Value())
				panic(nil)
			}
		}

		fmt.Printf("id:%v num:%v\n", region.ID, num)
		it.Release()
	}

}
