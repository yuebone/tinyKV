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
    "math/big"
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
    size int
}

func (sc *StoreCluster) getDb(id int) *memdb.DB {
    db, ok := sc.stores[id]
    if !ok {
        db = memdb.New(comparer.DefaultComparer, 10240)
        sc.stores[id] = db
    }

    return db
}

func calMiddleKey(begKey, endKey []byte) []byte {
    begInt, endInt := new(big.Int), new(big.Int)
    begInt.SetBytes(begKey)
    endInt.SetBytes(endKey)

    begInt.Div(begInt, big.NewInt(2))
    endInt.Div(endInt, big.NewInt(2))

    middleInt := new(big.Int)
    middleInt.Add(begInt, endInt)

    return middleInt.Bytes()
}

// when size of one region beyond a threshold, migrate half of the data to a small region
func (sc *StoreCluster) migrateRegion(metas Metas, srcIdx int) {
    pSrcRegion := &metas[srcIdx]
    srcDb := sc.getDb(pSrcRegion.ID)

    avgSize := sc.size / metaCount
    threshold := avgSize * 2
    if threshold < 4096 { threshold = 4096 }
    // not need to migrate
    if threshold >= srcDb.Len() { return }

    // 1. find a small region keep minimum size of data
    minRegionIdx := 0
    var minDb *memdb.DB
    minSize := sc.size + 1
    for i := range metas {
        db := sc.getDb(metas[i].ID)
        if db.Len() < minSize {
            minSize = db.Len()
            minRegionIdx = i
            minDb = db
            if db.Len() == 0 { break }
        }
    }

    // 2. find a neighbor region into where we can migrate the data of minRegion,
    //    so that we can get a empty region
    var neighborDb *memdb.DB
    if minRegionIdx == 0 {
        copy(metas[minRegionIdx+1].StartKey, metas[minRegionIdx].StartKey)
        neighborDb = sc.getDb(metas[minRegionIdx+1].ID)
    } else if minRegionIdx == metaCount-1 {
        copy(metas[minRegionIdx-1].EndKey, metas[minRegionIdx].EndKey)
        neighborDb = sc.getDb(metas[minRegionIdx-1].ID)
    } else {
        idx1, idx2 := minRegionIdx-1, minRegionIdx+1
        db1, db2 := sc.getDb(metas[idx1].ID), sc.getDb(metas[idx2].ID)
        if db1.Len() < db2.Len() {
            copy(metas[minRegionIdx-1].EndKey, metas[minRegionIdx].EndKey)
            neighborDb = db1
        } else {
            copy(metas[minRegionIdx+1].StartKey, metas[minRegionIdx].StartKey)
            neighborDb = db2
        }
    }

    // 3. merge the data of minRegion into neighborRegion, than clear minRegion
    it := minDb.NewIterator(&util.Range{metas[minRegionIdx].StartKey, metas[minRegionIdx].EndKey})
    for ok := it.First(); ok; ok = it.Next() {
        neighborDb.Put(it.Key(), it.Value())
    }
    it.Release()
    minDb.Reset()

    // 4. now migrate half of srcRegion into dstRegion/minRegion
    pDstRegion, dstDb := &metas[minRegionIdx], minDb
    middleKey := calMiddleKey(pSrcRegion.StartKey, pSrcRegion.EndKey)
    copy(pDstRegion.StartKey, pSrcRegion.StartKey)
    copy(pDstRegion.EndKey, middleKey)
    copy(pSrcRegion.StartKey, middleKey)

    it = srcDb.NewIterator(&util.Range{pDstRegion.StartKey, pDstRegion.EndKey})
    for ok := it.First(); ok; ok = it.Next() {
        dstDb.Put(it.Key(), it.Value())
        srcDb.Delete(it.Key())
    }
    it.Release()

    // since metas almost sorted, here can be optimized
    sort.Sort(metas)
}

func (sc *StoreCluster) InsertData(metas Metas, k, v []byte) error {
    region, idx := LocateKey(metas, k)
    db := sc.getDb(region.ID)

    err := db.Put(k, v)

    if err == nil {
        sc.size++
        // when size of one region beyond a threshold, migrate half of the data to a small region
        // this operation may delay the response of InsertData, but can be solved by putting the
        // migration in a background routine
        sc.migrateRegion(metas, idx)
    }

    return err
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
                    appendForValue(sc.getDb(metas[idx].ID), begKey, endKey, appendByte)
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

    lastDb, ok := sc.stores[metas[endIdx].ID]
    if !ok { return }

    if v, ok := lastDb.Get(endKey); ok == nil {
        newV := make([]byte, len(v), len(v)+1)
        newV = append(newV, appendByte)
        lastDb.Put(endKey, newV)
    }
}

func initMeta() Metas {
    rand.Seed(time.Now().Unix())

    startKey, endKey := make([]byte, keyLen), make([]byte, keyLen)
    memsetLoop(endKey, 0xff)

    metas := Metas{}
    metas = append(metas, Region{ID: 1, StartKey: startKey})
    for i := 2; i <= metaCount; i++ {
        nextKey := make([]byte, keyLen)
        rand.Read(nextKey)
        metas = append(metas, Region{ID: i, StartKey: nextKey})
    }
    sort.Sort(metas)
    metas[len(metas)-1].EndKey = endKey

    // set meta.EndKey to the next Region's StartKey.
    for i, _ := range metas {
        if i == len(metas)-1 {
            break
        }
        metas[i].EndKey = make([]byte, keyLen)
        copy(metas[i].EndKey, metas[i+1].StartKey)
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
        storeCluster.InsertData(metas, key, mockValue)
    }
}

func insertRandomData(storeCluster *StoreCluster, metas Metas) {
    key := make([]byte, keyLen)
    prefixDataCount := 500000
    for i := 0; i < prefixDataCount; i++ {
        rand.Read(key)
        mockValue := []byte("1")

        storeCluster.InsertData(metas, key, mockValue)
    }
}

func initStorage(metas Metas) *StoreCluster {
    storeCluster := &StoreCluster{
        stores: make(map[int]*memdb.DB),
        size: 0,
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
    fmt.Printf("\nafter update\n")
    updatedValue := []byte("10")

    sum := 0
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
        sum += num

        fmt.Printf("id:%v num:%v\n", region.ID, num)
        it.Release()
    }

    fmt.Printf("sum:%v\n", sum)

}
