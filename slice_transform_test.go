package gorocksdb

import (
	"log"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	rdb        *DB
	rdbClosed  bool
	dbpath     string
	rdbOptions *Options
)

func setup() {
	dbpath := os.TempDir() + "/TestSliceTransformInconsistency"
	transform := NewFixedPrefixTransform(3)

	rdbOptions := NewDefaultOptions()
	rdbOptions.SetPrefixExtractor(transform)
	rdbOptions.SetHashSkipListRep(50000, 4, 4)
	rdbOptions.SetAllowMmapReads(true)
	rdbOptions.SetAllowMmapWrites(true)
	rdbOptions.SetPlainTableFactory(4, 10, 0.75, 16)
	rdbOptions.SetCreateIfMissing(true)

	var err error
	rdb, err = OpenDb(rdbOptions, dbpath)
	if err != nil {
		log.Printf("Setup error, can not opendb: %s", err)
	}
}

func teardown() {
	if !rdbClosed {
		closeDB()
		err := DestroyDb(dbpath, rdbOptions)
		if err != nil {
			log.Printf("fail on destroy rdb: %s", err)
		}
	}
}

func closeDB() {
	rdb.Close()
	rdbClosed = true
}

type testSliceTransform struct {
	initiated bool
}

func (self *testSliceTransform) Transform(src []byte) []byte {
	return src[0:3]
}

func (self *testSliceTransform) InDomain(src []byte) bool {
	return len(src) >= 3
}

func (self *testSliceTransform) InRange(src []byte) bool {
	return len(src) == 3
}

func (self *testSliceTransform) Name() string {
	self.initiated = true
	return "gorocksdb.test"
}

func TestCustomSliceTransform(t *testing.T) {
	dbName := os.TempDir() + "/TestNewSliceTransform"

	Convey("Subject: Prefix filtering with custom slice transform", t, func() {
		sliceTransform := &testSliceTransform{}

		Convey("The old Iteration", func() {
			options := NewDefaultOptions()
			DestroyDb(dbName, options)

			options.SetPrefixExtractor(sliceTransform)
			options.SetHashSkipListRep(50000, 4, 4)
			options.SetAllowMmapReads(true)
			options.SetAllowMmapWrites(true)
			options.SetPlainTableFactory(4, 10, 0.75, 16)
			options.SetCreateIfMissing(true)

			db, err := OpenDb(options, dbName)
			defer db.Close()

			So(err, ShouldBeNil)

			wo := NewDefaultWriteOptions()
			So(db.Put(wo, []byte("foo1"), []byte("foo")), ShouldBeNil)
			So(db.Put(wo, []byte("foo2"), []byte("foo")), ShouldBeNil)
			So(db.Put(wo, []byte("foo3"), []byte("foo")), ShouldBeNil)
			So(db.Put(wo, []byte("bar1"), []byte("bar")), ShouldBeNil)
			So(db.Put(wo, []byte("bar2"), []byte("bar")), ShouldBeNil)
			So(db.Put(wo, []byte("bar3"), []byte("bar")), ShouldBeNil)

			ro := NewDefaultReadOptions()

			it := db.NewIterator(ro)
			defer it.Close()
			numFound := 0
			for it.Seek([]byte("bar")); it.Valid(); it.Next() {
				numFound++
			}

			So(it.Err(), ShouldBeNil)
			So(numFound, ShouldEqual, 3)
		})

		Convey("Iteration without destroy first, reopen old db", func() {
			options := NewDefaultOptions()
			defer DestroyDb(dbName, options)

			options.SetPrefixExtractor(sliceTransform)
			options.SetHashSkipListRep(50000, 4, 4)
			options.SetAllowMmapReads(true)
			options.SetAllowMmapWrites(true)
			options.SetPlainTableFactory(4, 10, 0.75, 16)
			options.SetCreateIfMissing(true)

			db, err := OpenDb(options, dbName)
			defer db.Close()

			So(err, ShouldBeNil)

			ro := NewDefaultReadOptions()

			it := db.NewIterator(ro)
			defer it.Close()
			numFound := 0
			for it.Seek([]byte("bar")); it.Valid(); it.Next() {
				numFound++
			}

			So(it.Err(), ShouldBeNil)
			So(numFound, ShouldEqual, 3)
		})

	})
}

func TestFixedPrefixTransform(t *testing.T) {
	dbName := os.TempDir() + "/TestNewFixedPrefixTransform"

	Convey("Subject: Prefix filtering with end condition checking", t, func() {
		options := NewDefaultOptions()
		DestroyDb(dbName, options)

		options.SetHashSkipListRep(50000, 4, 4)
		options.SetAllowMmapReads(true)
		options.SetAllowMmapWrites(true)
		options.SetPlainTableFactory(4, 10, 0.75, 16)
		options.SetCreateIfMissing(true)

		db, err := OpenDb(options, dbName)
		defer db.Close()

		So(err, ShouldBeNil)

		wo := NewDefaultWriteOptions()
		So(db.Put(wo, []byte("foo1"), []byte("foo")), ShouldBeNil)
		So(db.Put(wo, []byte("foo2"), []byte("foo")), ShouldBeNil)
		So(db.Put(wo, []byte("foo3"), []byte("foo")), ShouldBeNil)
		So(db.Put(wo, []byte("bar1"), []byte("bar")), ShouldBeNil)
		So(db.Put(wo, []byte("bar2"), []byte("bar")), ShouldBeNil)
		So(db.Put(wo, []byte("bar3"), []byte("bar")), ShouldBeNil)

		ro := NewDefaultReadOptions()

		it := db.NewIterator(ro)
		defer it.Close()
		numFound := 0
		prefix := []byte("bar")
		// Iterators must now be checked for passing the end condition
		// See https://github.com/facebook/rocksdb/wiki/Prefix-Seek-API-Changes
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			numFound++
		}

		So(it.Err(), ShouldBeNil)
		So(numFound, ShouldEqual, 3)
	})
}

func TestFixedPrefixTransformWithMax(t *testing.T) {
	setup()
	defer teardown()

	Convey("Subject: Prefix filtering with custom slice transform", t, func() {
		Convey("The old Iteration", func() {
			wo := NewDefaultWriteOptions()

			maxKey := []byte{
				0xFF, 0xFF, 0xFF, 0xFF,
			}
			So(rdb.Put(wo, maxKey, []byte("")), ShouldBeNil)

			So(rdb.Put(wo, []byte("foo1"), []byte("foo")), ShouldBeNil)
			So(rdb.Put(wo, []byte("foo2"), []byte("foo")), ShouldBeNil)
			So(rdb.Put(wo, []byte("foo3"), []byte("foo")), ShouldBeNil)
			So(rdb.Put(wo, []byte("bar1"), []byte("bar")), ShouldBeNil)
			So(rdb.Put(wo, []byte("bar2"), []byte("bar")), ShouldBeNil)
			So(rdb.Put(wo, []byte("bar3"), []byte("bar")), ShouldBeNil)

			ro := NewDefaultReadOptions()

			it := rdb.NewIterator(ro)
			defer it.Close()
			numFound := 0
			for it.Seek([]byte("bar")); it.Valid(); it.Next() {
				numFound++
			}

			So(it.Err(), ShouldBeNil)
			So(numFound, ShouldEqual, 3)
		})

		Convey("Iteration without destroy first, reopen old db", func() {
			// reopen
			closeDB()
			setup()

			ro := NewDefaultReadOptions()
			it := rdb.NewIterator(ro)
			defer it.Close()
			numFound := 0
			for it.Seek([]byte("bar")); it.Valid(); it.Next() {
				numFound++
			}

			So(it.Err(), ShouldBeNil)
			So(numFound, ShouldEqual, 3)
		})

		Convey("SeekToLast after reopen ", func() {
			// reopen
			closeDB()
			setup()
			defer teardown()

			ro := NewDefaultReadOptions()
			it := rdb.NewIterator(ro)
			defer it.Close()
			numFound := 0
			it.Seek([]byte("bar"))
			for it.SeekToLast(); it.Valid(); it.Prev() {
				numFound++
			}

			So(it.Err(), ShouldBeNil)
			So(numFound, ShouldEqual, 3)
		})
	})
}
