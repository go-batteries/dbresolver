package dbresolver_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/go-batteries/dbresolver"
	"golang.org/x/sync/singleflight"
)

type testState struct {
	val int
}

func (ts *testState) add() {
	ts.val += 1
}

type testStateQoalesced struct {
	val     int32
	blocker chan struct{}
	koala   *dbresolver.QueryKoalescer
}

func (ts *testStateQoalesced) add(idx int) <-chan singleflight.Result {
	adder := func() (interface{}, error) {
		<-ts.blocker

		atomic.AddInt32(&ts.val, 1)
		return 1, nil
	}

	return ts.koala.DoChan("adder", adder)
}

func Test_KoalesceWithTimeEvictor(t *testing.T) {
	t.Run("without singleflight, method called twice", func(t *testing.T) {
		ts := &testState{val: 0}
		ts.add()
		ts.add()

		if ts.val == 2 {
			t.Log("success")
			return
		}

		t.Error("value should have incremented from 0 to 2")
	})

	t.Run("should get the data only once", func(t *testing.T) {
		ts := &testStateQoalesced{
			val:     0,
			blocker: make(chan struct{}),
			koala:   dbresolver.NewKoalescer(&dbresolver.NoopEvictor{}),
		}

		// Perform the calls concurrently
		r := ts.add(0)
		s := ts.add(1)

		close(ts.blocker)

		<-r
		<-s

		if ts.val == 1 {
			t.Log("success")
			return
		}

		t.Errorf("value should not have increased beyond 1 %d\n", ts.val)
	})
}

type result struct {
	val    interface{}
	shared bool
}

func Test_SingleFlight(t *testing.T) {
	g := new(singleflight.Group)

	block := make(chan struct{})
	// block2 := make(chan struct{})
	// logchan := make(chan result, 2)

	res1c := g.DoChan("key", func() (interface{}, error) {
		<-block
		fmt.Println("res1")

		return "func 1", nil
	})
	res2c := g.DoChan("key", func() (interface{}, error) {
		<-block
		fmt.Println("res2")

		return "func 2", nil
	})

	close(block)

	res1 := <-res1c
	res2 := <-res2c

	if res1.Val != res2.Val {
		t.Error("two calls should have returned same value")
	}

	if !res1.Shared || !res2.Shared {
		t.Error("atleast one should have returned a shared true")
	}

	// close(block2)
	//
	// prev := <-logchan
	// next := <-logchan
	//
	// close(logchan)
	//
	// if prev.val != next.val {
	// 	t.Errorf("two calls should have returned same value %v %v\n", prev, next)
	// }
}
