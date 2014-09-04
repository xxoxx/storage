package db

import (
	"testing"
	"time"
)

var storage KeyStorage

func init() {
	storage = LoadStorage("+syZJnfrbquJpdlfBn2QFHfgjC2KM/q+", "../test.out")
}

func TestStorageUpdate(t *testing.T) {
	storage.keyMap = map[string]string{}
	storage.Update("name", "vincenting")
	time.Sleep(time.Microsecond * 100)
	if result, _ := storage.Get("name"); result != "vincenting" {
		t.Log(storage.Get("name"))
		t.Error("Value changed unexcepted")
	}
}

func TestLock(t *testing.T) {
	storage.keyMap = map[string]string{}
	go func() {
		name, ok := storage.Get("name")
		if ok && name == "vt1" {
			t.Log(name)
			t.Error("Upate happened before get")
		}
	}()
	go func() {
		storage.Update("name", "vt1")
	}()
}

func TestFunc(t *testing.T) {
	storage.keyMap = map[string]string{}
	storage.Update("name", "vt")
	if result, _ := storage.Get("name"); result != "vt" {
		t.Error("Update work unexpected")
	}
	storage.Destroy("name")
	if _, ok := storage.Get("name"); ok {
		t.Error("Destroy work unexpected")
	}
}

func TestFilter(t *testing.T) {
	storage.keyMap = map[string]string{}
	storage.Update("name:vt", "Vincent")
	storage.Update("notname:at", "Alvin")
	if storage.Keys("name:")[0] != "name:vt" {
		t.Log(storage.Keys("name:"))
		t.Error("Keys work unexpected")
	}
}

func TestSaddAndSMembers(t *testing.T) {
	storage.keyMap = map[string]string{}
	storage.SAdd("test", "1")
	storage.SAdd("test", "1")
	storage.SAdd("test", "1")
	storage.SAdd("test", "2")
	if storage.SMembers("test")[0] != "1" {
		t.Log(storage.SMembers("test"))
		t.Error("SAdd work unexpected")
	}
	if storage.SSize("test") != 2 {
		t.Log(storage.SMembers("test"))
		t.Error("SSize work unexpected")
	}
}

func TestSRem(t *testing.T) {
	storage.keyMap = map[string]string{}
	storage.SAdd("test", "1")
	storage.SRem("test", "1")
	if storage.SSize("test") != 0 {
		t.Log(storage.SMembers("test"))
		t.Error("SSize work unexpected")
	}
}

func TestPush(t *testing.T) {
	storage.keyMap = map[string]string{}
	storage.SAdd("test", "1")
	storage.LPush("test", []string{"-1", "0"})
	storage.RPush("test", []string{"2", "3"})
	if storage.SSize("test") != 5 {
		t.Log(storage.SMembers("test"))
		t.Error("push work unexpected")
	}
	if storage.SMembers("test")[0] != "-1" && storage.SMembers("test")[4] != "3" {
		t.Log(storage.SMembers("test"))
		t.Error("push work unexpected")
	}
}

func TestPop(t *testing.T) {
	storage.keyMap = map[string]string{}
	storage.LPush("test", []string{"-1", "0"})
	storage.RPush("test", []string{"2", "3"})
	reply, _ := storage.BLPOP("test", time.Microsecond)
	if reply != "-1" {
		t.Log(storage.SMembers("test"))
		t.Error("pop work unexpected")
	}
}

func TestPopWithBlock(t *testing.T) {
	storage.keyMap = map[string]string{}
	go func() {
		wait := time.NewTimer(time.Millisecond * 100)
		<-wait.C
		storage.LPush("test", []string{"-1", "0"})
		storage.RPush("test", []string{"2", "3"})
	}()
	reply, err := storage.BLPOP("test", time.Second)
	if reply != "-1" {
		t.Log(storage.SMembers("test"), err)
		t.Error("BLOCK work unexpected")
	}
}

func TestPopWithBlockAndTimeout(t *testing.T) {
	storage.keyMap = map[string]string{}
	go func() {
		wait := time.NewTimer(time.Millisecond * 100)
		<-wait.C
		storage.LPush("test", []string{"-1", "0"})
		storage.RPush("test", []string{"2", "3"})
	}()
	_, err := storage.BLPOP("test", time.Millisecond)
	if err == nil {
		t.Log(storage.SMembers("test"), err)
		t.Error("BLOCK work unexpected")
	}
}

func TestClean(t *testing.T) {
	storage.keyMap = map[string]string{}
}
