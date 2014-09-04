package db

import (
	"errors"
	"sync"
	"time"
)

func (k *KeyStorage) listNotify(key string) {
	blockLock, ok := k.chanBlockSet[key]
	if ok {
		blockLock.Lock()
		blockChan, ok := k.blockSet[key]
		if ok {
			blockChan <- true
		} else {
			blockLock.Unlock()
		}
	}
}

// Add items to Array left
func (k *KeyStorage) LPush(key string, items []string) (size int) {
	k.execLock.Lock()
	defer k.execLock.Unlock()
	currentItems := k.sMembers(key)
	k.setList(key, append(items, currentItems...))
	k.listNotify(key)
	size = len(k.sMembers(key))
	return
}

// Add items to Array right
func (k *KeyStorage) RPush(key string, items []string) (size int) {
	k.execLock.Lock()
	defer k.execLock.Unlock()
	currentItems := k.sMembers(key)
	k.setList(key, append(currentItems, items...))
	k.listNotify(key)
	size = len(k.sMembers(key))
	return
}

// Remove and return from the left. return err if not found
func (k *KeyStorage) lPop(key string, withLock bool) (item string, err error) {
	if withLock {
		k.execLock.Lock()
		defer k.execLock.Unlock()
	}
	currentItems := k.sMembers(key)
	if len(currentItems) > 0 {
		item = currentItems[0]
		k.setList(key, currentItems[1:])
		return
	}
	err = errors.New("No record found")
	return
}

// Remove and return from the left. return err if timeout
func (k *KeyStorage) BLPOP(key string, timeout time.Duration) (item string, err error) {
	defer func() {
		blockChan, ok := k.blockSet[key]
		if ok {
			close(blockChan)
			delete(k.blockSet, key)
		}
		delete(k.chanBlockSet, key)
	}()
	item, err = k.lPop(key, true)
	if err == nil {
		return
	}
	// wait new item push or timeout
	k.blockSet[key] = make(chan bool)
	k.chanBlockSet[key] = &sync.Mutex{}
	select {
	// return result if new item found
	case <-k.blockSet[key]:
		k.chanBlockSet[key].Unlock()
		return k.lPop(key, false)
	// timeout otherwise
	case <-time.After(timeout):
		err = errors.New("Timeout")
		return
	}
}
