package db

import (
	"log"

	"github.com/ugorji/go/codec"
)

// GET All items of a given key
func (k *KeyStorage) SMembers(key string) (items []string) {
	k.execLock.Lock()
	defer k.execLock.Unlock()
	return k.sMembers(key)
}

// Return current length of this key
func (k *KeyStorage) SSize(key string) (size int) {
	k.execLock.Lock()
	defer k.execLock.Unlock()
	return len(k.sMembers(key))
}

// update key with list
func (k *KeyStorage) setList(key string, valueArr []string) {
	if len(valueArr) == 0 {
		delete(k.keyMap, key)
		go k.dump()
		return
	}
	newValue := []byte{}
	enc := codec.NewEncoderBytes(&newValue, &mh)
	enc.Encode(valueArr)
	k.keyMap[key] = string(newValue)
	go k.dump()
}
func (k *KeyStorage) sMembers(key string) (items []string) {
	result, ok := k.keyMap[key]
	if !ok {
		return
	}
	dec := codec.NewDecoderBytes([]byte(result), &mh)
	if err := dec.Decode(&items); err != nil {
		log.Fatal("Key storage file has been changed unexpeceted.")
	}
	return
}

// Add item if not exist
func (k *KeyStorage) SAdd(key string, value string) {
	k.execLock.Lock()
	defer k.execLock.Unlock()
	currentItems := k.sMembers(key)
	for _, item := range currentItems {
		if item == value {
			return
		}
	}
	k.setList(key, append(currentItems, value))
}

// Remove item if exist
func (k *KeyStorage) SRem(key string, value string) {
	k.execLock.Lock()
	defer k.execLock.Unlock()
	currentItems := k.sMembers(key)
	resultList := []string{}
	for _, item := range currentItems {
		if item != value {
			resultList = append(resultList, item)
		}
	}
	k.setList(key, resultList)
	return
}
