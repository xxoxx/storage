package db

import (
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/ugorji/go/codec"
)

var mh codec.MsgpackHandle

func init() {
	mh.MapType = reflect.TypeOf(map[string]interface{}(nil))
}

type KeyStorage struct {
	screct       string
	keyMap       map[string]string
	blockSet     map[string]chan bool
	chanBlockSet map[string]*sync.Mutex
	fileName     string
	fileLock     *sync.Mutex
	execLock     *sync.Mutex
}

func LoadStorage(screct string, storageName string) (storage KeyStorage) {
	storage = KeyStorage{
		screct,
		map[string]string{},
		map[string]chan bool{},
		map[string]*sync.Mutex{},
		storageName,
		&sync.Mutex{},
		&sync.Mutex{},
	}
	if _, err := os.Stat(storageName); err != nil {
		if os.IsNotExist(err) {
			file, err := os.Create(storageName)
			defer file.Close()
			if err != nil {
				log.Fatalf("Can not create key storage file with error: %s", err.Error())
			}
		}
		return
	}
	fileContent, err := ioutil.ReadFile(storageName)
	if err != nil {
		log.Fatalf("Can not read key storage file with error: %s", err.Error())
	}
	if len(fileContent) == 0 {
		return
	}
	decodedStorage := Decrypt([]byte(screct), fileContent)
	dec := codec.NewDecoderBytes(decodedStorage, &mh)
	if err := dec.Decode(&(storage.keyMap)); err != nil {
		log.Fatal("Key storage file has been changed unexpeceted.")
	}
	return
}

// write keyMap into file
func (k *KeyStorage) dump() (err error) {
	k.fileLock.Lock()
	defer k.fileLock.Unlock()
	storage := []byte{}
	enc := codec.NewEncoderBytes(&storage, &mh)
	err = enc.Encode(k.keyMap)
	if err != nil {
		return
	}
	encodedStorage := Encrypt([]byte(k.screct), storage)
	return ioutil.WriteFile(k.fileName, encodedStorage, 0644)
}

// filter keys in storage
func (k *KeyStorage) Keys(prefix string) (keyArr []string) {
	k.execLock.Lock()
	defer k.execLock.Unlock()
	for key, _ := range k.keyMap {
		if strings.HasPrefix(key, prefix) {
			keyArr = append(keyArr, key)
		}
	}
	return
}
