package db

// Get key from memery
func (k *KeyStorage) Get(key string) (result string, ok bool) {
	k.execLock.Lock()
	defer k.execLock.Unlock()
	result, ok = k.keyMap[key]
	return
}

// delete a key and update storage file
func (k *KeyStorage) Destroy(key string) {
	k.execLock.Lock()
	defer k.execLock.Unlock()
	delete(k.keyMap, key)
	go k.dump()
}

// update/create a key and update storage file
func (k *KeyStorage) Update(key string, value string) {
	k.execLock.Lock()
	defer k.execLock.Unlock()
	k.keyMap[key] = value
	go k.dump()
}
