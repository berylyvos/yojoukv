package shardkv

type InMemSM struct {
	Table  map[string]string
	Status ShardStatus
}

func NewInMemSM() *InMemSM {
	return &InMemSM{
		Table:  make(map[string]string),
		Status: ShardNormal,
	}
}

func (sm *InMemSM) Get(key string) (string, Err) {
	if value, ok := sm.Table[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (sm *InMemSM) Put(key, value string) Err {
	sm.Table[key] = value
	return OK
}

func (sm *InMemSM) Append(key, value string) Err {
	sm.Table[key] += value
	return OK
}

func (sm *InMemSM) copy() map[string]string {
	newSM := make(map[string]string)
	for k, v := range sm.Table {
		newSM[k] = v
	}
	return newSM
}

func (sm *InMemSM) copyFrom(table map[string]string) {
	for k, v := range table {
		sm.Table[k] = v
	}
}
