package kvraft

type InMemSM struct {
	Table map[string]string
}

func NewInMemSM() *InMemSM {
	return &InMemSM{
		Table: make(map[string]string),
	}
}

func (sm *InMemSM) Get(key string) (string, Err) {
	if val, ok := sm.Table[key]; ok {
		return val, OK
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
