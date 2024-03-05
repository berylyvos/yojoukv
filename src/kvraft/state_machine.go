package kvraft

type InMemSM struct {
	table map[string]string
}

func NewInMemSM() *InMemSM {
	return &InMemSM{
		table: make(map[string]string),
	}
}

func (sm *InMemSM) Get(key string) (string, Err) {
	if val, ok := sm.table[key]; ok {
		return val, OK
	}
	return "", ErrNoKey
}

func (sm *InMemSM) Put(key, value string) Err {
	sm.table[key] = value
	return OK
}

func (sm *InMemSM) Append(key, value string) Err {
	sm.table[key] += value
	return OK
}
