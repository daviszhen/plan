package main

type Vector interface {
}

type Chunk interface {
	Count() int
	GetVector(int) Vector
}

type Exec interface {
	Open() error
	GetNext() (Chunk, error)
	Close() error
}
