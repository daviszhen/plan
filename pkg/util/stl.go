package util

func Back[T any](data []T) T {
	l := len(data)
	if l == 0 {
		panic("empty slice")
	} else if l == 1 {
		return data[0]
	}
	return data[l-1]
}

func Size[T any](data []T) int {
	return len(data)
}

func Empty[T any](data []T) bool {
	return Size(data) == 0
}

func FindIf[T any](data []T, pred func(t T) bool) int {
	for i, ele := range data {
		if pred(ele) {
			return i
		}
	}
	return -1
}

// RemoveIf removes the one that pred is true.
func RemoveIf[T any](data []T, pred func(t T) bool) []T {
	if len(data) == 0 {
		return data
	}
	res := 0
	for i := 0; i < len(data); i++ {
		if !pred(data[i]) {
			if res != i {
				data[res] = data[i]
			}
			res++
		}
	}
	return data[:res]
}

func Erase[T any](a []T, i int) []T {
	if i < 0 || i >= len(a) {
		return a
	}
	a[i], a[len(a)-1] = a[len(a)-1], a[i]
	return a[:len(a)-1]
}

func Swap[T any](a []T, i, j int) {
	if i >= 0 && i < len(a) && j >= 0 && j < len(a) {
		t := a[i]
		a[i] = a[j]
		a[j] = t
	}
}

func Pop[T any](a []T) []T {
	if len(a) > 0 {
		return a[:len(a)-1]
	}
	return a
}

func CopyTo[T any](src []T) []T {
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}
