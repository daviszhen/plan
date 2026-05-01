package util

type Bitmap struct {
	Bits []uint8
}

func (bm *Bitmap) Data() []uint8 {
	return bm.Bits
}

func (bm *Bitmap) Bytes(count int) int {
	return EntryCount(count)
}

func (bm *Bitmap) Init(count int) {
	cnt := EntryCount(count)
	bm.Bits = GAlloc.Alloc(cnt)
	for i := range bm.Bits {
		bm.Bits[i] = 0xFF
	}
}

func (bm *Bitmap) ShareWith(other *Bitmap) {
	bm.Bits = other.Bits
}

func (bm *Bitmap) Invalid() bool {
	return len(bm.Bits) == 0
}

func (bm *Bitmap) GetEntry(eIdx uint64) uint8 {
	if bm.Invalid() {
		return 0xFF
	}
	return bm.Bits[eIdx]
}

func GetEntryIndex(idx uint64) (uint64, uint64) {
	return idx / 8, idx % 8
}

func EntryIsSet(e uint8, pos uint64) bool {
	return e&(1<<pos) != 0
}

func (bm *Bitmap) Combine(other *Bitmap, count int) {
	if other.AllValid() {
		return
	}
	if bm.AllValid() {
		bm.ShareWith(other)
		return
	}
	oldData := bm.Bits
	bm.Init(count)
	eCnt := EntryCount(count)
	for i := 0; i < eCnt; i++ {
		bm.Bits[i] = oldData[i] & other.Bits[i]
	}
}

func RowIsValidInEntry(e uint8, pos uint64) bool {
	return EntryIsSet(e, pos)
}

func (bm *Bitmap) RowIsValidUnsafe(idx uint64) bool {
	eIdx, pos := GetEntryIndex(idx)
	e := bm.GetEntry(eIdx)
	return EntryIsSet(e, pos)
}

func (bm *Bitmap) RowIsValid(idx uint64) bool {
	if bm.Invalid() {
		return true
	}
	return bm.RowIsValidUnsafe(idx)
}

func (bm *Bitmap) SetValid(ridx uint64) {
	if bm.Invalid() {
		return
	}
	bm.SetValidUnsafe(ridx)
}

func (bm *Bitmap) Set(ridx uint64, valid bool) {
	if valid {
		bm.SetValid(ridx)
	} else {
		bm.SetInvalid(ridx)
	}
}

func (bm *Bitmap) SetValidUnsafe(ridx uint64) {
	eIdx, pos := GetEntryIndex(ridx)
	bm.Bits[eIdx] |= 1 << pos
}

func (bm *Bitmap) SetInvalid(ridx uint64) {
	if bm.Invalid() {
		bm.Init(DefaultVectorSize)
	}
	bm.SetInvalidUnsafe(ridx)
}

func (bm *Bitmap) SetInvalidUnsafe(ridx uint64) {
	eIdx, pos := GetEntryIndex(ridx)
	bm.Bits[eIdx] &= ^(1 << pos)
}

func (bm *Bitmap) Reset() {
	bm.Bits = nil
}

func EntryCount(cnt int) int {
	return (cnt + 7) / 8
}

func SizeInBytes(cnt int) int {
	return EntryCount(cnt)
}

func (bm *Bitmap) Resize(old int, new int) {
	if new <= old {
		return
	}
	if bm.Bits != nil {
		ncnt := EntryCount(new)
		ocnt := EntryCount(old)
		newData := GAlloc.Alloc(ncnt)
		copy(newData, bm.Bits)
		for i := ocnt; i < ncnt; i++ {
			newData[i] = 0xFF
		}
		bm.Bits = newData
	} else {
		bm.Init(new)
	}
}

func (bm *Bitmap) PrepareSpace(cnt int) {
	if bm.Invalid() {
		bm.Init(int(cnt))
	}
}

func (bm *Bitmap) SetAllInvalid(cnt int) {
	bm.PrepareSpace(cnt)
	if cnt == 0 {
		return
	}
	lastEidx := EntryCount(int(cnt)) - 1
	for i := 0; i < lastEidx; i++ {
		bm.Bits[i] = 0
	}
	lastBits := cnt % 8
	if lastBits == 0 {
		bm.Bits[lastEidx] = 0
	} else {
		bm.Bits[lastEidx] = 0xFF << lastBits
	}
}
func NoneValidInEntry(entry uint8) bool {
	return entry == 0
}

func AllValidInEntry(entry uint8) bool {
	return entry == 0xFF
}

func (bm *Bitmap) AllValid() bool {
	return bm.Invalid()
}

func (bm *Bitmap) CopyFrom(other *Bitmap, count int) {
	if other.AllValid() {
		bm.Bits = nil
	} else {
		eCnt := EntryCount(count)
		bm.Bits = make([]uint8, eCnt)
		copy(bm.Bits, other.Bits[:eCnt])
	}
}

func (bm *Bitmap) SetAllValid(cnt int) {
	bm.PrepareSpace(cnt)
	if cnt == 0 {
		return
	}
	lastEidx := EntryCount(int(cnt)) - 1
	for i := 0; i < lastEidx; i++ {
		bm.Bits[i] = 0xFF
	}
	lastBits := cnt % 8
	if lastBits == 0 {
		bm.Bits[lastEidx] = 0xFF
	} else {
		bm.Bits[lastEidx] = ^(0xFF << lastBits)
	}
}

func (bm *Bitmap) IsMaskSet() bool {
	return bm.Bits != nil
}

func (bm *Bitmap) Slice(
	other *Bitmap,
	sourceOffset uint64,
	count uint64) {
	if other.AllValid() {
		bm.Bits = nil
		return
	}
	if sourceOffset == 0 {
		bm.Init2(other)
		return
	}

	newBm := &Bitmap{}
	newBm.Init(int(count))
	newBm.SliceInPlace(other, 0, sourceOffset, count)
	bm.Init2(newBm)
}

func (bm *Bitmap) Init2(other *Bitmap) {
	bm.Bits = other.Bits
}

func (bm *Bitmap) SliceInPlace(
	other *Bitmap, targetOffset, sourceOffset, count uint64) {
	for i := uint64(0); i < count; i++ {
		bm.Set(targetOffset+i,
			other.RowIsValid(sourceOffset+i))
	}
}
