package util

type Bitmap struct {
	bits CPtr
}

func (bm *Bitmap) Data() []uint8 {
	return bm.bits.Bytes()
}

func (bm *Bitmap) Bytes(count int) int {
	return EntryCount(count)
}

func (bm *Bitmap) Init(count int) {
	cnt := EntryCount(count)
	bm.bits = NewCPtr(cnt)
	b := bm.bits.Bytes()
	for i := range b {
		b[i] = 0xFF
	}
}

func (bm *Bitmap) Destroy() {
	bm.bits.Destroy()
}

func (bm *Bitmap) ShareWith(other *Bitmap) {
	bm.bits = other.bits.Borrow()
}

func (bm *Bitmap) Invalid() bool {
	return bm.bits.IsNil()
}

func (bm *Bitmap) GetEntry(eIdx uint64) uint8 {
	if bm.Invalid() {
		return 0xFF
	}
	return bm.bits.Bytes()[eIdx]
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
		eCnt := EntryCount(count)
		bm.bits = NewCPtr(eCnt)
		copy(bm.bits.Bytes(), other.bits.Bytes()[:eCnt])
		return
	}
	old := bm.bits.Transfer()
	bm.Init(count)
	eCnt := EntryCount(count)
	b := bm.bits.Bytes()
	ob := old.Bytes()
	otherB := other.bits.Bytes()
	for i := 0; i < eCnt; i++ {
		b[i] = ob[i] & otherB[i]
	}
	old.Destroy()
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
	bm.bits.Bytes()[eIdx] |= 1 << pos
}

func (bm *Bitmap) SetInvalid(ridx uint64) {
	if bm.Invalid() {
		bm.Init(DefaultVectorSize)
	}
	bm.SetInvalidUnsafe(ridx)
}

func (bm *Bitmap) SetInvalidUnsafe(ridx uint64) {
	eIdx, pos := GetEntryIndex(ridx)
	bm.bits.Bytes()[eIdx] &= ^(1 << pos)
}

func (bm *Bitmap) Reset() {
	if bm.bits.IsOwner() {
		bm.bits.Destroy()
	} else {
		bm.bits = CPtr{}
	}
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
	if !bm.Invalid() {
		ncnt := EntryCount(new)
		ocnt := EntryCount(old)
		oldBits := bm.bits.Transfer()
		bm.bits = NewCPtr(ncnt)
		b := bm.bits.Bytes()
		copy(b, oldBits.Bytes()[:ocnt])
		for i := ocnt; i < ncnt; i++ {
			b[i] = 0xFF
		}
		oldBits.Destroy()
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
	b := bm.bits.Bytes()
	lastEidx := EntryCount(int(cnt)) - 1
	for i := 0; i < lastEidx; i++ {
		b[i] = 0
	}
	lastBits := cnt % 8
	if lastBits == 0 {
		b[lastEidx] = 0
	} else {
		b[lastEidx] = 0xFF << lastBits
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
		bm.bits.Destroy()
		return
	}
	eCnt := EntryCount(count)
	bm.bits = NewCPtr(eCnt)
	copy(bm.bits.Bytes(), other.bits.Bytes()[:eCnt])
}

func (bm *Bitmap) SetAllValid(cnt int) {
	bm.PrepareSpace(cnt)
	if cnt == 0 {
		return
	}
	b := bm.bits.Bytes()
	lastEidx := EntryCount(int(cnt)) - 1
	for i := 0; i < lastEidx; i++ {
		b[i] = 0xFF
	}
	lastBits := cnt % 8
	if lastBits == 0 {
		b[lastEidx] = 0xFF
	} else {
		b[lastEidx] = ^(0xFF << lastBits)
	}
}

func (bm *Bitmap) IsMaskSet() bool {
	return !bm.bits.IsNil()
}

func (bm *Bitmap) Slice(
	other *Bitmap,
	sourceOffset uint64,
	count uint64) {
	if other.AllValid() {
		bm.bits.Destroy()
		return
	}
	if sourceOffset == 0 {
		bm.bits = other.bits.Borrow()
		return
	}

	bm.Init(int(count))
	bm.SliceInPlace(other, 0, sourceOffset, count)
}

func (bm *Bitmap) Init2(other *Bitmap) {
	bm.bits = other.bits.Borrow()
}

// BitmapFromBytes creates a Bitmap that borrows the given byte slice (non-owning).
// The caller must ensure the slice outlives the Bitmap.
func BitmapFromBytes(data []byte) Bitmap {
	if len(data) == 0 {
		return Bitmap{}
	}
	return Bitmap{bits: CPtr{ptr: BytesSliceToPointer(data), len: len(data), owns: false}}
}

func (bm *Bitmap) SliceInPlace(
	other *Bitmap, targetOffset, sourceOffset, count uint64) {
	for i := uint64(0); i < count; i++ {
		bm.Set(targetOffset+i,
			other.RowIsValid(sourceOffset+i))
	}
}
