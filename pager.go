package main

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/willf/bitset"
)

const (
	// PAGE_SIZE is the size of a page in bytes.
	PAGE_SIZE uint64 = 1024

	// MAX_PAGE is the maximum number of pages in the database.
	MAX_MAGE uint64 = 1073741823
)

// page number. 0 denotes "not a page".
// it begins from 1.
type PageNo uint64

type PgHdr struct {
	pPager               *Pager //which pager belongs to
	pgno                 PageNo //page number
	nRef                 int
	pNextFree, pPrevFree *PgHdr //free list.
	pNextAll, pRevAll    *PgHdr //all pages list.
	inJournal            bool   //written to journal
	inCkpt               bool   //written to checkpoint journal
	dirty                bool   //page is dirty
}

func PGHDR_TO_DATA(P *PgHdr) unsafe.Pointer { // ((void*)(&(P)[1]))
	return unsafe.Pointer(P[1])
}

// #define DATA_TO_PGHDR(D)  (&((PgHdr*)(D))[-1])
// #define PGHDR_TO_EXTRA(P) ((void*)&((char*)(&(P)[1]))[SQLITE_PAGE_SIZE])

const (
	_UNLOCK    = 0
	_READLOCK  = 1
	_WRITELOCK = 2
)

type Pager struct {
	zFilename          string            //database file
	zJournal           string            //journal file
	fd, jfd            *os.File          //database file and journal file
	cpfd               *os.File          //checkpoint journal file
	dbSize             uint64            //number of pages in the database
	origDbSize         uint64            //dbSize before the current change
	nExtra             int               //number of bytes added to each in-memory page
	xDestructor        func()            //page destructor
	nPage              uint64            //number of pages in memory
	nRef               uint64            //number of pages that PgHdr.nRef > 0
	mxPage             uint64            //maximum number of pages to hold in cache
	nHit, nMiss, nOvfl uint64            //cache hits, missing, and lru overflows
	journalOpen        bool              //true if journal file is valid
	ckptOpen           bool              //true if checkpoint journal is open
	ckptInUse          bool              //true if in checkpoint
	noSync             bool              //do not sync to disk if true
	state              int               //_UNLOCK, _READLOCK or _WRITELOCK
	errMask            uint8             //error mask
	tempFile           bool              //zFilename is a temporary file
	readOnly           bool              //true if readonly database
	needSync           bool              //true if fsync() needed on the journal
	dirtyFile          bool              //true if database file has changed
	aInJournal         bitset.BitSet     //bitmap. true if page is in journal
	aInCkpt            bitset.BitSet     //bitmap. true if page is in checkpoint journal
	pFirst, pLast      *PgHdr            //free pages
	pAll               *PgHdr            //all pages
	aHash              map[PageNo]*PgHdr //page number -> PgHdr
}

const (
	ERR_FULL    uint8 = 0x01 //write failed
	ERR_MEM     uint8 = 0x02 //malloc failed
	ERR_LOCK    uint8 = 0x04 //error in locking protocol
	ERR_CORRUPT uint8 = 0x08 //database or journal corruption
	ERR_DISK    uint8 = 0x10 //general disk io error
)

type PageRecord struct {
	pgno  PageNo
	aData [PAGE_SIZE]byte //original data for page pgno
}

/*
Journal files begin with the following magic string.
*/
var (
	aJournalMagic = []byte{
		0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd4,
	}
)

var pager_refinfo_enable bool = false
var refInfoCnt int = 0

func REFINFO(p *PgHdr) {
	if !pager_refinfo_enable {
		return
	}
	fmt.Printf(
		"REFCNT: %4d addr=0x%08x nRef=%d\n",
		p.pgno, PGHDR_TO_DATA(p), p.nRef)
	refInfoCnt++
}
