package main

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/bits-and-blooms/bitset"
)

const (
	// PAGE_SIZE is the size of a page in bytes.
	SQLITE_PAGE_SIZE int = 1024

	// MAX_PAGE is the maximum number of pages in the database.
	SQLITE_MAX_MAGE int = 1073741823
)

// page number. 0 denotes "not a page".
// it begins from 1.
type Pgno uint32

type PgHdr struct {
	pPager               *Pager //which pager belongs to
	pgno                 Pgno   //page number
	nRef                 int
	pNextFree, pPrevFree *PgHdr //free list.
	pNextAll, pRevAll    *PgHdr //all pages list.
	inJournal            bool   //written to journal
	inCkpt               bool   //written to checkpoint journal
	dirty                bool   //page is dirty
}

func PGHDR_TO_DATA(P *PgHdr) unsafe.Pointer { // ((void*)(&(P)[1]))
	u := unsafe.Pointer((uintptr(unsafe.Pointer(P))) + unsafe.Sizeof(PgHdr{}))
	return u
}

// #define DATA_TO_PGHDR(D)  (&((PgHdr*)(D))[-1])
// #define PGHDR_TO_EXTRA(P) ((void*)&((char*)(&(P)[1]))[SQLITE_PAGE_SIZE])

const (
	SQLITE_UNLOCK    = 0
	SQLITE_READLOCK  = 1
	SQLITE_WRITELOCK = 2
)

type Pager struct {
	zFilename           string          //database file
	zJournal            string          //journal file
	fd, jfd             *os.File        //database file and journal file
	cpfd                *os.File        //checkpoint journal file
	dbSize              int             //number of pages in the database
	origDbSize          int             //dbSize before the current change
	ckptSize, ckptJSize int             /* Size of database and journal at ckpt_begin() */
	nExtra              int             //number of bytes added to each in-memory page
	xDestructor         func()          //page destructor
	nPage               int             //number of pages in memory
	nRef                int             //number of pages that PgHdr.nRef > 0
	mxPage              int             //maximum number of pages to hold in cache
	nHit, nMiss, nOvfl  int             //cache hits, missing, and lru overflows
	journalOpen         bool            //true if journal file is valid
	ckptOpen            bool            //true if checkpoint journal is open
	ckptInUse           bool            //true if in checkpoint
	noSync              bool            //do not sync to disk if true
	state               uint8           //_UNLOCK, _READLOCK or _WRITELOCK
	errMask             uint8           //error mask
	tempFile            bool            //zFilename is a temporary file
	readOnly            bool            //true if readonly database
	needSync            bool            //true if fsync() needed on the journal
	dirtyFile           bool            //true if database file has changed
	aInJournal          bitset.BitSet   //bitmap. true if page is in journal
	aInCkpt             bitset.BitSet   //bitmap. true if page is in checkpoint journal
	pFirst, pLast       *PgHdr          //free pages
	pAll                *PgHdr          //all pages
	aHash               map[Pgno]*PgHdr //page number . PgHdr
}

const sizeOfPgno = int(unsafe.Sizeof(Pgno(0)))
const sizeOfPgRecord = int(unsafe.Sizeof(PageRecord{}))

const (
	PAGER_ERR_FULL    uint8 = 0x01 //write failed
	PAGER_ERR_MEM     uint8 = 0x02 //malloc failed
	PAGER_ERR_LOCK    uint8 = 0x04 //error in locking protocol
	PAGER_ERR_CORRUPT uint8 = 0x08 //database or journal corruption
	PAGER_ERR_DISK    uint8 = 0x10 //general disk io error
)

type PageRecord struct {
	pgno  Pgno
	aData [SQLITE_PAGE_SIZE]byte //original data for page pgno
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

/*
** Convert the bits in the pPager.errMask into an approprate
** return code.
 */
func pager_errcode(pPager *Pager) int {
	rc := SQLITE_OK
	if (pPager.errMask & PAGER_ERR_LOCK) != 0 {
		rc = SQLITE_PROTOCOL
	}
	if (pPager.errMask & PAGER_ERR_DISK) != 0 {
		rc = SQLITE_IOERR
	}
	if (pPager.errMask & PAGER_ERR_FULL) != 0 {
		rc = SQLITE_FULL
	}
	if (pPager.errMask & PAGER_ERR_MEM) != 0 {
		rc = SQLITE_NOMEM
	}
	if (pPager.errMask & PAGER_ERR_CORRUPT) != 0 {
		rc = SQLITE_CORRUPT
	}
	return rc
}

/*
** Find a page in the hash table given its page number.  Return
** a pointer to the page or NULL if not found.
 */
func pager_lookup(pPager *Pager, pgno Pgno) *PgHdr {
	if p, ok := pPager.aHash[pgno]; ok {
		return p
	}
	return nil
}

/*
** Unlock the database and clear the in-memory cache.  This routine
** sets the state of the pager back to what it was when it was first
** opened.  Any outstanding pages are invalidated and subsequent attempts
** to access those pages will likely result in a coredump.
 */
//func pager_reset(pPager *Pager) {
//	var pPg, pNext *PgHdr
//	for pPg = pPager.pAll; pPg != nil; pPg = pNext {
//		pNext = pPg.pNextAll
//	}
//	pPager.pFirst = nil
//	pPager.pLast = nil
//	pPager.pAll = nil
//	pPager.aHash = nil
//	pPager.nPage = 0
//	if pPager.state >= SQLITE_WRITELOCK {
//		sqlitepager_rollback(pPager)
//	}
//	sqliteOsUnlock(pPager.fd)
//	pPager.state = SQLITE_UNLOCK
//	pPager.dbSize = -1
//	pPager.nRef = 0
//	assertx(!pPager.journalOpen)
//}

/*
** Rollback all changes.  The database falls back to read-only mode.
** All in-memory cache pages revert to their original data contents.
** The journal is deleted.
**
** This routine cannot fail unless some other process is not following
** the correct locking protocol (SQLITE_PROTOCOL) or unless some other
** process is writing trash into the journal file (SQLITE_CORRUPT) or
** unless a prior malloc() failed (SQLITE_NOMEM).  Appropriate error
** codes are returned for all these occasions.  Otherwise,
** SQLITE_OK is returned.
 */
//func sqlitepager_rollback(pPager *Pager) int {
//	var rc int
//	if pPager.errMask != 0 && pPager.errMask != PAGER_ERR_FULL {
//		if pPager.state >= SQLITE_WRITELOCK {
//			pager_playback(pPager)
//		}
//		return pager_errcode(pPager)
//	}
//	if pPager.state != SQLITE_WRITELOCK {
//		return SQLITE_OK
//	}
//	rc = pager_playback(pPager)
//	if rc != SQLITE_OK {
//		rc = SQLITE_CORRUPT
//		pPager.errMask |= PAGER_ERR_CORRUPT
//	}
//	pPager.dbSize = -1
//	return rc
//}

/*
** Playback the journal and thus restore the database file to
** the state it was in before we started making changes.
**
** The journal file format is as follows:  There is an initial
** file-type string for sanity checking.  Then there is a single
** Pgno number which is the number of pages in the database before
** changes were made.  The database is truncated to this size.
** Next come zero or more page records where each page record
** consists of a Pgno and SQLITE_PAGE_SIZE bytes of data.  See
** the PageRecord structure for details.
**
** If the file opened as the journal file is not a well-formed
** journal file (as determined by looking at the magic number
** at the beginning) then this routine returns SQLITE_PROTOCOL.
** If any other errors occur during playback, the database will
** likely be corrupted, so the PAGER_ERR_CORRUPT bit is set in
** pPager.errMask and SQLITE_CORRUPT is returned.  If it all
** works, then this routine returns SQLITE_OK.
 */
//func pager_playback(pPager *Pager) int {
//	var nRec int       /* Number of Records */
//	var i int          /* Loop counter */
//	var mxPg Pgno = 0  /* Size of the original file in pages */
//	var aMagic [8]byte //sizeof(aJournalMagic)
//	var rc int
//
//	var mxPgBuf [sizeOfPgno]byte
//
//	/* Figure out how many records are in the journal.  Abort early if
//	 ** the journal is empty.
//	 */
//	assertx(pPager.journalOpen)
//	sqliteOsSeek(pPager.jfd, 0)
//	rc = sqliteOsFileSize(pPager.jfd, &nRec)
//	if rc != SQLITE_OK {
//		goto end_playback
//	}
//
//	nRec = (nRec - (len(aMagic) + sizeOfPgno)) / sizeOfPgRecord
//	if nRec <= 0 {
//		goto end_playback
//	}
//
//	/* Read the beginning of the journal and truncate the
//	 ** database file back to its original size.
//	 */
//	rc = sqliteOsRead(pPager.jfd, aMagic[:], len(aMagic))
//	if rc != SQLITE_OK || bytes.Compare(aMagic[:], aJournalMagic) != 0 {
//		rc = SQLITE_PROTOCOL
//		goto end_playback
//	}
//	rc = sqliteOsRead(pPager.jfd, mxPgBuf[:], sizeOfPgno)
//	if rc != SQLITE_OK {
//		goto end_playback
//	}
//	mxPg = Pgno(bin.LittleEndian.Uint64(mxPgBuf[:]))
//	rc = sqliteOsTruncate(pPager.fd, int(mxPg)*int(SQLITE_PAGE_SIZE))
//	if rc != SQLITE_OK {
//		goto end_playback
//	}
//	pPager.dbSize = int(mxPg)
//
//	/* Copy original pages out of the journal and back into the database file.
//	 */
//	for i = nRec - 1; i >= 0; i-- {
//		rc = pager_playback_one_page(pPager, pPager.jfd)
//		if rc != SQLITE_OK {
//			break
//		}
//	}
//
//end_playback:
//	if rc != SQLITE_OK {
//		//TODO: pager_unwritelock(pPager)
//		pPager.errMask |= PAGER_ERR_CORRUPT
//		rc = SQLITE_CORRUPT
//	} else {
//		//TODO: rc = pager_unwritelock(pPager)
//	}
//	return rc
//}

/*
** Read a single page from the journal file opened on file descriptor
** jfd.  Playback this one page.
 */
//func pager_playback_one_page(pPager *Pager, jfd *os.File) int {
//	var rc int
//	var pPg *PgHdr /* An existing page in the cache */
//	var pgRec PageRecord
//	pointer := []byte(unsafe.Pointer(&pgRec))
//	rc = sqliteOsRead(jfd, pgRec, sizeof(pgRec))
//	if rc != SQLITE_OK {
//		return rc
//	}
//
//	/* Sanity checking on the page */
//	if pgRec.pgno > pPager.dbSize || pgRec.pgno == 0 {
//		return SQLITE_CORRUPT
//	}
//
//	/* Playback the page.  Update the in-memory copy of the page
//	 ** at the same time, if there is one.
//	 */
//	pPg = pager_lookup(pPager, pgRec.pgno)
//	if pPg {
//		memcpy(PGHDR_TO_DATA(pPg), pgRec.aData, SQLITE_PAGE_SIZE)
//		memset(PGHDR_TO_EXTRA(pPg), 0, pPager.nExtra)
//	}
//	rc = sqliteOsSeek(&pPager.fd, (pgRec.pgno-1)*SQLITE_PAGE_SIZE)
//	if rc == SQLITE_OK {
//		rc = sqliteOsWrite(&pPager.fd, pgRec.aData, SQLITE_PAGE_SIZE)
//	}
//	return rc
//}

func assertx(b bool) {
	if !b {
		panic("must be true")
	}
}
