package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_initNewBtreePage(t *testing.T) {
	type args struct {
		desc   *BTDesc
		blkno  Blkno
		flags  uint16
		level  uint16
		noLock bool
	}
	tests := []struct {
		name     string
		args     args
		prepare  func(arg *args)
		teardown func(arg *args)
		wantErr  assert.ErrorAssertionFunc
	}{
		{
			name: "test1",
			args: args{
				desc: &BTDesc{
					oids: RelOids{
						datoid:  1,
						reloid:  2,
						relnode: 3,
					},
					idxType: IndexRegular,
				},
				blkno:  0,
				flags:  BTREE_FLAG_LEAF,
				noLock: true,
			},
			prepare: func(arg *args) {
				InitPages(1)
			},
			teardown: func(arg *args) {
				ClosePages()
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				arg := i[0].(*args)
				pagePtr := GetInMemPage(arg.blkno)
				pageHeader := GetPageHeader(pagePtr)
				assert.Equal(t, arg.desc.oids.datoid, Oid(1))
				assert.Equal(t, arg.desc.oids.reloid, Oid(2))
				assert.Equal(t, arg.desc.oids.relnode, Oid(3))
				assert.Equal(t, arg.desc.idxType, IndexType(IndexRegular))
				assert.Equal(t, arg.blkno, Blkno(0))
				assert.Equal(t, uint32(1), pageHeader.usageCountAtomic)
				pageDesc := GetInMemPageDesc(arg.blkno)
				assert.Equal(t, arg.desc.oids.datoid, pageDesc.oids.datoid)
				assert.Equal(t, arg.desc.oids.reloid, pageDesc.oids.reloid)
				assert.Equal(t, arg.desc.oids.relnode, pageDesc.oids.relnode)

				btPageHeader := (*BTPageHeader)(pagePtr)
				assert.Equal(t, uint16(BTREE_FLAG_LEAF), btPageHeader.GetFlags())
				assert.Equal(t, uint16(0), btPageHeader.GetField1())
				assert.Equal(t, uint16(0), btPageHeader.GetField2())
				assert.Equal(t, uint64(InvalidRightLink), btPageHeader.rightLink)
				assert.Equal(t, COMMITSEQNO_FROZEN, btPageHeader.csn)
				assert.Equal(t, InvalidUndoLocation, btPageHeader.undoLocation)
				assert.Equal(t, MaxOffsetNumber, btPageHeader.prevInsertOffset)
				return assert.NoError(t, err)
			},
		},
		{
			name: "test2",
			args: args{
				desc: &BTDesc{
					oids: RelOids{
						datoid:  1,
						reloid:  2,
						relnode: 3,
					},
					idxType: IndexRegular,
				},
				blkno:  0,
				flags:  BTREE_FLAG_LEFTMOST,
				noLock: true,
				level:  1,
			},
			prepare: func(arg *args) {
				InitPages(1)
			},
			teardown: func(arg *args) {
				ClosePages()
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				arg := i[0].(*args)
				pagePtr := GetInMemPage(arg.blkno)
				pageHeader := GetPageHeader(pagePtr)
				assert.Equal(t, arg.desc.oids.datoid, Oid(1))
				assert.Equal(t, arg.desc.oids.reloid, Oid(2))
				assert.Equal(t, arg.desc.oids.relnode, Oid(3))
				assert.Equal(t, arg.desc.idxType, IndexType(IndexRegular))
				assert.Equal(t, arg.blkno, Blkno(0))
				assert.Equal(t, uint32(1), pageHeader.usageCountAtomic)
				pageDesc := GetInMemPageDesc(arg.blkno)
				assert.Equal(t, arg.desc.oids.datoid, pageDesc.oids.datoid)
				assert.Equal(t, arg.desc.oids.reloid, pageDesc.oids.reloid)
				assert.Equal(t, arg.desc.oids.relnode, pageDesc.oids.relnode)

				btPageHeader := (*BTPageHeader)(pagePtr)
				assert.Equal(t, uint16(BTREE_FLAG_LEFTMOST), btPageHeader.GetFlags())
				assert.Equal(t, uint16(1), btPageHeader.GetField1())
				assert.Equal(t, uint16(0), btPageHeader.GetField2())
				assert.Equal(t, uint64(InvalidRightLink), btPageHeader.rightLink)
				assert.Equal(t, COMMITSEQNO_FROZEN, btPageHeader.csn)
				assert.Equal(t, InvalidUndoLocation, btPageHeader.undoLocation)
				assert.Equal(t, MaxOffsetNumber, btPageHeader.prevInsertOffset)

				return assert.NoError(t, err)
			},
		},
	}
	for _, tt := range tests {
		if tt.prepare != nil {
			tt.prepare(&tt.args)
		}
		t.Run(tt.name,
			func(t *testing.T) {
				initNewBtreePage(tt.args.desc, tt.args.blkno, tt.args.flags, tt.args.level, tt.args.noLock)
				tt.wantErr(t, nil, &tt.args)
			})
		if tt.teardown != nil {
			tt.teardown(&tt.args)
		}
	}
}
