package storage

import (
	"fmt"

	"github.com/tidwall/btree"
)

const (
	DependTypeRegular   uint8 = 0
	DependTypeAutomatic uint8 = 1
	DependTypeOwns      uint8 = 2
	DependTypeOwnedBy   uint8 = 3
)

type DependItem struct {
	_dependTyp uint8
	_entry     *CatalogEntry
}

func dependItemLess(a, b *DependItem) bool {
	if a._dependTyp < b._dependTyp {
		return true
	}
	if a._dependTyp > b._dependTyp {
		return false
	}
	return catalogEntryLess(a._entry, b._entry)
}

type DependOnMeItem struct {
	_me      *CatalogEntry
	_onMeSet *btree.BTreeG[*DependItem]
}

func newDependOnMeItem(me *CatalogEntry) *DependOnMeItem {
	ret := &DependOnMeItem{
		_me:      me,
		_onMeSet: btree.NewBTreeG[*DependItem](dependItemLess),
	}

	return ret
}

type IDependToItem struct {
	_me    *CatalogEntry
	_toSet *btree.BTreeG[*CatalogEntry]
}

func newIDependToItem(me *CatalogEntry,
	set *btree.BTreeG[*CatalogEntry]) *IDependToItem {
	ret := &IDependToItem{
		_me:    me,
		_toSet: set,
	}

	return ret
}

type DependList struct {
	_set *btree.BTreeG[*CatalogEntry]
}

func NewDependList() *DependList {
	ret := &DependList{
		_set: btree.NewBTreeG[*CatalogEntry](catalogEntryLess),
	}
	return ret
}

func (list *DependList) AddDepend(ent *CatalogEntry) {
	list._set.Set(ent)
}

type DependMgr struct {
	_catalog        *Catalog
	_whoDependsOnMe *btree.BTreeG[*DependOnMeItem]
	_whoIDependOn   *btree.BTreeG[*IDependToItem]
}

func NewDependMgr(catalog *Catalog) *DependMgr {
	ret := &DependMgr{
		_catalog: catalog,
		_whoDependsOnMe: btree.NewBTreeG[*DependOnMeItem](
			func(a, b *DependOnMeItem) bool {
				return catalogEntryLess(a._me, b._me)
			}),
		_whoIDependOn: btree.NewBTreeG[*IDependToItem](
			func(a, b *IDependToItem) bool {
				return catalogEntryLess(a._me, b._me)
			}),
	}

	return ret
}

func (mgr *DependMgr) AddObject(
	txn *Txn,
	ent *CatalogEntry,
	list *DependList,
) error {
	var err error
	list._set.Scan(func(item *CatalogEntry) bool {
		if item._set == nil {
			err = fmt.Errorf("no set for depend")
			return false
		}
		ent2 := item._set.GetEntryInternal(txn, item._name, nil)
		if ent2 == nil {
			err = fmt.Errorf("depend may be deleted")
			return false
		}
		return true
	})
	if err != nil {
		return err
	}

	depTyp := DependTypeRegular
	list._set.Scan(func(item *CatalogEntry) bool {
		check := &DependOnMeItem{
			_me: item,
		}
		check2, has := mgr._whoDependsOnMe.Get(check)
		if !has {
			check3 := newDependOnMeItem(item)
			check2 = check3
			mgr._whoDependsOnMe.Set(check3)
		}
		check2._onMeSet.Set(&DependItem{
			_dependTyp: depTyp,
			_entry:     ent,
		})
		return true
	})
	mgr._whoDependsOnMe.Set(newDependOnMeItem(ent))
	mgr._whoIDependOn.Set(newIDependToItem(ent, list._set))
	return nil
}

func (mgr *DependMgr) EraseObject(ent *CatalogEntry) {
	ons, has := mgr._whoIDependOn.Get(&IDependToItem{
		_me: ent,
	})
	if has && ons._toSet != nil {
		ons._toSet.Scan(func(item *CatalogEntry) bool {
			//remove ent from those the ent depends on
			onMes, has2 := mgr._whoDependsOnMe.Get(&DependOnMeItem{
				_me: item,
			})
			if has2 && onMes._onMeSet != nil {
				item2 := &DependItem{
					_dependTyp: DependTypeRegular,
					_entry:     ent,
				}
				onMes._onMeSet.Delete(item2)
			}
			return true
		})
	}
	mgr._whoDependsOnMe.Delete(&DependOnMeItem{_me: ent})
	mgr._whoIDependOn.Delete(&IDependToItem{_me: ent})
}
