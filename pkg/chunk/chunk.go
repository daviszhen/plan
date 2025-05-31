package chunk

import (
	"errors"
	"fmt"
	"io"
	"os"

	wire "github.com/jeroenrinzema/psql-wire"
	"go.uber.org/zap"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type Chunk struct {
	Data  []*Vector
	Count int
	_Cap  int
}

func (c *Chunk) Init(types []common.LType, cap int) {
	c._Cap = cap
	c.Data = nil
	for _, lType := range types {
		c.Data = append(c.Data, NewVector2(lType, c._Cap))
	}
}

func (c *Chunk) Reset() {
	if len(c.Data) == 0 {
		return
	}
	for _, vec := range c.Data {
		vec.Reset()
	}
	c._Cap = util.DefaultVectorSize
	c.Count = 0
}

func (c *Chunk) Cap() int {
	return c._Cap
}

func (c *Chunk) SetCap(cap int) {
	c._Cap = cap
}

func (c *Chunk) SetCard(count int) {
	util.AssertFunc(c.Count <= c._Cap)
	c.Count = count
}

func (c *Chunk) Card() int {
	return c.Count
}

func (c *Chunk) ColumnCount() int {
	if c == nil {
		return 0
	}
	return len(c.Data)
}

func (c *Chunk) ReferenceIndice(other *Chunk, indice []int) {
	//assertFunc(other.columnCount() <= c.columnCount())
	c.SetCard(other.Card())
	for i, idx := range indice {
		c.Data[i].Reference(other.Data[idx])
	}
}

func (c *Chunk) Reference(other *Chunk) {
	util.AssertFunc(other.ColumnCount() <= c.ColumnCount())
	c.SetCap(other.Cap())
	c.SetCard(other.Card())
	for i := 0; i < other.ColumnCount(); i++ {
		c.Data[i].Reference(other.Data[i])
	}
}

func (c *Chunk) SliceIndice(other *Chunk, sel *SelectVector, count int, colOffset int, indice []int) {
	//assertFunc(other.columnCount() <= colOffset+c.columnCount())
	c.SetCard(count)
	for i, idx := range indice {
		if other.Data[i].PhyFormat().IsDict() {
			c.Data[i+colOffset].Reference(other.Data[idx])
			c.Data[i+colOffset].Slice2(sel, count)
		} else {
			c.Data[i+colOffset].Slice(other.Data[idx], sel, count)
		}
	}
}

func (c *Chunk) Slice(other *Chunk, sel *SelectVector, count int, colOffset int) {
	util.AssertFunc(other.ColumnCount() <= colOffset+c.ColumnCount())
	c.SetCard(count)
	for i := 0; i < other.ColumnCount(); i++ {
		if other.Data[i].PhyFormat().IsDict() {
			c.Data[i+colOffset].Reference(other.Data[i])
			c.Data[i+colOffset].Slice2(sel, count)
		} else {
			c.Data[i+colOffset].Slice(other.Data[i], sel, count)
		}
	}
}

func (c *Chunk) ToUnifiedFormat() []*UnifiedFormat {
	ret := make([]*UnifiedFormat, c.ColumnCount())
	for i := 0; i < c.ColumnCount(); i++ {
		ret[i] = &UnifiedFormat{}
		c.Data[i].ToUnifiedFormat(c.Card(), ret[i])
	}
	return ret
}

func (c *Chunk) Print() {
	for i := 0; i < c.Card(); i++ {
		for j := 0; j < c.ColumnCount(); j++ {
			val := c.Data[j].GetValue(i)
			valStr := val.String()
			//if len(valStr) > 50 {
			//	valStr = valStr[:50] + "..."
			//}
			fmt.Print(valStr)
			fmt.Print("\t")
		}
		fmt.Println()
	}
	//if c.card() > 0 {
	//	fmt.Println()
	//}
}

func (c *Chunk) Print2(rwoPrefix string) {
	for i := 0; i < c.Card(); i++ {
		fields := make([]zap.Field, 0)
		for j := 0; j < c.ColumnCount(); j++ {
			val := c.Data[j].GetValue(i)
			valStr := val.String()
			//if len(valStr) > 50 {
			//	valStr = valStr[:50] + "..."
			//}
			fields = append(fields, zap.String("", valStr))
		}
		util.Info(rwoPrefix, fields...)
	}
	//if c.card() > 0 {
	//	fmt.Println()
	//}
}

func (c *Chunk) SliceItself(sel *SelectVector, cnt int) {
	c.Count = cnt
	for i := 0; i < c.ColumnCount(); i++ {
		c.Data[i].SliceOnSelf(sel, cnt)
	}
}

func (c *Chunk) Hash(result *Vector) {
	util.AssertFunc(result.Typ().Id == common.HashType().Id)
	HashTypeSwitch(c.Data[0], result, nil, c.Card(), false)
	for i := 1; i < c.ColumnCount(); i++ {
		CombineHashTypeSwitch(result, c.Data[i], nil, c.Card(), false)
	}
}

func (c *Chunk) Serialize(serial util.Serialize) error {
	//save row count
	err := util.Write[uint32](uint32(c.Card()), serial)
	if err != nil {
		return err
	}
	//save column count
	err = util.Write[uint32](uint32(c.ColumnCount()), serial)
	if err != nil {
		return err
	}
	//save column types
	for i := 0; i < c.ColumnCount(); i++ {
		err = c.Data[i].Typ().Serialize(serial)
		if err != nil {
			return err
		}
	}
	//save column data
	for i := 0; i < c.ColumnCount(); i++ {
		err = c.Data[i].Serialize(c.Card(), serial)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Chunk) SaveToFile(resFile *os.File) (err error) {
	rowCnt := c.Card()
	colCnt := c.ColumnCount()
	for i := 0; i < rowCnt; i++ {
		for j := 0; j < colCnt; j++ {
			val := c.Data[j].GetValue(i)
			_, err = resFile.WriteString(val.String())
			if err != nil {
				return err
			}
			if j == colCnt-1 {
				continue
			}
			_, err = resFile.WriteString("\t")
			if err != nil {
				return err
			}
		}
		_, err = resFile.WriteString("\n")
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Chunk) Deserialize(deserial util.Deserialize) error {
	//read row count
	rowCnt := uint32(0)
	err := util.Read[uint32](&rowCnt, deserial)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	//read column count
	colCnt := uint32(0)
	err = util.Read[uint32](&colCnt, deserial)
	if err != nil {
		return err
	}
	//read column types
	typs := make([]common.LType, colCnt)
	for i := uint32(0); i < colCnt; i++ {
		typs[i], err = common.DeserializeLType(deserial)
		if err != nil {
			return err
		}
	}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(int(rowCnt))
	//read column data
	for i := uint32(0); i < colCnt; i++ {
		err = c.Data[i].Deserialize(int(rowCnt), deserial)
		if err != nil {
			return err
		}
	}
	return err
}

func (c *Chunk) SaveToWriter(writer wire.DataWriter) (err error) {
	rowCnt := c.Card()
	colCnt := c.ColumnCount()
	row := make([]any, colCnt)
	for i := 0; i < rowCnt; i++ {
		for j := 0; j < colCnt; j++ {
			val := c.Data[j].GetValue(i)
			row[j] = val.String()
		}
		err = writer.Row(row)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Chunk) Flatten() {
	for i := 0; i < c.ColumnCount(); i++ {
		c.Data[i].Flatten(c.Card())
	}
}
