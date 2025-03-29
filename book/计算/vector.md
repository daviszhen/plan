# 简介
介绍向量执行器需要的列式内存结构：vector,chunk和unified format。

# vector
vector逻辑上表示元素数组，固定个数。
vector数据结构关键组成：
- 元素的数据类型。int,float,string等
- 物理表示方案：数据在内存中的组织方式。
	- 不同场景需要不同方案。
	- 节省内存
	- 逻辑上和物理上可能不一致。
- 内存空间。

物理表示方案：
- FLAT。连续数组。与逻辑表示相同。也是默认的表示方案。
- CONSTANT。所有元素相同。物理上只存一份。
- DICTIONARY。字典形式。元素实质是执行字典值的索引。类似指针。组成方式：select vector和child vector（字典）。取元素的方式：用select vector[i]的值在child vector中取最终值。
- SEQUENCE。序列值。组成方式：base（基础值）和increment（递增值）。取元素的方式：base + increment * i。
- unified format。通用格式。 连续数据。与flat相比，多了select vector作为指针。用于开发基于vector的通用算法，避免为具体的物理表示写特殊代码。上面的物理类型都可转成unified format。

```go

type Vector struct {  
    _PhyFormat PhyFormat  //物理表示
    _Typ       common.LType  //数据类型
    Mask       *util.Bitmap  //NULL值
    //内存
    Data       []byte  
    Buf        *VecBuffer  
    Aux        *VecBuffer  
}

```

使用vector方式，有对应的接口：
- 基础接口：
	- Init。为某种数据类型分配数组内存。
	- Reset。
- 物理表示转换接口：
	- Flatten
	- ToUnifiedFormat
- 元素引用接口：
	- slice
	- SetValute/GetValue
	- GetSliceInPhyFormatXXX
	- GetMaskInPhyFormatXXX
- 数据移动接口：
	- copy。

讲一些复杂的接口实现。

## flatten
物理表示转成flat。可以理解为拍平。通常用在执行器算法的某一步。转成flat后，方便后续处理。

目前支持的源物理表示：
- flat。不需要转换。
- constant。将单个常数展开为元素数组。

constant转flat的实现方案：
- 按flat要求分配内存
- 数据复制。单个元素复制到flat数组的每个元素。
```go

func (vec *Vector) Flatten(cnt int) {
	//constant => falt
	按flat要求分配内存;
	如果值是NULL,设置mask；
	constant值 复制 到flat数组的全部元素。

}

```

##  ToUnifiedFormat
物理表示转成unified format.

unified format格式的关键成分：
- select vector。select vector[i]对于的是实际位置下标。
- data。内存。
- mask。null位图。
- 与flat相比，多了select vector作为指针。
```go
type UnifiedFormat struct {  
    Sel      *SelectVector  
    Data     []byte  
    Mask     *util.Bitmap  
    InterSel SelectVector  
    PTypSize int  
}

```

转成unified format具体过程，实质上是确定其关键成分。简单说，是把dict这层vector给去掉，直接引用child vector.
dict转unified format稍微麻烦些。
具体方案：
```go
func (vec *Vector) ToUnifiedFormat(count int, output *UnifiedFormat) {
	switch vec的物理表示{
	case dict:
		select vector 为vec.sel；
		if child vector是flat {
			直接引用child vector的select vector, 内存和mask；
		} else {
			child vector先flatten；
			再引用child vector的select vector, 内存和mask；
		}
	case constant:
		select vector的值都为0。指向第一个元素；
		直接引用内存和mask；
	case flat:
		select vector的值都为0～N-1。
		直接引用内存和mask；
	}

}

```

## slice
slice对vector的切片操作。有多种具体实现。

- SliceOnSelf
基于select vector对vector本身进行slice。如果vector的物理表示是dict和flat，结果vector的物理表示是dict. 如果是constant,结果不变。

```go

func (vec *Vector) SliceOnSelf(sel *SelectVector, count int) {
	
	if 物理表示是constant {
		什么都不做；
	}else if dict{
		//dict
		用输入的select vector对dict的select vector进行slice，生成新的select vector；（新的select vector[i] = dict.select vector[sel[i]]。简单理解成二级指针）
			
	}else {
		//flat
		物理表示转为dict；
		dict的select vector为输入的select vector；
		dict的child vector指向vec本身；
	}

}

```

- Slice, Slice2, Slice3

Slice是引用另一个vector，并用select vector进行slice。
Slice2是对当前vector，用select vector进行slice。
Slice3也是引用另一个vector，并用下标区间[offset,end]进行slice。与Slice相比，接口不同，实现有些区别。意义确实一样的。

实现上，Slice和Slice2，最终使用SliceOnSelf。Slice3却是直接对内存进行切片。具体实现都不复杂，不再细说。

```go

func (vec *Vector) Slice(other *Vector, sel *SelectVector, count int){
...
}

func (vec *Vector) Slice2(sel *SelectVector, count int) {
...
}

func (vec *Vector) Slice3(other *Vector, offset uint64, end uint64) {
    ...
    }

```

## SetValute/GetValue
SetValue(idx,value)给vector指定位置设置新值。
GetValue(idx)从vector指定位置取值。

 - GetValue(idx)
```go
func (vec *Vector) GetValue(idx int) *Value {
	switch{
	case constant:
		//constant vector只有一个元素
		idx = 0；
	case flat:
	case dict:
		从child vector的位置select vector[idx]处取值；
	case sequence:
		返回值为start + idx * increment
	}
	如果idx是努力了，返回；
	返回 内存idx处的元素；
}

```

- SetValue（idx,value)
镜像操作，实现不复杂。
## GetSliceInPhyFormatXXX
取出vector的底层元素数组。XXX包括flat,constant,sequence，unified format。不包含dict.
实现上是简单的范型转换。

## GetMaskInPhyFormatXXX
取出vector的mask（NULL位图）。实现简单。

## Copy
从源vector复制数据到目的vector.  要求目的vector是flat.

copy接口：
```go

func Copy(  
    srcP *Vector,  //源vector
    dstP *Vector,  //目的vector
    selP *SelectVector,  //源select vector
    srcCount int,  //要复制的源元素个数
    srcOffset int,  //要复制的源头开始位置
    dstOffset int,  //目的开始位置
){
	//step 1：确定实际的源vector和select vector
	dict类型源vector：
		源vector是child vector；
		源select vector是输入selP对dict select vector切片后的结果。
	constant类型源vector：
		源select vector每个值为0.

	//step 2：复制Mask
	//step 3: 复制实际数组元素
	dst vector[dstOffset + i] = src vector[selP[srcOffset + i]]
	
}

```

# chunk
一组元素个数相同的vector组成chunk. 执行器的输入，输出以chunk为单位。
chunk上的操作，最终都转为对每个vector的操作。因此没有特别要细说的。

# 小结
介绍vector的特点和主要接口的实现方式。