# Proto 文件清单（来自 Lance）

| 文件 | 来源路径 | 说明 |
|------|----------|------|
| file.proto | lance/protos/file.proto | lance.file：Field, Schema, FileDescriptor, Metadata, Encoding |
| table.proto | lance/protos/table.proto | lance.table：Manifest, DataFragment, DataFile, DeletionFile, BasePath, IndexMetadata 等 |
| transaction.proto | lance/protos/transaction.proto | lance.table：Transaction 及各 Operation（Append, Delete, Overwrite, CreateIndex, Rewrite, Merge, Project 等） |

## 依赖关系

- table.proto 依赖 file.proto、google/protobuf/any.proto、google/protobuf/timestamp.proto
- transaction.proto 依赖 file.proto、table.proto、google/protobuf/any.proto

## 生成 Go 代码

见 README.md，执行 `make` 生成 file.pb.go、table.pb.go、transaction.pb.go。
