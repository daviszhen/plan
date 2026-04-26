# Proto definitions (from Lance)

Proto files are copied from the [Lance](https://github.com/lancedb/lance) repository for metadata compatibility (Manifest, Fragment, DataFile, Transaction).

## Files

- `file.proto` – Field, Schema, FileDescriptor (lance.file)
- `table.proto` – Manifest, DataFragment, DataFile, DeletionFile (lance.table)
- `transaction.proto` – Transaction and operations (lance.table)

## Regenerating Go code

Requires `protoc` and `protoc-gen-go`:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
make
```

If `google/protobuf/*.proto` are not under `/usr/include`, set `PROTOC_INCLUDE`:

```bash
make PROTOC_INCLUDE=/path/to/protobuf/include
```
