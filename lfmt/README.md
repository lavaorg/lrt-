lfmt
=======

Human-readable byte formatter.

Example:

```go
lfmt.ByteSize(100.5*bytefmt.MEGABYTE) // returns "100.5M"
lfmt.ByteSize(uint64(1024)) // returns "1K"
```

