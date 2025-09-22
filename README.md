# ConcordanceDAT

A tiny, async, streaming reader for **Concordance DAT** files written for .NET 8.
It’s designed to be simple, memory-efficient, and robust with very large, multi-line fields.

---

## Features

* **Async streaming**: `IAsyncEnumerable<Dictionary<string,string>>` — process rows as they’re decoded.
* **Encoding detection**: UTF-8 (with/without BOM), UTF-16 LE/BE via BOM; falls back to UTF-8 (no BOM).
* **Format support**:

  * Field **separator**: `0x14`
  * Field **quote / qualifier**: `0xFE`
  * **Escaped qualifier**: inside a field, literal `0xFE` is written as doubled `0xFE 0xFE`
  * **Multi-line fields**: CR/LF/CRLF inside quotes are treated as data (not record terminators)
* **Header handling**: the **first record is the header** and is **not yielded**; it defines dictionary keys.
* **Validation**: each data row must have the **same number of fields** as the header (throws `FormatException` if not).
* **Final record**: if the file ends **without a trailing newline**, the final (unterminated) record is still accepted and yielded.
* **Memory efficiency**: uses `ArrayPool<char>` for decode buffers and reuses builders/collections.
* **Order guaranteed**: rows are yielded in file order; no parallelism in the iterator.
* **Cancellation**: honors `CancellationToken` during read/parse.

---

## Requirements

* **.NET 8** (C# 12/13)
* Any readable `Stream` (`FileStream`, network streams, etc.)

---

## Quick Start

```csharp
using ConcordanceDAT;

await using var fs = File.OpenRead("data/export.dat");

await foreach (var row in DATFile.ReadAsync(fs))
{
    // Access by column name from the header
    var docId = row["DOCID"];
    var title = row.TryGetValue("TITLE", out var t) ? t : "";
    Console.WriteLine($"{docId}: {title}");
}
```

### With cancellation and custom buffer sizes

```csharp
var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

await foreach (var row in DATFile.ReadAsync(
                   stream: fs,
                   readerBufferChars: 128 * 1024,  // StreamReader's internal buffer (chars)
                   parseChunkChars:  128 * 1024,  // pooled parse buffer (chars)
                   cancel: cts.Token))
{
    // ...
}
```

---

## API

```csharp
public static class DATFile
{
    public static IAsyncEnumerable<Dictionary<string, string>> ReadAsync(
        Stream stream,
        int readerBufferChars = 128 * 1024,
        int parseChunkChars   = 128 * 1024,
        CancellationToken cancel = default);
}
```

* **stream**: any readable `Stream`.
* **readerBufferChars**: `StreamReader`’s internal buffer size in **chars** (clamped to 4 KB–1 MB).
* **parseChunkChars**: pooled `char[]` chunk used by the parser (clamped to 4 KB–1 MB).
* **cancel**: cooperative cancellation for long reads / very large fields.

---

## Concordance DAT Rules (as implemented)

* **Separator**: `0x14`
* **Qualifier**: `0xFE`
* **Escaping**: inside a quoted field, a literal qualifier is written as doubled (`0xFE 0xFE`)
* **Multiline**: CR (`\r`), LF (`\n`), or CRLF are allowed inside quoted fields
* **Record termination**: LF, CRLF, or a lone CR (when not in quotes)
* **Header**: first record defines column names (unique names recommended)

---

## Error Handling

* **Mismatched column count**
  Throws `FormatException` with details when a data row’s field count differs from the header count.

* **Encoding issues**
  BOM is honored (UTF-8/UTF-16 LE/BE). No BOM -> assume UTF-8.

> Tip: If you ingest third-party DATs with inconsistent structure, catch `FormatException` and log the row index/context.

---

## Performance Tips

* When opening files yourself, use:

  ```csharp
  var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read,
                          bufferSize: 1 << 20,
                          options: FileOptions.Asynchronous | FileOptions.SequentialScan);
  ```
* Tune `readerBufferChars` / `parseChunkChars`:

  * Local disk / LAN SMB: **64–256 KB** often ideal
  * Higher-latency streams: **256 KB–1 MB** may improve throughput
* Downstream processing: keep it streaming; avoid buffering all rows in memory.

---

## FAQ

**Does async preserve row order?**
Yes. The iterator is single-threaded; `await foreach` yields rows in the exact order parsed.

**Can fields contain newlines?**
Yes. Newlines inside quotes are treated as data. Records terminate only when **not** inside quotes.

**What if the final record has no newline?**
It is accepted and yielded (column-count validation still applies).

**What happens with duplicate header names?**
Later duplicates overwrite earlier ones in the resulting `Dictionary`. Prefer unique column names.

---

## Example: Safe access helpers

```csharp
await foreach (var row in DATFile.ReadAsync(fs))
{
    if (row.TryGetValue("DOCID", out var id))
    {
        // ...
    }
}
```

---

Created from [JandaBox](https://github.com/Jandini/JandaBox)
