# Concordance.DAT

[![Build](https://github.com/Jandini/Concordance.Dat/actions/workflows/build.yml/badge.svg)](https://github.com/Jandini/Concordance.Dat/actions/workflows/build.yml)
[![NuGet](https://github.com/Jandini/Concordance.Dat/actions/workflows/nuget.yml/badge.svg)](https://github.com/Jandini/Concordance.Dat/actions/workflows/nuget.yml)

Asynchronous, streaming reader for **Concordance DAT** files targeting **.NET 8**.
This library centralizes configuration via `DatFileOptions` (with sensible defaults) and follows .NET naming conventions: **namespace** `Concordance.Dat`, **type** `DatFile`.

**Author:** GPT5

---

## Key behaviors

* **Separator:** `0x14` (DC4) between fields
* **Quote / Qualifier:** `0xFE` around every field
* **Escaped quote:** doubled `0xFE` within a quoted field
* **Multiline values:** `CR`, `LF`, or `CRLF` inside quotes are treated as data
* **Record terminators:** `LF` or `CRLF` when not in quotes; a trailing `CR` at EOF is accepted
* **Header handling:** the first record is the header and is **not** yielded; subsequent rows are dictionaries keyed by header names
* **Validation:** each data record must have the same number of fields as the header; otherwise a `FormatException` is thrown
* **Encoding:** BOM-aware detection for **UTF-8**, **UTF-16 LE**, **UTF-16 BE**; BOM-less files must begin with `U+00FE`
* **Stream positioning:** after detection, if a BOM exists, the stream is positioned to the first character after the BOM

---

## Performance notes

* Uses `ArrayPool<char>` for chunked decoding and reuses `StringBuilder`/`List` instances across records to minimize allocations.
* Prefer opening files with `FileOptions.Asynchronous | FileOptions.SequentialScan` for best throughput.
* Buffer sizes are specified in **characters**, not bytes (UTF-16 typically uses 2 bytes per char).

---

## Quick start

Install your project reference (source or package), then:

```csharp
using Concordance.Dat;

await foreach (var row in DatFile.ReadAsync("c:\\data\\export.dat"))
{
    var docId = row["DOCID"];
    // process...
}
```

---

## Configuration with `DatFileOptions`

`DatFileOptions` centralizes buffer sizes and file-open behavior. Defaults are tuned for good throughput; override as needed.

```csharp
using Concordance.Dat;

var opts = DatFileOptions.Default with
{
    ReaderBufferChars = 256 * 1024,   // StreamReader decode buffer (chars)
    ParseChunkChars   = 128 * 1024,   // Parser working buffer (chars)
    File = new FileStreamOptions
    {
        Mode = FileMode.Open,
        Access = FileAccess.Read,
        Share = FileShare.Read,
        Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
        BufferSize = 1 << 20          // 1 MiB
    }
};

var cancel = CancellationToken.None;

await foreach (var row in DatFile.ReadAsync("c:\\data\\export.dat", opts, cancel))
{
    // ...
}
```

You can also supply a `Stream` directly:

```csharp
await using var fs = File.Open("c:\\data\\export.dat", new FileStreamOptions
{
    Mode = FileMode.Open,
    Access = FileAccess.Read,
    Share = FileShare.Read,
    Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
    BufferSize = 1 << 20
});

await foreach (var row in DatFile.ReadAsync(fs, DatFileOptions.Default))
{
    // ...
}
```

---

## API surface

```csharp
public static class DatFile
{
    // Path-based
    public static IAsyncEnumerable<Dictionary<string, string>> ReadAsync(
        string path,
        DatFileOptions? options = null,
        CancellationToken cancel = default);

    // Stream-based
    public static IAsyncEnumerable<Dictionary<string, string>> ReadAsync(
        Stream stream,
        DatFileOptions? options = null,
        CancellationToken cancel = default);
}

public sealed record DatFileOptions
{
    public int ReaderBufferChars { get; init; } = 128 * 1024;
    public int ParseChunkChars   { get; init; } = 128 * 1024;
    public FileStreamOptions File { get; init; } = /* defaults to async + sequential scan, 1 MiB */;
    public static DatFileOptions Default { get; }
}
```

---

## Error handling

* Throws `FormatException` if a record’s field count does not match the header.
* Throws `FormatException` if the file does not begin (after optional BOM) with the required `U+00FE` quote character.
* Honors `CancellationToken` during async enumeration.

---
Created from [JandaBox](https://github.com/Jandini/JandaBox)
Box icon created by [Freepik - Flaticon](https://www.flaticon.com/free-icons/box)
