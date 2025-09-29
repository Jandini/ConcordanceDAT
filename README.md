# Concordance.DAT

[![Build](https://github.com/Jandini/Concordance.Dat/actions/workflows/build.yml/badge.svg)](https://github.com/Jandini/Concordance.Dat/actions/workflows/build.yml)
[![NuGet](https://github.com/Jandini/Concordance.Dat/actions/workflows/nuget.yml/badge.svg)](https://github.com/Jandini/Concordance.Dat/actions/workflows/nuget.yml)

**Concordance.DAT** is a high-performance, asynchronous streaming reader for Concordance DAT files, designed for .NET 8. It efficiently parses large legal discovery exports, supports robust encoding detection, and provides flexible handling of empty fields and multiline data. Built for reliability and speed in e-discovery and document management workflows.

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
* **Empty field handling:** configurable via `EmptyFieldMode` in `DatFileOptions`:
  * `Null` (default): empty fields are included as `null` values
  * `Keep`: empty fields are included as empty strings
  * `Omit`: empty fields are omitted from the output dictionary

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

`DatFileOptions` centralizes buffer sizes, file-open behavior, and empty field handling. Defaults are tuned for good throughput; override as needed.

```csharp
using Concordance.Dat;

var options = DatFileOptions.Default with
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
    },
    EmptyFieldMode = EmptyField.Omit // Omit empty fields from output dictionary
};

var cancellationToken = CancellationToken.None;

await foreach (var row in DatFile.ReadAsync("c:\\data\\export.dat", options, cancellationToken))
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

var options = DatFileOptions.Default with { EmptyFieldMode = EmptyField.Keep };

await foreach (var row in DatFile.ReadAsync(fs, options))
{
    // ...
}
```

---

## Reading only the header

To read just the header (field names) from a Concordance DAT file without streaming all records:

```csharp
using Concordance.Dat;

// From a file path:
var header = await DatFile.GetHeaderAsync("c:\\data\\export.dat");

// From a stream:
await using var fs = File.OpenRead("c:\\data\\export.dat");
var header = await DatFile.GetHeaderAsync(fs);
```

Returns a read-only list of field names in file order. Throws `FormatException` if the file is empty or invalid.

---

## API surface

```csharp
public enum EmptyField
{
    Null, // empty fields as null (default)
    Keep, // empty fields as empty string
    Omit  // empty fields omitted from dictionary
}

public static class DatFile
{
    // Path-based
    public static IAsyncEnumerable<Dictionary<string, string>> ReadAsync(
        string path,
        DatFileOptions options = null,
        CancellationToken cancellationToken = default);

    // Stream-based
    public static IAsyncEnumerable<Dictionary<string, string>> ReadAsync(
        Stream stream,
        DatFileOptions options = null,
        CancellationToken cancellationToken = default);

    // Header-only (path)
    public static Task<IReadOnlyList<string>> GetHeaderAsync(
        string path,
        CancellationToken cancellationToken = default);

    // Header-only (stream)
    public static Task<IReadOnlyList<string>> GetHeaderAsync(
        Stream stream,
        CancellationToken cancellationToken = default);
}

public sealed record DatFileOptions
{
    public int ReaderBufferChars { get; init; } = 128 * 1024;
    public int ParseChunkChars   { get; init; } = 128 * 1024;
    public FileStreamOptions File { get; init; } = /* defaults to async + sequential scan, 1 MiB */;
    public EmptyField EmptyFieldMode { get; init; } = EmptyField.Null;
    public static DatFileOptions Default { get; }
}
```

---

## Error handling

* Throws `FormatException` if a record's field count does not match the header.
* Throws `FormatException` if the file does not begin (after optional BOM) with the required `U+00FE` quote character.
* Honors `CancellationToken` during async enumeration and header reading.

---
Created from [JandaBox](https://github.com/Jandini/JandaBox) | Icon created by [Freepik - Flaticon](https://www.flaticon.com/free-icons/box)
