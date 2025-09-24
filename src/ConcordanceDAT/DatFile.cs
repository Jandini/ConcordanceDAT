using System.Buffers;
using System.Text;

namespace Concordance.Dat;

/// <summary>
/// Configuration for the Concordance DAT reader. Provides tuning knobs for decoding and parsing,
/// plus default file-open options used by the path-based overload.
/// </summary>
public sealed record DatFileOptions
{
    /// <summary>
    /// Size in characters of the internal StreamReader buffer. Clamped between 4 KB and 1 MB. Default is 128 KB.
    /// </summary>
    public int ReaderBufferChars { get; init; } = 128 * 1024;

    /// <summary>
    /// Size in characters of the pooled parsing buffer used per read cycle. Clamped between 4 KB and 1 MB. Default is 128 KB.
    /// </summary>
    public int ParseChunkChars { get; init; } = 128 * 1024;

    /// <summary>
    /// File stream options applied by the ReadAsync(string path, ...) overload. If not provided,
    /// defaults to asynchronous, sequential read with a 1 MiB OS buffer.
    /// </summary>
    public FileStreamOptions File { get; init; } = new FileStreamOptions
    {
        Mode = FileMode.Open,
        Access = FileAccess.Read,
        Share = FileShare.Read,
        Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
        BufferSize = 1 << 20 // 1 MiB
    };

    /// <summary>
    /// A reusable default instance with conservative, high-throughput settings.
    /// </summary>
    public static DatFileOptions Default { get; } = new DatFileOptions();

    internal DatFileOptions Clamp()
        => this with
        {
            ReaderBufferChars = Math.Clamp(ReaderBufferChars, 4 * 1024, 1 * 1024 * 1024),
            ParseChunkChars = Math.Clamp(ParseChunkChars, 4 * 1024, 1 * 1024 * 1024),
        };
}

/// <summary>
/// Asynchronous, streaming reader for Concordance DAT files.
/// This reader is designed for very large files and minimizes allocations by reusing buffers.
/// It supports UTF-8 and UTF-16 encodings, honors the Concordance format rules, and yields
/// one dictionary per data record where keys come from the header row.
/// </summary>
public static class DatFile
{


    /// <summary>
    /// Convenience overload that opens and streams a Concordance DAT file using
    /// <see cref="DatFileOptions.Default"/>. This returns the underlying async iterator
    /// without creating an extra state machine (the method itself is not marked <c>async</c>).
    /// </summary>
    /// <param name="path">
    /// Full filesystem path to the Concordance DAT file. The file is opened for asynchronous,
    /// sequential read with defaults defined in <see cref="DatFileOptions.Default.File"/>.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancellation token to cooperatively cancel the asynchronous enumeration.
    /// </param>
    /// <returns>
    /// An <see cref="IAsyncEnumerable{T}"/> of rows, where each row is a
    /// <see cref="Dictionary{TKey, TValue}"/> keyed by header column names.
    /// The header record itself is not yielded.
    /// </returns>
    public static IAsyncEnumerable<Dictionary<string, string>> ReadAsync(
        string path,
        CancellationToken cancellationToken = default)
        => ReadAsync(path, DatFileOptions.Default, cancellationToken);

    /// <summary>
    /// Opens a file path and streams it as a Concordance DAT reader, returning an async sequence of rows.
    /// The first record in the file is treated as the header and is not yielded to the caller.
    /// After the header is parsed, each subsequent record is returned as a dictionary using the header names as keys.
    /// </summary>
    /// <param name="path">File system path to a Concordance DAT file. Opened for asynchronous, sequential read.</param>
    /// <param name="options">Optional reader options. If null, DatFileOptions.Default is used.</param>
    /// <param name="cancel">Cancellation token to cooperatively cancel the asynchronous enumeration.</param>
    /// <returns>Async sequence of rows as dictionaries keyed by header names. The header row itself is not yielded.</returns>
    public static async IAsyncEnumerable<Dictionary<string, string>> ReadAsync(
        string path,
        DatFileOptions options = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancel = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        var opts = (options ?? DatFileOptions.Default).Clamp();

        await using var fs = File.Open(path, opts.File);

        await foreach (var row in ReadAsync(fs, opts, cancel).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// Opens a text stream as a Concordance DAT reader and returns an async sequence of rows.
    /// The first record in the file is treated as the header and is not yielded to the caller.
    /// After the header is parsed, each subsequent record is returned as a dictionary using
    /// the header names as keys. Records are streamed in file order.
    ///
    /// The method detects the encoding using DetectEncoding. The stream position is adjusted
    /// so that if a BOM exists it will be set to the first character after the BOM. If no BOM
    /// exists it remains at the original position.
    /// </summary>
    /// <param name="stream">Readable, seekable stream positioned at the beginning of a Concordance DAT file.</param>
    /// <param name="options">Optional reader options. If null, DatFileOptions.Default is used.</param>
    /// <param name="cancel">Cancellation token to cooperatively cancel the asynchronous enumeration.</param>
    /// <returns>Async sequence of rows as dictionaries keyed by header names. The header row itself is not yielded.</returns>
    public static async IAsyncEnumerable<Dictionary<string, string>> ReadAsync(
        Stream stream,
        DatFileOptions options = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanRead) throw new ArgumentException("Stream must be readable.", nameof(stream));

        var opts = (options ?? DatFileOptions.Default).Clamp();

        // Detect encoding and position the stream correctly with respect to a possible BOM.
        var encoding = DetectEncoding(stream);

        // Create a StreamReader only for decoding. Disable BOM auto detection since DetectEncoding already handled it.
        using var reader = new StreamReader(stream, encoding, detectEncodingFromByteOrderMarks: false, bufferSize: opts.ReaderBufferChars, leaveOpen: false);

        // Delegate parsing to the inner async iterator that yields rows.
        await foreach (var row in GetRowsAsync(reader, opts.ParseChunkChars, cancellationToken).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// Parses the text provided by the StreamReader and yields records as dictionaries.
    /// The first record parsed is treated as the header and is not yielded.
    ///
    /// Parsing rules based on Concordance DAT format:
    /// Field separator is 0x14.
    /// Field quote character is 0xFE.
    /// All fields are quoted. A literal quote inside a field is written as 0xFE 0xFE.
    /// Newlines inside quoted fields are part of the value and do not end the record.
    /// A record ends on LF or CRLF when not inside quotes. A final CR at end of file is also accepted.
    ///
    /// This method uses a pooled char buffer and reuses StringBuilder and List instances across records
    /// to reduce allocations. It also supports cancellation checks on each chunk.
    /// </summary>
    private static async IAsyncEnumerable<Dictionary<string, string>> GetRowsAsync(
        StreamReader reader,
        int parseChunkChars,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancel)
    {
        var pool = ArrayPool<char>.Shared;
        var buffer = pool.Rent(parseChunkChars);
        try
        {
            var sep = (char)0x14;   // Column separator
            var quote = (char)0xFE; // Field quote

            var inQuotes = false;   // True when inside a quoted field
            var lastWasCR = false;  // True if last char was CR and we need to check for CRLF across chunks

            var field = new StringBuilder(8192);  // Accumulates the current field text
            var fields = new List<string>(128);   // Collects fields for the current record
            List<string> headers = null;          // Captured from the first record

            void EndField()
            {
                fields.Add(field.ToString());
                field.Clear();
            }

            Dictionary<string, string> EndRecord()
            {
                if (headers is null)
                {
                    headers = [.. fields];
                    fields.Clear();
                    return []; // Do not yield header
                }

                if (fields.Count != headers.Count)
                    throw new FormatException($"Invalid field count: got {fields.Count}, expected {headers.Count}. Each record must match the header column count and end with a line break.");

                var dict = new Dictionary<string, string>(headers.Count, StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < headers.Count; i++)
                    dict[headers[i]] = fields[i];

                fields.Clear();
                return dict;
            }

            int read;
            while ((read = await reader.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false)) > 0)
            {
                for (int i = 0; i < read; i++)
                {
                    cancel.ThrowIfCancellationRequested();

                    char ch = buffer[i];

                    if (lastWasCR)
                    {
                        lastWasCR = false;
                        if (ch == '\n' && !inQuotes)
                        {
                            EndField();
                            var row = EndRecord();
                            if (row.Count > 0) yield return row;
                            continue; // consume LF of CRLF
                        }
                        else
                        {
                            field.Append('\r'); // CR was data
                        }
                    }

                    if (ch == quote)
                    {
                        if (inQuotes && i + 1 < read && buffer[i + 1] == quote)
                        {
                            field.Append(quote); // escaped quote
                            i++;
                        }
                        else
                        {
                            inQuotes = !inQuotes; // toggle quote state
                        }
                    }
                    else if (!inQuotes && ch == sep)
                    {
                        EndField();
                    }
                    else if (!inQuotes && ch == '\n')
                    {
                        EndField();
                        var row = EndRecord();
                        if (row.Count > 0) yield return row;
                    }
                    else if (ch == '\r')
                    {
                        if (!inQuotes)
                            lastWasCR = true; // might be CRLF
                        else
                            field.Append('\r'); // literal CR inside quotes
                    }
                    else
                    {
                        field.Append(ch);
                    }
                }
            }

            // Handle trailing CR as a record terminator.
            if (lastWasCR && !inQuotes)
            {
                EndField();
                var rowCR = EndRecord();
                if (rowCR.Count > 0) yield return rowCR;
            }

            // Flush any remaining data as the final record (handles missing trailing newline).
            if (field.Length > 0 || fields.Count > 0)
            {
                EndField();
                var last = EndRecord();
                if (last.Count > 0) yield return last;
            }
        }
        finally
        {
            pool.Return(buffer);
        }
    }

    /// <summary>
    /// Detects text encoding for a Concordance DAT stream and positions the stream correctly relative to any BOM.
    ///
    /// If a BOM is present, recognizes UTF-16 LE, UTF-16 BE, or UTF-8 BOM and validates that the first character
    /// after the BOM is the Concordance quote U+00FE. If valid, returns the corresponding encoding and sets the
    /// stream position to the first character after the BOM.
    ///
    /// If no BOM is present, validates the first two bytes as one of:
    /// UTF-16 LE without BOM where the first character is FE 00,
    /// UTF-16 BE without BOM where the first character is 00 FE,
    /// UTF-8 without BOM where the first character is C3 BE.
    /// In these BOM-less cases, returns the corresponding encoding and leaves the stream at the original position.
    ///
    /// If none of the patterns match, throws a FormatException because a valid Concordance DAT must begin
    /// with the quote character after the optional BOM.
    /// </summary>
    /// <param name="stream">Seekable stream positioned at the start of a Concordance DAT file.</param>
    /// <returns>The detected text encoding with strict decoding settings.</returns>
    private static Encoding DetectEncoding(Stream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanSeek) throw new InvalidOperationException("Stream must be seekable for encoding detection.");

        long start = stream.Position;

        // Read enough bytes to identify a BOM and confirm the first character (þ).
        byte[] buf = new byte[6];
        int bytes = stream.Read(buf, 0, buf.Length);

        static Encoding Utf8Strict() => new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
        static Encoding Utf16LE() => new UnicodeEncoding(bigEndian: false, byteOrderMark: false, throwOnInvalidBytes: true);
        static Encoding Utf16BE() => new UnicodeEncoding(bigEndian: true, byteOrderMark: false, throwOnInvalidBytes: true);

        // UTF-16 LE with BOM: FF FE, expect FE 00 as first char (U+00FE)
        if (bytes >= 2 && buf[0] == 0xFF && buf[1] == 0xFE)
        {
            if (bytes >= 4 && !(buf[2] == 0xFE && buf[3] == 0x00))
                throw new FormatException("Invalid Concordance DAT: expected U+00FE after UTF-16 LE BOM.");

            stream.Position = start + 2; // after BOM
            return Utf16LE();
        }

        // UTF-16 BE with BOM: FE FF, expect 00 FE as first char (U+00FE)
        if (bytes >= 2 && buf[0] == 0xFE && buf[1] == 0xFF)
        {
            if (bytes >= 4 && !(buf[2] == 0x00 && buf[3] == 0xFE))
                throw new FormatException("Invalid Concordance DAT: expected U+00FE after UTF-16 BE BOM.");

            stream.Position = start + 2; // after BOM
            return Utf16BE();
        }

        // UTF-8 with BOM: EF BB BF, expect C3 BE as first char (U+00FE)
        if (bytes >= 3 && buf[0] == 0xEF && buf[1] == 0xBB && buf[2] == 0xBF)
        {
            if (bytes >= 5 && !(buf[3] == 0xC3 && buf[4] == 0xBE))
                throw new FormatException("Invalid Concordance DAT: expected U+00FE after UTF-8 BOM.");

            stream.Position = start + 3; // after BOM
            return Utf8Strict();
        }

        // No BOM: the first character must be U+00FE in one of the supported encodings.
        stream.Position = start;

        // UTF-16 LE without BOM: FE 00
        if (bytes >= 2 && buf[0] == 0xFE && buf[1] == 0x00)
            return Utf16LE();

        // UTF-16 BE without BOM: 00 FE
        if (bytes >= 2 && buf[0] == 0x00 && buf[1] == 0xFE)
            return Utf16BE();

        // UTF-8 without BOM: C3 BE
        if (bytes >= 2 && buf[0] == 0xC3 && buf[1] == 0xBE)
            return Utf8Strict();

        throw new FormatException("Invalid Concordance DAT: after an optional BOM, the file must begin with the quote character U+00FE (þ). " 
            + "The detected byte pattern does not match UTF-8 or UTF-16 (LE/BE) with U+00FE at the start. ");
    }
}
