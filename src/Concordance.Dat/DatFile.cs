using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;

namespace Concordance.Dat;

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
    public static IAsyncEnumerable<Dictionary<string, object>> ReadAsync(
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
    /// <param name="cancellationToken">Cancellation token to cooperatively cancel the asynchronous enumeration.</param>
    /// <returns>Async sequence of rows as dictionaries keyed by header names. The header row itself is not yielded.</returns>
    public static async IAsyncEnumerable<Dictionary<string, object>> ReadAsync(
        string path,
        DatFileOptions options = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        var opts = (options ?? DatFileOptions.Default).Clamp();

        await using var fs = File.Open(path, opts.File);

        await foreach (var row in ReadAsync(fs, opts, cancellationToken).ConfigureAwait(false))
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
    public static async IAsyncEnumerable<Dictionary<string, object>> ReadAsync(
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
        await foreach (var row in GetRowsAsync(reader, opts, cancellationToken).ConfigureAwait(false))
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
    private static async IAsyncEnumerable<Dictionary<string, object>> GetRowsAsync(
        StreamReader reader,
        DatFileOptions options,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var pool = ArrayPool<char>.Shared;
        var buffer = pool.Rent(options.ParseChunkChars);
        try
        {
            var sep = (char)0x14;   // Column separator
            var quote = (char)0xFE; // Field quote
            var empty = options.EmptyFieldMode;

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

            Dictionary<string, object> EndRecord()
            {
                if (headers is null)
                {
                    headers = [.. fields];
                    fields.Clear();
                    return []; // Do not yield header
                }

                if (fields.Count != headers.Count)
                    throw new FormatException($"Invalid field count: got {fields.Count}, expected {headers.Count}. Each record must match the header column count and end with a line break.");

                var count = headers.Count;
                var dict = new Dictionary<string, object>(count, StringComparer.OrdinalIgnoreCase);

                var hSpan = CollectionsMarshal.AsSpan(headers);
                var fSpan = CollectionsMarshal.AsSpan(fields);

                for (int i = 0; i < count; i++)
                {
                    var key = hSpan[i];
                    var val = fSpan[i];

                    if (!string.IsNullOrEmpty(val))
                    {
                        dict[key] = val;
                    }
                    else if (empty != EmptyField.Omit)
                    {
                        dict[key] = (empty == EmptyField.Null) ? null : string.Empty;
                    }
                }

                fields.Clear();
                return dict;
            }

            int read;
            while ((read = await reader.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false)) > 0)
            {
                for (int i = 0; i < read; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

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
    /// Reads only the header from a Concordance DAT file (by path) and returns the list of field names
    /// in file order. Uses a small fixed buffer since only the first record is needed.
    /// </summary>
    /// <param name="path">File system path to a Concordance DAT file.</param>
    /// <param name="cancellationToken">Cancellation token to cooperatively cancel the operation.</param>
    /// <returns>A read-only list of header field names in file order.</returns>
    public static async Task<IReadOnlyList<string>> GetHeaderAsync(
        string path,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        // Open with sensible defaults; header is tiny so defaults are fine here.
        await using var fs = File.Open(path, DatFileOptions.Default.File);
        return await GetHeaderAsync(fs, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads only the header from a Concordance DAT stream and returns the list of field names
    /// in file order. Uses a small fixed buffer since only the first record is needed.
    /// </summary>
    /// <param name="stream">Readable, seekable stream positioned at the start of a Concordance DAT file.</param>
    /// <param name="cancellationToken">Cancellation token to cooperatively cancel the operation.</param>
    /// <returns>A read-only list of header field names in file order.</returns>
    public static async Task<IReadOnlyList<string>> GetHeaderAsync(
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanRead) throw new ArgumentException("Stream must be readable.", nameof(stream));

        // Detect encoding and position stream after BOM if present
        var encoding = DetectEncoding(stream);

        // Small fixed buffers are sufficient for the header
        const int ReaderBufferChars = 8 * 1024;
        const int ParseChunkChars = 8 * 1024;

        using var reader = new StreamReader(stream, encoding, detectEncodingFromByteOrderMarks: false, bufferSize: ReaderBufferChars, leaveOpen: false);

        var pool = ArrayPool<char>.Shared;
        var buffer = pool.Rent(ParseChunkChars);
        try
        {
            var sep = (char)0x14; // field separator
            var quote = (char)0xFE; // field quote

            var inQuotes = false;
            var lastWasCR = false;

            var field = new StringBuilder(1024);
            var fields = new List<string>(64);

            void EndField()
            {
                fields.Add(field.ToString());
                field.Clear();
            }

            // Returns header snapshot and completes
            static IReadOnlyList<string> SnapshotHeader(List<string> f)
                => Array.AsReadOnly(f.ToArray());

            int read;
            while ((read = await reader.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false)) > 0)
            {
                for (int i = 0; i < read; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    char ch = buffer[i];

                    if (lastWasCR)
                    {
                        lastWasCR = false;
                        if (ch == '\n' && !inQuotes)
                        {
                            // header terminated by CRLF
                            EndField();
                            return SnapshotHeader(fields);
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
                            inQuotes = !inQuotes; // toggle
                        }
                    }
                    else if (!inQuotes && ch == sep)
                    {
                        EndField();
                    }
                    else if (!inQuotes && ch == '\n')
                    {
                        // header terminated by LF
                        EndField();
                        return SnapshotHeader(fields);
                    }
                    else if (ch == '\r')
                    {
                        if (!inQuotes) lastWasCR = true; // maybe CRLF
                        else field.Append('\r');
                    }
                    else
                    {
                        field.Append(ch);
                    }
                }
            }

            // EOF handling for header-only read:

            // Lone trailing CR as terminator
            if (lastWasCR && !inQuotes)
            {
                EndField();
                return SnapshotHeader(fields);
            }

            // Header without trailing newline (allowed)
            if (field.Length > 0 || fields.Count > 0)
            {
                EndField();
                return SnapshotHeader(fields);
            }

            throw new FormatException("Empty or invalid Concordance DAT. Header row not found.");
        }
        finally
        {
            pool.Return(buffer);
        }
    }

    /// <summary>
    /// Reads the header and counts data rows in a Concordance DAT file.
    /// </summary>
    /// <param name="path">File system path to a Concordance DAT file.</param>
    /// <param name="progress">
    /// Optional progress callback that receives (header fields, current row count) updates and returns the number of rows to process before next update.
    /// Always called when header is available (row count = 0) and when counting completes.
    /// </param>
    /// <param name="cancellationToken">Cancellation token to cooperatively cancel the operation.</param>
    /// <returns>A tuple containing the header field names and the number of data rows in the file.</returns>
    public static async Task<(IReadOnlyList<string> Header, long RowCount)> GetCountAsync(
        string path,
        Func<IReadOnlyList<string>, long, int> progress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        var opts = DatFileOptions.Default.Clamp();
        await using var fs = File.Open(path, opts.File);
        var encoding = DetectEncoding(fs);
        using var reader = new StreamReader(fs, encoding, detectEncodingFromByteOrderMarks: false, bufferSize: opts.ReaderBufferChars, leaveOpen: false);
        return await GetCountAsync(reader, opts, progress, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads the header and counts data rows from a Concordance DAT stream.
    /// </summary>
    /// <param name="stream">Readable, seekable stream positioned at the start of a Concordance DAT file.</param>
    /// <param name="progress">
    /// Optional progress callback that receives (header fields, current row count) updates and returns the number of rows to process before next update.
    /// Always called when header is available (row count = 0) and when counting completes.
    /// </param>
    /// <param name="cancellationToken">Cancellation token to cooperatively cancel the operation.</param>
    /// <returns>A tuple containing the header field names and the number of data rows in the stream.</returns>
    public static async Task<(IReadOnlyList<string> Header, long RowCount)> GetCountAsync(
        Stream stream,
        Func<IReadOnlyList<string>, long, int> progress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanRead) throw new ArgumentException("Stream must be readable.", nameof(stream));

        var opts = DatFileOptions.Default.Clamp();
        var encoding = DetectEncoding(stream);
        using var reader = new StreamReader(stream, encoding, detectEncodingFromByteOrderMarks: false, bufferSize: opts.ReaderBufferChars, leaveOpen: false);
        return await GetCountAsync(reader, opts, progress, cancellationToken).ConfigureAwait(false);
    }

    // Shared helper: parse header and count rows with optional progress updates
    private static async Task<(IReadOnlyList<string> Header, long RowCount)> GetCountAsync(
        StreamReader reader,
        DatFileOptions options,
        Func<IReadOnlyList<string>, long, int> progress,
        CancellationToken cancellationToken)
    {
        var pool = ArrayPool<char>.Shared;
        var buffer = pool.Rent(options.ParseChunkChars);
        var rowCount = 0L;
        var nextUpdate = 1L; // Always report first progress (header)
        try
        {
            var sep = (char)0x14;   // Column separator
            var quote = (char)0xFE; // Field quote
            var empty = options.EmptyFieldMode;

            var inQuotes = false;// True when inside a quoted field
            var lastWasCR = false;  // True if last char was CR and we need to check for CRLF across chunks

            var field = new StringBuilder(8192);  // Accumulates the current field text
            var fields = new List<string>(128);   // Collects fields for the current record
            List<string> headers = null;    // Captured from the first record

            void EndField()
            {
                // do not store field value after header is obtained
                fields.Add(headers is null ? field.ToString() : null);
                field.Clear();
            }

            void EndRecord()
            {
                if (headers is null)
                {
                    headers = [.. fields];
                    fields.Clear();

                    // Always notify when header is available
                    nextUpdate = progress?.Invoke(headers.AsReadOnly(), 0) ?? 1;
                }
                else
                {
                    rowCount++;

                    if (fields.Count != headers.Count)
                        throw new FormatException($"Invalid field count in row {rowCount}: got {fields.Count}, expected {headers.Count}. Each record must match the header column count and end with a line break.");

                    // Check if we should notify progress
                    if (rowCount == nextUpdate)
                    {
                        // Get rows until next update (default to 1 if callback returns <= 0)
                        var interval = progress?.Invoke(headers.AsReadOnly(), rowCount) ?? 1;
                        nextUpdate = rowCount + Math.Max(1, interval);
                    }

                    fields.Clear();
                }
            }

            int read;
            while ((read = await reader.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false)) > 0)
            {
                for (int i = 0; i < read; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    char ch = buffer[i];

                    if (lastWasCR)
                    {
                        lastWasCR = false;
                        if (ch == '\n' && !inQuotes)
                        {
                            EndField();
                            EndRecord();
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
                        EndRecord();
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
                        if (headers is null)
                            field.Append(ch); // accumulate header fields
                    }
                }
            }

            // Handle trailing CR as a record terminator.
            if (lastWasCR && !inQuotes)
            {
                EndField();
                EndRecord();
            }

            // Flush any remaining data as the final record (handles missing trailing newline).
            if (field.Length > 0 || fields.Count > 0)
            {
                EndField();
                EndRecord();
            }

            // Always notify on completion if we processed any rows after last update
            if (rowCount > 0 && rowCount >= nextUpdate)
                progress?.Invoke(headers.AsReadOnly(), rowCount);

            return (headers.AsReadOnly(), rowCount);
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

        throw new FormatException("Invalid Concordance DAT. After an optional BOM, the file must begin with the quote character U+00FE."
            + "The detected byte pattern does not match UTF-8 or UTF-16 (LE/BE) with U+00FE at the start. ");
    }
}
