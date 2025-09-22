using System.Buffers;
using System.Text;

namespace ConcordanceDAT;

/// <summary>
/// Asynchronous, streaming reader for Concordance DAT files.
/// </summary>
public static class DATFile
{
    /// <summary>
    /// Entry point. Wraps the stream with a BOM-aware encoding, then streams rows via an inner async iterator.
    /// readerBufferChars controls the internal <see cref="StreamReader"/> buffer (in chars),
    /// parseChunkChars controls the size of the pooled char[] used for per-iteration decoding/parsing.
    /// </summary>
    public static async IAsyncEnumerable<Dictionary<string, string>> ReadAsync(
        Stream stream,
        int readerBufferChars = 128 * 1024,
        int parseChunkChars = 128 * 1024,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancel = default)
    {
        // Validate input stream early; enumeration is lazy but we fail fast on obvious misuses.
        ArgumentNullException.ThrowIfNull(stream);

        if (!stream.CanRead)
            throw new ArgumentException("Stream must be readable.", nameof(stream));

        // Clamp buffer sizes to reasonable bounds (avoid tiny or very large allocations).
        // Note these are in *characters* (not bytes). UTF-16 will consume ~2x bytes per char.
        readerBufferChars = Math.Clamp(readerBufferChars, 4 * 1024, 1 * 1024 * 1024);
        parseChunkChars = Math.Clamp(parseChunkChars, 4 * 1024, 1 * 1024 * 1024);

        // BOM detection (UTF-8/UTF-16 LE/BE). If no BOM, default to UTF-8 without BOM.
        var encoding = DetectEncoding(stream);

        // StreamReader is used solely for text decoding; we drive it with ReadAsync into a pooled char buffer below.
        // detectEncodingFromByteOrderMarks:false because we already handled BOM; leaveOpen:false so the stream is disposed here.
        using var reader = new StreamReader(stream, encoding, detectEncodingFromByteOrderMarks: false, bufferSize: readerBufferChars, leaveOpen: false);

        // Defer actual parsing to the async iterator that yields one Dictionary per record (excluding header).
        await foreach (var row in GetRowsAsync(reader, parseChunkChars, cancel).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// Core async iterator that:
    /// - Decodes into a pooled char[] in chunks
    /// - Parses fields honoring quotes and separators
    /// - Builds and yields a dictionary per record
    /// 
    /// Implementation detail:
    /// We avoid keeping Span&lt;char&gt; locals across await/yield boundaries (a C# 13 restriction for byref-like types).
    /// Instead we index directly into the pooled char[] buffer.
    /// </summary>
    private static async IAsyncEnumerable<Dictionary<string, string>> GetRowsAsync(
       StreamReader reader,
        int parseChunkChars,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancel)
    {
        // Rent a pooled char[] for decode/parsing. Large enough to amortize syscalls, small enough to keep memory steady.
        var pool = ArrayPool<char>.Shared;
        var buffer = pool.Rent(parseChunkChars);
        try
        {
            // DAT dialect: 0x14 is the field separator, 0xFE is the field quote/qualifier.
            var sep = (char)0x14; // column separator
            var quote = (char)0xFE; // field quote

            // Parsing state flags:
            // - inQuotes : inside a quoted field (record terminators and separators are ignored until we close quotes).
            // - lastWasCR: encountered '\r' and are waiting to see if it is followed by '\n' to form CRLF across chunk boundaries.
            var inQuotes = false;
            var lastWasCR = false;

            // Reused builders/containers to minimize per-record allocations.
            // StringBuilder accumulates the current field's text (including any embedded newlines if inQuotes).
            // 'fields' collects the completed fields for the current record.
            var field = new StringBuilder(8192);
            var fields = new List<string>(128);
            List<string> headers = null; // Will be set from the first record; that record is *not* yielded.

            // Finalize the current field: take the accumulated chars as the field value and reset the builder.
            void EndField()
            {
                fields.Add(field.ToString());
                field.Clear();
            }

            // Finalize the current record:
            // - If this is the first record, treat it as the header (capture column names) and do not yield a row.
            // - Otherwise, validate field count, build a dictionary keyed by header names, and return it for yield.
            Dictionary<string, string> EndRecord()
            {
                if (headers is null)
                {
                    // Header row: copy current fields as header names; clear 'fields' to prepare for first data row.
                    headers = [.. fields];
                    fields.Clear();
                    return []; // skip header row
                }

                // strict column-count validation
                // Enforces: same number of fields as headers; also acts as a guard to detect missing separators/terminators.
                if (fields.Count != headers.Count)
                    throw new FormatException($"Invalid field count: got {fields.Count}, expected {headers.Count}. " +
                                              "Each record must have the same number of columns as the header and end with a line break.");

                // Build the row dictionary.
                // Note: if headers had duplicates, later keys overwrite earlier ones (Dictionary indexer semantics).
                var dict = new Dictionary<string, string>(headers.Count, StringComparer.OrdinalIgnoreCase);
                var count = Math.Max(headers.Count, fields.Count);
                for (int i = 0; i < count; i++)
                {
                    var key = i < headers.Count ? headers[i] : $"Column{i + 1}";
                    var val = i < fields.Count ? fields[i] : string.Empty;
                    dict[key] = val;
                }
                fields.Clear();
                return dict;
            }

            // Read/decode text into buffer and parse it chunk-by-chunk.
            int read;
            while ((read = await reader.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false)) > 0)
            {
                for (int i = 0; i < read; i++)
                {
                    // Allow consumers to cancel mid-record (important for very large fields).
                    cancel.ThrowIfCancellationRequested();

                    char ch = buffer[i];

                    // If previous char was CR and we are NOT in quotes, check for CRLF (spanning across chunk boundaries).
                    if (lastWasCR)
                    {
                        lastWasCR = false;
                        if (ch == '\n' && !inQuotes)
                        {
                            // CRLF terminates a record when not inside quotes.
                            EndField();
                            var row = EndRecord();
                            if (row.Count > 0) yield return row; // skip header (EndRecord returns empty)
                            continue; // consume LF and move on
                        }
                        else
                        {
                            // The CR was literal content (because either we're inside quotes or the next char wasn't LF).
                            field.Append('\r');
                            // fall through to handle current character normally
                        }
                    }

                    if (ch == quote)
                    {
                        // If we are inside quotes and the next char is also a quote, this is an escaped quote ("" → ").
                        // We materialize a single literal 0xFE into the field and skip the second char.
                        if (inQuotes && i + 1 < read && buffer[i + 1] == quote)
                        {
                            field.Append(quote); // escaped quote
                            i++;
                        }
                        else
                        {
                            // Toggle quote state: start or end of a quoted field.
                            // Outer quotes are not written into the field.
                            inQuotes = !inQuotes; // toggle
                        }
                    }
                    else if (!inQuotes && ch == sep)
                    {
                        // Separator only counts when NOT inside quotes.
                        EndField();
                    }
                    else if (!inQuotes && ch == '\n')
                    {
                        // LF (without a preceding CR in this same chunk) also terminates a record when not in quotes.
                        EndField();
                        var row = EndRecord();
                        if (row.Count > 0) yield return row;
                    }
                    else if (ch == '\r')
                    {
                        if (!inQuotes)
                            // Might be CRLF. We delay decision to the next character (possibly in the next chunk).
                            lastWasCR = true;
                        else
                            // CR inside a quoted field is literal content.
                            field.Append('\r');
                    }
                    else
                    {
                        // Regular data character (including newlines if inQuotes).
                        field.Append(ch);
                    }
                }
            }

            // End-of-stream handling:

            // If the very last char we saw was a CR (not in quotes) and we've reached EOF,
            // treat it as a record terminator (a lone CR, or CR ending the file without LF).
            if (lastWasCR && !inQuotes)
            {
                EndField();
                var rowCR = EndRecord();
                if (rowCR.Count > 0) yield return rowCR;
            }

            // If we still have buffered field/record content at EOF, flush it as the final (unterminated) record.
            // This accepts files that omit a trailing newline, which is common with some exporters.
            // Column-count validation still applies inside EndRecord().
            if (field.Length > 0 || fields.Count > 0)
            {
                EndField();
                var last = EndRecord();
                if (last.Count > 0) yield return last;
            }
        }
        finally
        {
            // Always return the pooled buffer to avoid leaks/fragmentation.
            pool.Return(buffer);
        }
    }

    /// <summary>
    /// Minimal BOM-based encoding detection:
    /// - UTF-8 with BOM → UTF8 (without emitting BOM on writes)
    /// - UTF-16 LE/BE   → Unicode/BigEndianUnicode
    /// - Otherwise      → UTF8 (no BOM)
    /// 
    /// We preserve the original stream position when seekable.
    /// </summary>
    private static Encoding DetectEncoding(Stream stream)
    {
        long pos = stream.CanSeek ? stream.Position : 0;

        // Use BinaryReader to peek a few bytes for BOM detection; leave the stream open.
        using var br = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);

        // 3 bytes are enough to detect UTF-8 BOM; 2 bytes detect UTF-16 BOMs.
        Span<byte> bom = stackalloc byte[3];
        int got = br.Read(bom);

        // Rewind to original position so the caller/StreamReader can re-read from the correct start.
        if (stream.CanSeek) stream.Position = pos;

        // UTF-8 BOM
        if (got >= 3 && bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF) return new UTF8Encoding(false);
        // UTF-16 LE BOM
        if (got >= 2 && bom[0] == 0xFF && bom[1] == 0xFE) return Encoding.Unicode;          // UTF-16 LE
        // UTF-16 BE BOM
        if (got >= 2 && bom[0] == 0xFE && bom[1] == 0xFF) return Encoding.BigEndianUnicode; // UTF-16 BE

        // Default: UTF-8 without BOM (common for Concordance exports).
        return new UTF8Encoding(false);
    }
}
