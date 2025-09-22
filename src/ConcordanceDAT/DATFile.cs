using System.Buffers;
using System.Text;

namespace ConcordanceDAT;

public static class DATFile
{
    public static async IAsyncEnumerable<Dictionary<string, string>> ReadAsync(Stream stream, int readerBufferChars = 128 * 1024, int parseChunkChars = 128 * 1024,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancel = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        
        if (!stream.CanRead) 
            throw new ArgumentException("Stream must be readable.", nameof(stream));

        readerBufferChars = Math.Clamp(readerBufferChars, 4 * 1024, 1 * 1024 * 1024);
        parseChunkChars = Math.Clamp(parseChunkChars, 4 * 1024, 1 * 1024 * 1024);

        var encoding = DetectEncoding(stream);
        using var reader = new StreamReader(stream, encoding, detectEncodingFromByteOrderMarks: false, bufferSize: readerBufferChars, leaveOpen: false);

        await foreach (var row in GetRowsAsync(reader, parseChunkChars, cancel).ConfigureAwait(false))
            yield return row;
    }

    private static async IAsyncEnumerable<Dictionary<string, string>> GetRowsAsync(
       StreamReader reader,
        int parseChunkChars,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancel)
    {
        var pool = ArrayPool<char>.Shared;
        var buffer = pool.Rent(parseChunkChars);
        try
        {
            var sep = (char)0x14; // column separator
            var quote = (char)0xFE; // field quote

            var inQuotes = false;
            var lastWasCR = false;

            var field = new StringBuilder(8192);
            var fields = new List<string>(128);
            List<string> headers = null;

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
                    return []; // skip header row
                }

                // strict column-count validation
                if (fields.Count != headers.Count)
                    throw new FormatException($"Invalid field count: got {fields.Count}, expected {headers.Count}. " +
                                              "Each record must have the same number of columns as the header and end with a line break.");

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
                            continue; // consume LF
                        }
                        else
                        {
                            field.Append('\r');
                            // fall through to handle ch
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
                        EndField();
                        var row = EndRecord();
                        if (row.Count > 0) yield return row;
                    }
                    else if (ch == '\r')
                    {
                        if (!inQuotes) lastWasCR = true;
                        else field.Append('\r');
                    }
                    else
                    {
                        field.Append(ch);
                    }
                }
            }

            if (lastWasCR && !inQuotes)
            {
                EndField();
                var rowCR = EndRecord();
                if (rowCR.Count > 0) yield return rowCR;
            }

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

    private static Encoding DetectEncoding(Stream stream)
    {
        long pos = stream.CanSeek ? stream.Position : 0;
        using var br = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);
        Span<byte> bom = stackalloc byte[3];
        int got = br.Read(bom);
        if (stream.CanSeek) stream.Position = pos;

        if (got >= 3 && bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF) return new UTF8Encoding(false);
        if (got >= 2 && bom[0] == 0xFF && bom[1] == 0xFE) return Encoding.Unicode;          // UTF-16 LE
        if (got >= 2 && bom[0] == 0xFE && bom[1] == 0xFF) return Encoding.BigEndianUnicode; // UTF-16 BE
        return new UTF8Encoding(false);
    }
}