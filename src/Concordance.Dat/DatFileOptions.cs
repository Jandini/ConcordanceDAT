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
    /// Controls how empty fields are represented in the output dictionary:
    /// - Null: included with value null
    /// - Keep: included with empty string
    /// - Omit: not included at all (default is Null)
    /// </summary>
    public EmptyField EmptyFieldMode { get; init; } = EmptyField.Null;

    /// <summary>
    /// A reusable default instance with conservative, high-throughput settings.
    /// </summary>
    public static DatFileOptions Default { get; } = new DatFileOptions();

    internal DatFileOptions Clamp() => this with
    {
        ReaderBufferChars = Math.Clamp(ReaderBufferChars, 4 * 1024, 1 * 1024 * 1024),
        ParseChunkChars = Math.Clamp(ParseChunkChars, 4 * 1024, 1 * 1024 * 1024),
    };
}

