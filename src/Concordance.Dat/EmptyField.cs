namespace Concordance.Dat;

/// <summary>
/// Specifies how empty fields are handled in the output dictionary.
/// </summary>
public enum EmptyField
{
    /// <summary>
    /// Empty fields are included in the dictionary with a value of null.
    /// </summary>
    Null,
    /// <summary>
    /// Empty fields are included in the dictionary with an empty string value.
    /// </summary>
    Keep,
    /// <summary>
    /// Empty fields are omitted from the output dictionary.
    /// </summary>
    Omit
}
