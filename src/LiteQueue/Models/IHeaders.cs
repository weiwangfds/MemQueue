namespace LiteQueue.Models;

/// <summary>
/// Ordered collection of message headers. Mirrors Kafka's Headers interface.
/// </summary>
public interface IHeaders : IEnumerable<Header>
{
    /// <summary>
    /// Add a header with the given key and binary value.
    /// </summary>
    IHeaders Add(string key, byte[]? value);

    /// <summary>
    /// Add a header.
    /// </summary>
    IHeaders Add(Header header);

    /// <summary>
    /// Remove all headers with the given key.
    /// </summary>
    IHeaders Remove(string key);

    /// <summary>
    /// Get the last header with the given key, or null if not found.
    /// </summary>
    Header? LastHeader(string key);

    /// <summary>
    /// Get all headers with the given key.
    /// </summary>
    IEnumerable<Header> this[string key] { get; }

    /// <summary>
    /// Number of headers.
    /// </summary>
    int Count { get; }
}
