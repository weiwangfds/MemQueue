using System.Collections;

namespace MemQueue.Models;

/// <summary>
/// Default in-memory implementation of IHeaders.
/// </summary>
public sealed class Headers : IHeaders
{
    private readonly List<Header> _headers = [];

    /// <summary>
    /// Gets the number of headers.
    /// </summary>
    public int Count => _headers.Count;

    /// <summary>
    /// Gets all headers with the specified key.
    /// </summary>
    /// <param name="key">The header key.</param>
    /// <returns>A collection of headers matching the key.</returns>
    public IEnumerable<Header> this[string key]
        => _headers.Where(h => h.Key == key);

    /// <summary>
    /// Adds a header with the specified key and value.
    /// </summary>
    /// <param name="key">The header key.</param>
    /// <param name="value">The header value.</param>
    /// <returns>This headers instance for chaining.</returns>
    public IHeaders Add(string key, byte[]? value)
    {
        _headers.Add(new Header(key, value));
        return this;
    }

    /// <summary>
    /// Adds a header.
    /// </summary>
    /// <param name="header">The header to add.</param>
    /// <returns>This headers instance for chaining.</returns>
    public IHeaders Add(Header header)
    {
        _headers.Add(header);
        return this;
    }

    /// <summary>
    /// Removes all headers with the specified key.
    /// </summary>
    /// <param name="key">The header key.</param>
    /// <returns>This headers instance for chaining.</returns>
    public IHeaders Remove(string key)
    {
        _headers.RemoveAll(h => h.Key == key);
        return this;
    }

    /// <summary>
    /// Gets the last header with the specified key.
    /// </summary>
    /// <param name="key">The header key.</param>
    /// <returns>The last header matching the key, or null if not found.</returns>
    public Header? LastHeader(string key)
        => _headers.FindLast(h => h.Key == key);

    /// <summary>
    /// Returns an enumerator that iterates through the headers.
    /// </summary>
    /// <returns>An enumerator for the headers.</returns>
    public IEnumerator<Header> GetEnumerator() => _headers.GetEnumerator();

    /// <summary>
    /// Returns an enumerator that iterates through the headers.
    /// </summary>
    /// <returns>An enumerator for the headers.</returns>
    IEnumerator IEnumerable.GetEnumerator() => _headers.GetEnumerator();
}
