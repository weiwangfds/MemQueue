namespace LiteQueue.Exceptions;

/// <summary>
/// Exception thrown when a buffer overflow occurs.
/// </summary>
public class BufferOverflowException : Exception
{
    /// <summary>
    /// Initializes a new instance of the BufferOverflowException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public BufferOverflowException(string message) : base(message) { }
}
