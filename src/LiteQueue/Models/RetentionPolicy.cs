namespace LiteQueue.Models;

/// <summary>
/// Defines how long messages are retained in the queue using a sealed discriminated union pattern.
/// </summary>
public abstract record RetentionPolicy
{
    private RetentionPolicy() { }

    /// <summary>
    /// Retention policy based on message count only.
    /// </summary>
    public sealed record ByCount(int MaxMessages) : RetentionPolicy
    {
        /// <summary>
        /// Creates a retention policy based on message count.
        /// </summary>
        /// <param name="n">The maximum number of messages to retain.</param>
        /// <returns>A new retention policy.</returns>
        public static ByCount Of(int n) => new(n);
    }

    /// <summary>
    /// Retention policy based on message age only.
    /// </summary>
    public sealed record ByAge(TimeSpan MaxAge) : RetentionPolicy
    {
        /// <summary>
        /// Creates a retention policy based on message age.
        /// </summary>
        /// <param name="t">The maximum age of messages to retain.</param>
        /// <returns>A new retention policy.</returns>
        public static ByAge Of(TimeSpan t) => new(t);
    }

    /// <summary>
    /// Retention policy based on both message count and age.
    /// </summary>
    public sealed record ByCountOrAge(int MaxMessages, TimeSpan MaxAge) : RetentionPolicy;

    /// <summary>
    /// No retention policy (retain all messages indefinitely).
    /// </summary>
    public static RetentionPolicy None { get; } = new ByCount(int.MaxValue);
}
