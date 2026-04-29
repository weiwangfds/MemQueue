namespace LiteQueue;

/// <summary>
/// Attribute for marking classes as message subscribers.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
public sealed class SubscribeAttribute : Attribute
{
    /// <summary>
    /// Gets the topic name to subscribe to.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets or sets the consumer group identifier.
    /// </summary>
    public string? GroupId { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether auto-commit is enabled.
    /// </summary>
    public bool AutoCommit { get; set; } = true;

    /// <summary>
    /// Initializes a new instance of the SubscribeAttribute class.
    /// </summary>
    /// <param name="topic">The topic name to subscribe to.</param>
    public SubscribeAttribute(string topic)
    {
        Topic = topic;
    }
}
