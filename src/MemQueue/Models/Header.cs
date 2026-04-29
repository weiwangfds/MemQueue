namespace MemQueue.Models;

/// <summary>
/// A single message header with a string key and binary value.
/// Mirrors Kafka's Header interface.
/// </summary>
public readonly record struct Header(string Key, byte[]? Value);
