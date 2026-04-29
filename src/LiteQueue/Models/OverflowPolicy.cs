namespace LiteQueue.Models;

public enum OverflowPolicy
{
    /// <summary>Overwrite oldest entries when buffer is full. Default.</summary>
    OverwriteOldest = 0,
    /// <summary>Block the producer until space is freed (backpressure). Zero data loss.</summary>
    Block = 1,
    /// <summary>Reject new messages when buffer is full.</summary>
    DropNewest = 2
}
