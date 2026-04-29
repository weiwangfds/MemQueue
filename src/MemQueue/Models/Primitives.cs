namespace MemQueue.Models;

public readonly record struct TopicId(string Value)
{
    public override string ToString() => Value;
    public static implicit operator string(TopicId id) => id.Value;
    public static explicit operator TopicId(string s) => new(s);
}

public readonly record struct ConsumerGroupId(string Value)
{
    public override string ToString() => Value;
    public static implicit operator string(ConsumerGroupId id) => id.Value;
    public static explicit operator ConsumerGroupId(string s) => new(s);
}

public readonly record struct ConsumerId(string Value)
{
    public override string ToString() => Value;
    public static implicit operator string(ConsumerId id) => id.Value;
    public static explicit operator ConsumerId(string s) => new(s);
}

public readonly record struct PartitionId(int Value) : IComparable<PartitionId>
{
    public override string ToString() => Value.ToString();
    public static implicit operator int(PartitionId id) => id.Value;
    public int CompareTo(PartitionId other) => Value.CompareTo(other.Value);
}

public readonly record struct Offset(long Value) : IComparable<Offset>
{
    public static readonly Offset Unset = new(-1);
    public bool IsValid => Value >= 0;
    public Offset Next() => new(Value + 1);
    public int CompareTo(Offset other) => Value.CompareTo(other.Value);
    public static implicit operator long(Offset o) => o.Value;
    public override string ToString() => Value.ToString();
}
