namespace LightweightActors;

internal sealed class SyncMessage(Action action) : Message
{
    public override ValueTask ExecuteAsync()
    {
        action();
        return ValueTask.CompletedTask;
    }
}

internal sealed class SyncMessage<TState>(TState state, Action<TState> action) : Message
{
    public override ValueTask ExecuteAsync()
    {
        action(state);
        return ValueTask.CompletedTask;
    }
}