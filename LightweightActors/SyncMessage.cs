namespace LightweightActors;

internal sealed class SyncMessage(Action action) : Message
{
    public override ValueTask ExecuteAsync()
    {
        action();
        return ValueTask.CompletedTask;
    }
}