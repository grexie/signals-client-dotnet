using System.Runtime.CompilerServices;
using Grexie.Signals.Client;
using Xunit;

namespace Grexie.Signals.Client.Tests;

public sealed class SignalsManagerTests
{
    [Fact]
    public async Task SubscribesWithStateAndEmitsRouterIntents()
    {
        var client = new FakeClient(
            new SubscribedEvent(9, "okx", "BTC-USDT-SWAP"),
            new CreateMarketOrderEvent(9, "intent_1", "open_position", "entry", "okx", "BTC-USDT-SWAP", "buy", "market", 3, 1, false, 0, 0, 0, 0, null));
        var manager = new SignalsManager(
            client,
            new SignalsManagerState(
                new[] { new AssetSnapshot { Venue = "okx", Currency = "USDT", Available = 100, Equity = 100 } },
                new[] { new Position { Venue = "okx", Instrument = "BTC-USDT-SWAP", Size = 2, EntryPrice = 100, LastPrice = 101 } }),
            new SignalsManagerConfig { Venue = "okx", Instruments = new[] { "BTC-USDT-SWAP" } });

        await manager.RunAsync();

        Assert.Equal("subscribe", client.Sent[0]["type"]);
        Assert.Equal(1, ((RiskConfig)client.Sent[0]["risk"]).MaxMarginRatio);
        Assert.Equal(0, ((RiskConfig)client.Sent[0]["risk"]).MinLotHaircutRatio);
        Assert.True(client.Sent.Any(item => (string)item["type"] == "update-asset" && (long)item["subscriptionId"] == 9));
        Assert.True(client.Sent.Any(item => (string)item["type"] == "update-position" && (long)item["subscriptionId"] == 9));
        var intent = await manager.Intents.Reader.ReadAsync();
        Assert.Equal("intent_1", intent.IntentId);
        Assert.Equal(3, intent.ContractSize);
    }

    [Fact]
    public async Task UpdatesSnapshotsAfterSubscription()
    {
        var client = new FakeClient();
        var manager = new SignalsManager(client, config: new SignalsManagerConfig { Venue = "okx", Instruments = new[] { "ETH-USDT-SWAP" } });
        await manager.HandleEventAsync(new SubscribedEvent(15, "okx", "ETH-USDT-SWAP"));

        await manager.UpdateAssetAsync(new AssetSnapshot { Currency = "usdt", Available = 50, MaxUsage = 0.5 });
        await manager.UpdatePositionAsync(new Position { Instrument = "ETH-USDT-SWAP", Size = -4, EntryPrice = 2000 });

        Assert.Equal(25, manager.AvailableOrderCash("USDT"));
        Assert.Equal("open", manager.State().Positions![0].Status);
        Assert.True(client.Sent.Any(item => (string)item["type"] == "update-asset" && (string)item["currency"] == "USDT"));
        Assert.True(client.Sent.Any(item => (string)item["type"] == "update-position" && (string)item["side"] == "sell" && (double)item["size"] == 4));
    }

    [Fact]
    public async Task ReconnectsAndResubscribesWithCurrentState()
    {
        var client = new ReconnectingFakeClient();
        var manager = new SignalsManager(client, config: new SignalsManagerConfig { Venue = "okx", Instruments = new[] { "BTC-USDT-SWAP" } });
        using var cts = new CancellationTokenSource();
        var run = manager.RunAsync(cts.Token);

        await WaitForAsync(() => manager.SubscriptionId == 7);
        await manager.UpdateAssetAsync(new AssetSnapshot { Currency = "usdt", Available = 100, Equity = 100 });
        await manager.UpdatePositionAsync(new Position { Instrument = "BTC-USDT-SWAP", Size = 2, EntryPrice = 100, LastPrice = 101 });
        client.Drop();

        await WaitForAsync(() => client.Sent.Count(item => (string)item["type"] == "subscribe") >= 2);
        var replay = client.Sent.Last(item => (string)item["type"] == "subscribe");
        var assets = Assert.IsAssignableFrom<IReadOnlyList<AssetSnapshot>>(replay["assets"]);
        var positions = Assert.IsAssignableFrom<IReadOnlyList<Position>>(replay["positions"]);
        Assert.Equal("USDT", assets[0].Currency);
        Assert.Equal("BTC-USDT-SWAP", positions[0].Instrument);

        cts.Cancel();
        await run;
    }

    private static async Task WaitForAsync(Func<bool> predicate, int timeoutMs = 3000)
    {
        var deadline = DateTimeOffset.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (predicate()) return;
            await Task.Delay(10);
        }
        throw new TimeoutException("timed out waiting for condition");
    }
}

internal sealed class FakeClient(params SignalsEvent[] events) : ISignalsManagerClient
{
    public List<Dictionary<string, object>> Sent { get; } = new();

    public async IAsyncEnumerable<SignalsEvent> EventsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var ev in events)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return ev;
            await Task.Yield();
        }
    }

    public Task SubscribeBasketAsync(string venue, IReadOnlyList<string> instruments, RiskConfig? risk = null, double profitWithdrawRatio = 0, IReadOnlyList<AssetSnapshot>? assets = null, IReadOnlyList<Position>? positions = null, string? mode = null, CancellationToken cancellationToken = default)
    {
        Sent.Add(new Dictionary<string, object> { ["type"] = "subscribe", ["venue"] = venue, ["instruments"] = instruments, ["risk"] = risk ?? new RiskConfig(), ["assets"] = assets ?? Array.Empty<AssetSnapshot>(), ["positions"] = positions ?? Array.Empty<Position>() });
        return Task.CompletedTask;
    }

    public Task UpdateAssetAsync(long subscriptionId, AssetSnapshot asset, CancellationToken cancellationToken = default)
    {
        Sent.Add(new Dictionary<string, object> { ["type"] = "update-asset", ["subscriptionId"] = subscriptionId, ["currency"] = asset.Currency });
        return Task.CompletedTask;
    }

    public Task UpdatePositionAsync(long subscriptionId, Position position, CancellationToken cancellationToken = default)
    {
        Sent.Add(new Dictionary<string, object> { ["type"] = "update-position", ["subscriptionId"] = subscriptionId, ["instrument"] = position.Instrument, ["side"] = position.Side?.ToString().ToLowerInvariant() ?? "", ["size"] = Math.Abs(position.Size) });
        return Task.CompletedTask;
    }

    public Task AddInstrumentAsync(long subscriptionId, string instrument, CancellationToken cancellationToken = default)
    {
        Sent.Add(new Dictionary<string, object> { ["type"] = "add-instrument", ["subscriptionId"] = subscriptionId, ["instrument"] = instrument });
        return Task.CompletedTask;
    }

    public Task RemoveInstrumentAsync(long subscriptionId, string instrument, CancellationToken cancellationToken = default)
    {
        Sent.Add(new Dictionary<string, object> { ["type"] = "remove-instrument", ["subscriptionId"] = subscriptionId, ["instrument"] = instrument });
        return Task.CompletedTask;
    }

    public Task UpdateConfigAsync(long subscriptionId, RuntimeConfig config, CancellationToken cancellationToken = default)
    {
        Sent.Add(new Dictionary<string, object> { ["type"] = "update-config", ["subscriptionId"] = subscriptionId, ["config"] = config });
        return Task.CompletedTask;
    }

    public Task ScheduleWithdrawalAsync(long subscriptionId, string currency, double amount, string? venue = null, string? reason = null, CancellationToken cancellationToken = default)
    {
        Sent.Add(new Dictionary<string, object> { ["type"] = "schedule-withdrawal", ["subscriptionId"] = subscriptionId, ["currency"] = currency, ["amount"] = amount, ["venue"] = venue ?? "", ["reason"] = reason ?? "" });
        return Task.CompletedTask;
    }

    public Task UnsubscribeAsync(long subscriptionId, CancellationToken cancellationToken = default)
    {
        Sent.Add(new Dictionary<string, object> { ["type"] = "unsubscribe", ["subscriptionId"] = subscriptionId });
        return Task.CompletedTask;
    }
}

internal sealed class ReconnectingFakeClient : ISignalsManagerClient, ISignalsReconnectableClient
{
    private readonly TaskCompletionSource _drop = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _attempt;

    public List<Dictionary<string, object>> Sent { get; } = new();
    public int Connects { get; private set; }

    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        Connects++;
        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<SignalsEvent> EventsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _attempt++;
        if (_attempt == 1)
        {
            yield return new SubscribedEvent(7, "okx", "BTC-USDT-SWAP");
            await _drop.Task.WaitAsync(cancellationToken);
            throw new IOException("websocket closed");
        }
        yield return new SubscribedEvent(8, "okx", "BTC-USDT-SWAP");
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(10, cancellationToken);
        }
    }

    public void Drop() => _drop.TrySetResult();

    public Task SubscribeBasketAsync(string venue, IReadOnlyList<string> instruments, RiskConfig? risk = null, double profitWithdrawRatio = 0, IReadOnlyList<AssetSnapshot>? assets = null, IReadOnlyList<Position>? positions = null, string? mode = null, CancellationToken cancellationToken = default)
    {
        Sent.Add(new Dictionary<string, object> { ["type"] = "subscribe", ["venue"] = venue, ["instruments"] = instruments, ["risk"] = risk ?? new RiskConfig(), ["assets"] = assets ?? Array.Empty<AssetSnapshot>(), ["positions"] = positions ?? Array.Empty<Position>() });
        return Task.CompletedTask;
    }

    public Task UpdateAssetAsync(long subscriptionId, AssetSnapshot asset, CancellationToken cancellationToken = default)
    {
        Sent.Add(new Dictionary<string, object> { ["type"] = "update-asset", ["subscriptionId"] = subscriptionId, ["currency"] = asset.Currency });
        return Task.CompletedTask;
    }

    public Task UpdatePositionAsync(long subscriptionId, Position position, CancellationToken cancellationToken = default)
    {
        Sent.Add(new Dictionary<string, object> { ["type"] = "update-position", ["subscriptionId"] = subscriptionId, ["instrument"] = position.Instrument, ["side"] = position.Side?.ToString().ToLowerInvariant() ?? "", ["size"] = Math.Abs(position.Size) });
        return Task.CompletedTask;
    }

    public Task AddInstrumentAsync(long subscriptionId, string instrument, CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task RemoveInstrumentAsync(long subscriptionId, string instrument, CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task UpdateConfigAsync(long subscriptionId, RuntimeConfig config, CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task ScheduleWithdrawalAsync(long subscriptionId, string currency, double amount, string? venue = null, string? reason = null, CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task UnsubscribeAsync(long subscriptionId, CancellationToken cancellationToken = default) => Task.CompletedTask;
}
