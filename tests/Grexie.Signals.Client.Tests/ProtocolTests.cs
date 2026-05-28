using Xunit;

namespace Grexie.Signals.Client.Tests;

public sealed class ProtocolTests
{
    [Fact]
    public void ParsesSignalReplayEvents()
    {
        var ev = SignalsEventParser.Parse("""
        {"type":"signal","subscriptionId":7,"venue":"okx","instrument":"BTC-USDT-SWAP","timestamp":"2026-05-26T00:00:00Z","replay":true,"signal":{"confidence":0.8,"side":"buy","takeProfit":0.01,"stopLoss":0.004,"trailingStopActivation":0.02,"trailingStopDistance":0.01,"trailingStopMinProfit":0.001}}
        """);

        var signal = Assert.IsType<SignalEvent>(ev);
        Assert.Equal(7, signal.SubscriptionId);
        Assert.Equal("okx", signal.Signal.Venue);
        Assert.Equal("BTC-USDT-SWAP", signal.Signal.Instrument);
        Assert.Equal(0.02, signal.Signal.TrailingStopActivation, 9);
        Assert.Equal(0.01, signal.Signal.TrailingStopDistance, 9);
        Assert.Equal(0.001, signal.Signal.TrailingStopMinProfit, 9);
        Assert.True(signal.Replay);
    }

    [Fact]
    public void ParsesInfoAndErrorEvents()
    {
        var info = Assert.IsType<InfoEvent>(SignalsEventParser.Parse("""
        {"type":"info","subscriptionId":3,"venue":"okx","instrument":"DOGE-USDT-SWAP","stage":"ready","message":"ready","replay":true,"replayedAt":"2026-05-26T00:00:01Z"}
        """));
        Assert.Equal("ready", info.Stage);
        Assert.True(info.Replay);
        Assert.NotNull(info.ReplayedAt);

        var error = Assert.IsType<ErrorEvent>(SignalsEventParser.Parse("""
        {"type":"error","code":"forbidden","message":"no access"}
        """));
        Assert.Equal("forbidden", error.Code);
        Assert.Equal("no access", error.Message);
    }
}
