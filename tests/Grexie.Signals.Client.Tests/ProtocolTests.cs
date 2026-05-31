using Xunit;

namespace Grexie.Signals.Client.Tests;

public sealed class ProtocolTests
{
    [Fact]
    public void ParsesSignalReplayEvents()
    {
        var ev = SignalsEventParser.Parse("""
        {"type":"signal","subscriptionId":7,"venue":"okx","instrument":"BTC-USDT-SWAP","timestamp":"2026-05-26T00:00:00Z","replay":true,"signal":{"confidence":0.8,"side":"buy","takeProfit":0.01,"stopLoss":0.004,"trailingStopActivation":0.02,"trailingStopDistance":0.01,"trailingStopMinProfit":0.001,"managePositionsOnly":true}}
        """);

        var signal = Assert.IsType<SignalEvent>(ev);
        Assert.Equal(7, signal.SubscriptionId);
        Assert.Equal("okx", signal.Signal.Venue);
        Assert.Equal("BTC-USDT-SWAP", signal.Signal.Instrument);
        Assert.Equal(0.02, signal.Signal.TrailingStopActivation, 9);
        Assert.Equal(0.01, signal.Signal.TrailingStopDistance, 9);
        Assert.Equal(0.001, signal.Signal.TrailingStopMinProfit, 9);
        Assert.True(signal.Signal.ManagePositionsOnly);
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

    [Fact]
    public void ParsesOrderRouterEvents()
    {
        var order = Assert.IsType<CreateMarketOrderEvent>(SignalsEventParser.Parse("""
        {"type":"create-market-order","subscriptionId":12,"intentId":"intent_1","reason":"preempted_by_better_route","venue":"okx","instrument":"BTC-USDT-SWAP","side":"buy","contractSize":3}
        """));
        Assert.Equal("preempted_by_better_route", order.Reason);

        var tpsl = Assert.IsType<UpdateTPSLEvent>(SignalsEventParser.Parse("""
        {"type":"update-tpsl","subscriptionId":12,"intentId":"intent_2","venue":"okx","instrument":"BTC-USDT-SWAP","side":"buy","takeProfitPrice":72100,"stopLossPrice":70050,"takeProfit":0.03,"stopLoss":0.0007}
        """));
        Assert.Equal(72100, tpsl.TakeProfitPrice);
        Assert.Equal(70050, tpsl.StopLossPrice);

        var withdraw = Assert.IsType<WithdrawEvent>(SignalsEventParser.Parse("""
        {"type":"withdraw","subscriptionId":12,"intentId":"withdraw_1","venue":"okx","currency":"USDT","amount":42}
        """));
        Assert.Equal("USDT", withdraw.Currency);
        Assert.Equal(42, withdraw.Amount);
    }
}
