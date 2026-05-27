# Grexie Signals .NET Client

Typed C# client package for Grexie Signals websocket subscriptions and production-style in-memory position management.

```sh
dotnet add package Grexie.Signals.Client --version 0.1.7
```

## Websocket Client

```csharp
using Grexie.Signals.Client;

await using var client = new SignalsClient(new SignalsWebSocketToken("ws_your_token"));
await client.ConnectAsync();
await client.SubscribeAsync("okx", "BTC-USDT-SWAP");

while (await client.ReceiveAsync() is { } ev)
{
    if (ev is SignalEvent signal)
    {
        Console.WriteLine($"{signal.Signal.Instrument} {signal.Signal.Side} {signal.Signal.Confidence}");
    }
    if (ev is InfoEvent info)
    {
        Console.WriteLine($"{info.Stage}: {info.Message}");
    }
}
```

## Position Manager

```csharp
var manager = new PositionManager(
    client,
    PositionManagerConfig.ProductionDefaults() with
    {
        PositionSize = 0.10,
        MaxLeverage = 3.0
    });
manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata
{
    Venue = "okx",
    Instrument = "BTC-USDT-SWAP",
    SettlementCurrency = "USDT"
});

var orders = manager.HandleSignal(new Signal
{
    Venue = "okx",
    Instrument = "BTC-USDT-SWAP",
    Side = Side.Buy,
    Confidence = 0.82,
    TakeProfit = 0.012,
    StopLoss = 0.004,
    Price = 68000
});
```

The manager mirrors the production server sizing model: shared portfolio budget, confidence-weighted rebalance, reductions/closes/first-phase flips before openings or increases, live `AssetManager` available-exposure caps for openings, `MinOrderDelta` scaled by `PositionSize`, flip-safe opposite-side handling, fee-aware realized PnL, and leverage selected from confidence, fee-adjusted expected edge, and score.

`PositionManager` ignores replay signal events and ignores live signals whose venue/instrument pair has not been configured in its `InstrumentManager`. `RunAsync` uses an independent event stream, so multiple position managers can share one `SignalsClient`.

## Assets, Instruments, And Stats

Use `AssetManager` to update cash, available balance, used margin, and equity. Use `InstrumentManager` to update settlement currency, lot size, minimum size, tick size, and exchange max leverage. Orders include concrete quantity, notional, settlement currency, and fee-value estimates.

Call `Stats()` for realized and unrealized PnL in account value and percent, grouped by instrument and settlement currency.

## Development

```sh
dotnet build
dotnet test tests/Grexie.Signals.Client.Tests
dotnet pack -c Release
```
