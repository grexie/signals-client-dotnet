# Grexie Signals .NET Client

Typed .NET client for the Grexie Signals router websocket protocol.

## SignalsManager

`SignalsManager` owns one router basket subscription. It sends your asset and venue-position snapshots to the server, then exposes server-created router intents from the websocket. It does not calculate order management locally.

```csharp
await using var client = new SignalsClient(new SignalsWebSocketToken("ws_your_token"));
await client.ConnectAsync();

var manager = new SignalsManager(
    client,
    new SignalsManagerState(
        new[] { new AssetSnapshot { Venue = "okx", Currency = "USDT", Cash = 1000, Available = 1000, Equity = 1000 } },
        Array.Empty<Position>()),
    new SignalsManagerConfig
    {
        Venue = "okx",
        Instruments = new[] { "BTC-USDT-SWAP", "ETH-USDT-SWAP" },
        Risk = new RiskConfig { MaxMarginRatio = 1, MaxConcurrentPositions = 1, MinLeverage = 1, MaxLeverage = 1 }
    });

await manager.SubscribeAsync();
```

Client-to-server updates include `UpdateAssetAsync`, `UpdatePositionAsync`, `AddInstrumentAsync`, `RemoveInstrumentAsync`, `UpdateConfigAsync`, and `ScheduleWithdrawalAsync`. Server-created orders arrive as `CreateMarketOrderEvent` values on `manager.Intents`.

## Development

```sh
dotnet test
```
