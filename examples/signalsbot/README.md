# .NET Signalsbot Example

Paper-trading command line bot for Grexie Signals. It subscribes to `SIGNALS_INSTRUMENTS`, reads OKX candle prices, feeds the .NET client `PositionManager`, and persists manager state, orders, and snapshots in a local JSON file database.

## Run

```sh
cd examples/signalsbot
cp .env.example .env
$EDITOR .env
dotnet run -- papertrader
```

Clean the local database with:

```sh
dotnet run -- clean
```

The bot logs position opens, closes with PnL, margin adds/removals, and detailed order sizing. Every five minutes it reports position-manager stats and current PnL.

## Docker

```sh
cd examples/signalsbot
cp .env.example .env
docker compose up --build
```
