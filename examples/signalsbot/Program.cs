using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Grexie.Signals.Client;

internal static class Program
{
    private const string DefaultSignalsWsUrl = "wss://signals.grexie.com/ws";
    private const string DefaultOkxBaseUrl = "https://www.okx.com";
    private const string DefaultOkxWsUrl = "wss://ws.okx.com:8443";
    private const string DefaultDbPath = "./data/signalsbot.json";
    private const double DefaultEquity = 10000;

    public static async Task<int> Main(string[] args)
    {
        LoadDotEnv(".env");
        var command = args.FirstOrDefault() ?? "papertrader";
        if (command == "clean")
        {
            var path = Env("SIGNALS_DB_PATH", DefaultDbPath);
            if (File.Exists(path)) File.Delete(path);
            Console.WriteLine($"Cleaned signalsbot local database path={path}");
            return 0;
        }
        if (command != "papertrader")
        {
            Console.Error.WriteLine("usage: signalsbot [papertrader|clean]");
            return 2;
        }

        try
        {
            await RunPaperTrader();
            return 0;
        }
        catch (OperationCanceledException)
        {
            return 0;
        }
    }

    private static async Task RunPaperTrader()
    {
        var cfg = Config.Load();
        var store = Store.Load(cfg.DbPath);
        var initialState = store.State.ToRuntime();

        var manager = new PositionManager(null, PositionManagerConfig.ProductionDefaults() with
        {
            InitialState = initialState,
            Persist = state =>
            {
                store.State = StoredState.From(state);
                store.Save();
            }
        });
        var bot = new Bot(manager, store, cfg.InitialEquity)
        {
            ClosedRealized = initialState.ClosedTrades.Sum(trade => trade.RealizedPnL),
            LastClosedCount = initialState.ClosedTrades.Count
        };
        bot.SyncAsset();

        foreach (var instrument in cfg.Instruments)
        {
            var metadata = await FetchOkxInstrument(cfg.OkxBaseUrl, instrument);
            manager.InstrumentManager.UpdateInstrument(metadata);
            if (await FetchLatestCandle(cfg.OkxBaseUrl, cfg.CandleBar, instrument) is { } tick)
            {
                bot.LatestPriceByKey[Key("okx", instrument)] = tick;
                bot.HandleOrders(manager.UpdatePrice("okx", instrument, tick.Price));
            }
            Console.WriteLine($"Loaded OKX instrument instrument={metadata.Instrument} settlement={metadata.SettlementCurrency} lot={Fmt(metadata.LotSize)} min={Fmt(metadata.MinSize)} tick={Fmt(metadata.TickSize)} contract={Fmt(metadata.ContractValue)}");
        }

        if (initialState.Positions.Count > 0 || initialState.ClosedTrades.Count > 0)
        {
            Console.WriteLine($"Hydrated position manager state open_positions={initialState.Positions.Count} closed_trades={initialState.ClosedTrades.Count}");
        }

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cts.Cancel();
        };

        var ticks = Channel.CreateUnbounded<PriceTick>();
        _ = Task.Run(() => SubscribeOkxCandles(cfg, ticks.Writer, cts.Token), cts.Token);
        _ = Task.Run(async () =>
        {
            await foreach (var tick in ticks.Reader.ReadAllAsync(cts.Token))
            {
                bot.LatestPriceByKey[Key("okx", tick.Instrument)] = tick;
                bot.HandleOrders(manager.UpdatePrice("okx", tick.Instrument, tick.Price));
            }
        }, cts.Token);
        _ = Task.Run(async () =>
        {
            using var timer = new PeriodicTimer(cfg.StatsInterval);
            while (await timer.WaitForNextTickAsync(cts.Token)) bot.ReportStats();
        }, cts.Token);

        await using var client = new SignalsClient(new SignalsWebSocketToken(cfg.Token), new Uri(cfg.WebsocketUrl));
        await client.ConnectAsync(cts.Token);
        foreach (var instrument in cfg.Instruments)
        {
            await client.SubscribeAsync("okx", instrument, cts.Token);
            Console.WriteLine($"Subscribed to Grexie Signals venue=okx instrument={instrument}");
        }
        Console.WriteLine($"signalsbot running instruments={string.Join(",", cfg.Instruments)} db={cfg.DbPath} ws={cfg.WebsocketUrl}");

        await foreach (var ev in client.EventsAsync(cts.Token))
        {
            bot.HandleSignalEvent(ev);
        }
    }

    private sealed class Bot(PositionManager manager, Store store, double initialEquity)
    {
        public double ClosedRealized { get; set; }
        public int LastClosedCount { get; set; }
        public Dictionary<string, PriceTick> LatestPriceByKey { get; } = new();

        public void HandleSignalEvent(SignalsEvent ev)
        {
            switch (ev)
            {
                case ReadyEvent ready:
                    Console.WriteLine($"Signals websocket ready message=\"{ready.Message}\"");
                    return;
                case InfoEvent info:
                    Console.WriteLine($"Instrument info instrument={info.Instrument} stage={info.Stage} replay={info.Replay} message=\"{info.Message}\"");
                    return;
                case ErrorEvent error:
                    Console.WriteLine($"Signals websocket error code={error.Code ?? ""} message=\"{error.Message ?? ""}\"");
                    return;
                case SubscribedEvent sub:
                    Console.WriteLine($"Subscription confirmed subscription={sub.SubscriptionId} instrument={sub.Instrument}");
                    return;
                case UnsubscribedEvent unsub:
                    Console.WriteLine($"Subscription removed subscription={unsub.SubscriptionId ?? 0} instrument={unsub.Instrument ?? ""} code={unsub.Code ?? ""} message=\"{unsub.Message ?? ""}\"");
                    return;
                case SignalEvent signalEvent:
                    if (signalEvent.Signal.Price <= 0 && LatestPriceByKey.TryGetValue(Key(signalEvent.Venue, signalEvent.Instrument), out var tick))
                    {
                        signalEvent.Signal.Price = tick.Price;
                        signalEvent.Signal.Timestamp ??= tick.Timestamp;
                    }
                    if (signalEvent.Signal.Price <= 0)
                    {
                        Console.WriteLine($"Signal skipped instrument={signalEvent.Instrument} side={signalEvent.Signal.Side} confidence={Fmt(signalEvent.Signal.Confidence)} reason=no OKX candle price yet");
                        return;
                    }
                    var orders = manager.HandleEvent(signalEvent);
                    Console.WriteLine($"Signal received instrument={signalEvent.Signal.Instrument} side={signalEvent.Signal.Side} confidence={Fmt(signalEvent.Signal.Confidence)} price={Fmt(signalEvent.Signal.Price)} replay={signalEvent.Replay} orders={orders.Count}");
                    HandleOrders(orders);
                    return;
            }
        }

        public void HandleOrders(IReadOnlyList<Order> orders)
        {
            if (orders.Count == 0) return;
            foreach (var order in orders) LogOrder(order);
            var trades = manager.ClosedTrades();
            if (LastClosedCount < trades.Count)
            {
                foreach (var trade in trades.Skip(LastClosedCount))
                {
                    ClosedRealized += trade.RealizedPnL;
                    LogClosedTrade(trade, initialEquity);
                }
                LastClosedCount = trades.Count;
            }
            SyncAsset();
            store.Orders.AddRange(orders.Select(StoredOrder.From));
            store.Snapshots.Add(Snapshot());
            store.Save();
        }

        public void SyncAsset()
        {
            var openRealized = manager.Positions().Sum(position => position.RealizedPnL);
            var equity = Math.Max(initialEquity + ClosedRealized + openRealized, 1);
            manager.AssetManager.UpdateAsset(new AssetSnapshot { Currency = "USDT", Cash = equity, Available = equity, Equity = equity });
        }

        public void ReportStats()
        {
            var snapshot = Snapshot();
            var positions = manager.Positions();
            Console.WriteLine($"Position manager stats equity={Money(snapshot.Equity)} realized={Money(snapshot.RealizedPnL)} unrealized={Money(snapshot.UnrealizedPnL)} total={Money(snapshot.TotalPnL)} fees={Money(snapshot.Fees)} open_positions={positions.Count}");
            foreach (var position in positions)
            {
                Console.WriteLine($"Open position instrument={position.Instrument} side={position.Side} size={Fmt(position.Size)} entry={Fmt(position.EntryPrice)} last={Fmt(position.LastPrice)} unrealized={Money(position.UnrealizedPnL)} pnl={Percent(Ratio(position.UnrealizedPnL, snapshot.Equity))} confidence={Fmt(position.Confidence)} tp={Fmt(position.TakeProfit)} sl={Fmt(position.StopLoss)}");
            }
            store.Snapshots.Add(snapshot);
            store.Save();
        }

        private PnlSnapshot Snapshot()
        {
            var stats = manager.Stats();
            var realized = ClosedRealized + stats.RealizedPnL;
            var unrealized = stats.UnrealizedPnL;
            return new PnlSnapshot(DateTimeOffset.UtcNow, initialEquity + realized, realized, unrealized, realized + unrealized, stats.Fees, Ratio(realized, initialEquity), Ratio(unrealized, initialEquity), Ratio(realized + unrealized, initialEquity));
        }
    }

    private sealed record Config(string Token, string WebsocketUrl, string[] Instruments, string DbPath, double InitialEquity, TimeSpan StatsInterval, string OkxBaseUrl, string OkxWsUrl, string CandleBar)
    {
        public static Config Load()
        {
            var token = Env("SIGNALS_WEBSOCKET_TOKEN", "");
            if (string.IsNullOrWhiteSpace(token)) throw new InvalidOperationException("SIGNALS_WEBSOCKET_TOKEN is required");
            return new Config(
                token,
                Env("SIGNALS_WEBSOCKET_URL", DefaultSignalsWsUrl),
                Env("SIGNALS_INSTRUMENTS", "DOGE-USDT-SWAP").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(x => x.ToUpperInvariant()).ToArray(),
                Env("SIGNALS_DB_PATH", DefaultDbPath),
                double.TryParse(Env("SIGNALS_INITIAL_EQUITY", ""), out var equity) && equity > 0 ? equity : DefaultEquity,
                ParseDuration(Env("SIGNALS_STATS_INTERVAL", "5m")),
                Env("SIGNALS_OKX_BASE_URL", DefaultOkxBaseUrl).TrimEnd('/'),
                Env("SIGNALS_OKX_WEBSOCKET_URL", DefaultOkxWsUrl).TrimEnd('/'),
                Env("SIGNALS_OKX_CANDLE_BAR", "1m"));
        }
    }

    private sealed class Store
    {
        private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true, Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) } };
        [JsonIgnore]
        public string Path { get; set; } = "";
        public StoredState State { get; set; } = new();
        public List<StoredOrder> Orders { get; set; } = new();
        public List<PnlSnapshot> Snapshots { get; set; } = new();

        public Store() { }
        private Store(string path) => Path = path;
        public static Store Load(string path)
        {
            if (!File.Exists(path)) return new Store(path);
            if (JsonSerializer.Deserialize<Store>(File.ReadAllText(path), Json) is not { } store) return new Store(path);
            store.Path = path;
            return store;
        }
        public void Save()
        {
            Directory.CreateDirectory(System.IO.Path.GetDirectoryName(Path) ?? ".");
            if (Orders.Count > 1000) Orders = Orders.Skip(Orders.Count - 1000).ToList();
            if (Snapshots.Count > 2880) Snapshots = Snapshots.Skip(Snapshots.Count - 2880).ToList();
            var tmp = Path + ".tmp";
            File.WriteAllText(tmp, JsonSerializer.Serialize(this, Json));
            File.Move(tmp, Path, true);
        }
    }

    private sealed record StoredState(List<Position> Positions, List<ClosedTrade> ClosedTrades)
    {
        public StoredState() : this(new(), new()) { }
        public static StoredState From(PositionManagerState state) => new(state.Positions.ToList(), state.ClosedTrades.ToList());
        public PositionManagerState ToRuntime() => new(Positions, ClosedTrades);
    }

    private sealed record StoredOrder(string Instrument, Side Side, string Reason, double SizeDelta, double PreviousSize, double TargetSize, double Price, double Margin, double Notional, double EstimatedFeeValue, double Confidence)
    {
        public static StoredOrder From(Order order) => new(order.Instrument, order.Side, order.Reason, order.SizeDelta, order.PreviousSize, order.TargetSize, order.Price, order.Margin, order.Notional, order.EstimatedFeeValue, order.Confidence);
    }

    private sealed record PnlSnapshot(DateTimeOffset Timestamp, double Equity, double RealizedPnL, double UnrealizedPnL, double TotalPnL, double Fees, double RealizedPct, double UnrealizedPct, double TotalPct);
    private sealed record PriceTick(string Instrument, double Price, DateTimeOffset Timestamp);

    private static async Task<InstrumentMetadata> FetchOkxInstrument(string baseUrl, string instrument)
    {
        using var http = new HttpClient();
        using var doc = await JsonDocument.ParseAsync(await http.GetStreamAsync($"{baseUrl}/api/v5/public/instruments?instType=SWAP&instId={Uri.EscapeDataString(instrument)}"));
        var row = doc.RootElement.GetProperty("data")[0];
        return new InstrumentMetadata
        {
            Venue = "okx",
            Instrument = row.GetProperty("instId").GetString() ?? instrument,
            SettlementCurrency = row.GetProperty("settleCcy").GetString() ?? "USDT",
            LotSize = Number(row, "lotSz"),
            MinSize = Number(row, "minSz"),
            TickSize = Number(row, "tickSz"),
            ContractValue = Number(row, "ctVal"),
            ContractMultiplier = Math.Max(Number(row, "ctMult"), 1),
            MaxLeverage = 1
        };
    }

    private static async Task<PriceTick?> FetchLatestCandle(string baseUrl, string bar, string instrument)
    {
        using var http = new HttpClient();
        using var doc = await JsonDocument.ParseAsync(await http.GetStreamAsync($"{baseUrl}/api/v5/market/candles?instId={Uri.EscapeDataString(instrument)}&bar={Uri.EscapeDataString(bar)}&limit=1"));
        return TickFromCandle(instrument, doc.RootElement.GetProperty("data")[0]);
    }

    private static async Task SubscribeOkxCandles(Config cfg, ChannelWriter<PriceTick> writer, CancellationToken ct)
    {
        var channel = "candle" + cfg.CandleBar;
        var delay = TimeSpan.FromSeconds(1);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var ws = new ClientWebSocket();
                await ws.ConnectAsync(new Uri(cfg.OkxWsUrl + "/ws/v5/business"), ct);
                var sub = JsonSerializer.Serialize(new { op = "subscribe", args = cfg.Instruments.Select(i => new { channel, instId = i }) });
                await ws.SendAsync(Encoding.UTF8.GetBytes(sub), WebSocketMessageType.Text, true, ct);
                Console.WriteLine($"Connected OKX candle websocket channel={channel} instruments={string.Join(",", cfg.Instruments)}");
                var buffer = new byte[64 * 1024];
                while (ws.State == WebSocketState.Open && !ct.IsCancellationRequested)
                {
                    var result = await ws.ReceiveAsync(buffer, ct);
                    var text = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    if (text == "ping") { await ws.SendAsync(Encoding.UTF8.GetBytes("pong"), WebSocketMessageType.Text, true, ct); continue; }
                    using var doc = JsonDocument.Parse(text);
                    if (!doc.RootElement.TryGetProperty("data", out var data)) continue;
                    var instrument = doc.RootElement.GetProperty("arg").GetProperty("instId").GetString() ?? "";
                    foreach (var row in data.EnumerateArray())
                    {
                        if (TickFromCandle(instrument, row) is { } tick) await writer.WriteAsync(tick, ct);
                    }
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                Console.WriteLine($"okx candle websocket: {ex.Message}");
                await Task.Delay(delay, ct);
                delay = TimeSpan.FromSeconds(Math.Min(delay.TotalSeconds * 2, 60));
            }
        }
    }

    private static PriceTick? TickFromCandle(string instrument, JsonElement row)
    {
        if (row.GetArrayLength() < 5) return null;
        var price = double.Parse(row[4].GetString() ?? "0");
        var ts = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(row[0].GetString() ?? "0"));
        return price > 0 ? new PriceTick(instrument, price, ts) : null;
    }

    private static void LogOrder(Order order)
    {
        var action = Math.Abs(order.PreviousSize) <= 1e-9 && Math.Abs(order.TargetSize) > 1e-9 ? "Position Opened" :
            SameSign(order.PreviousSize, order.TargetSize) && Math.Abs(order.TargetSize) > Math.Abs(order.PreviousSize) ? "Added margin to position" :
            SameSign(order.PreviousSize, order.TargetSize) && Math.Abs(order.TargetSize) < Math.Abs(order.PreviousSize) ? "Removed margin from position" :
            Math.Abs(order.TargetSize) <= 1e-9 && Math.Abs(order.PreviousSize) > 1e-9 ? "Position close order" :
            !SameSign(order.PreviousSize, order.TargetSize) ? "Position flip reduction" : "Order";
        Console.WriteLine($"{action} instrument={order.Instrument} side={order.Side} reason={order.Reason} delta={Fmt(order.SizeDelta)} previous={Fmt(order.PreviousSize)} target={Fmt(order.TargetSize)} price={Fmt(order.Price)} margin={Money(order.Margin)} notional={Money(order.Notional)} fee={Money(order.EstimatedFeeValue)} leverage={Fmt(order.Leverage)} confidence={Fmt(order.Confidence)} expected_edge={Fmt(order.ExpectedEdge)} tp={Fmt(order.TakeProfit)} sl={Fmt(order.StopLoss)} reduce_only={order.ReduceOnly}");
    }

    private static void LogClosedTrade(ClosedTrade trade, double initialEquity)
    {
        Console.WriteLine($"Position Closed instrument={trade.Instrument} side={trade.Side} reason={trade.ExitReason} pnl={Percent(Ratio(trade.RealizedPnL, initialEquity))} realized={Money(trade.RealizedPnL)} gross={Money(trade.RealizedGross)} fees={Money(trade.Fees)} entry={Fmt(trade.EntryPrice)} exit={Fmt(trade.ExitPrice)} size={Fmt(trade.Size)} move={Percent(trade.ExitMove)} mfe={Percent(trade.MFE)} mae={Percent(trade.MAE)} closed_at={trade.ClosedAt:O}");
    }

    private static void LoadDotEnv(string path)
    {
        if (!File.Exists(path)) return;
        foreach (var line in File.ReadAllLines(path).Select(x => x.Trim()))
        {
            if (line.Length == 0 || line.StartsWith('#') || !line.Contains('=')) continue;
            var parts = line.Split('=', 2);
            if (Environment.GetEnvironmentVariable(parts[0].Trim()) is null)
                Environment.SetEnvironmentVariable(parts[0].Trim(), parts[1].Trim().Trim('"', '\''));
        }
    }

    private static string Env(string key, string fallback) => string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(key)) ? fallback : Environment.GetEnvironmentVariable(key)!.Trim();
    private static double Number(JsonElement row, string name) => row.TryGetProperty(name, out var value) && double.TryParse(value.GetString(), out var number) ? number : 0;
    private static TimeSpan ParseDuration(string value) => value.EndsWith("m") ? TimeSpan.FromMinutes(double.Parse(value[..^1])) : value.EndsWith("h") ? TimeSpan.FromHours(double.Parse(value[..^1])) : value.EndsWith("s") ? TimeSpan.FromSeconds(double.Parse(value[..^1])) : TimeSpan.FromMilliseconds(double.Parse(value));
    private static string Key(string venue, string instrument) => $"{venue.Trim().ToLowerInvariant()}:{instrument.Trim().ToUpperInvariant()}";
    private static bool SameSign(double a, double b) => Math.Abs(a) <= 1e-9 || Math.Abs(b) <= 1e-9 || (a < 0) == (b < 0);
    private static double Ratio(double value, double basis) => basis == 0 ? 0 : value / basis;
    private static string Money(double value) => $"{value:+0.00;-0.00} USDT";
    private static string Percent(double value) => $"{value * 100:+0.00;-0.00}%";
    private static string Fmt(double value) => value.ToString("0.00000000");
}
