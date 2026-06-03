using Grexie.Signals.Client;

LoadDotEnv(".env");

var token = Env("SIGNALS_WEBSOCKET_TOKEN", "");
var websocketUrl = Env("SIGNALS_WEBSOCKET_URL", "wss://signals.grexie.com/ws");
var instruments = Env("SIGNALS_INSTRUMENTS", "BTC-USDT-SWAP")
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
    .Select(static item => item.ToUpperInvariant())
    .ToArray();
var equity = double.TryParse(Env("SIGNALS_INITIAL_EQUITY", "1000"), out var parsed) ? parsed : 1000;

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cts.Cancel();
};

await using var client = new SignalsClient(new SignalsWebSocketToken(token), new Uri(websocketUrl));
await client.ConnectAsync(cts.Token);

var manager = new SignalsManager(
    client,
    new SignalsManagerState(
        new[] { new AssetSnapshot { Venue = "okx", Currency = "USDT", Cash = equity, Available = equity, Equity = equity } },
        Array.Empty<Position>()),
    new SignalsManagerConfig
    {
        Venue = "okx",
        Instruments = instruments,
        Risk = new RiskConfig { MaxMarginRatio = 1, MaxConcurrentPositions = 1, MinLeverage = 1, MaxLeverage = 1 }
    });

var runTask = manager.RunAsync(cts.Token);
Console.WriteLine($"signalsbot listening instruments={string.Join(",", instruments)} ws={websocketUrl}");

while (!cts.IsCancellationRequested)
{
    var read = await Task.WhenAny(
        manager.Intents.Reader.ReadAsync(cts.Token).AsTask().ContinueWith(task => (SignalsEvent)task.Result, cts.Token),
        manager.ProtectionUpdates.Reader.ReadAsync(cts.Token).AsTask().ContinueWith(task => (SignalsEvent)task.Result, cts.Token),
        manager.Withdrawals.Reader.ReadAsync(cts.Token).AsTask().ContinueWith(task => (SignalsEvent)task.Result, cts.Token),
        manager.Messages.Reader.ReadAsync(cts.Token).AsTask().ContinueWith(task => (SignalsEvent)task.Result, cts.Token));
    var ev = await read;
    switch (ev)
    {
        case CreateMarketOrderEvent intent:
            Console.WriteLine($"intent action={intent.Action ?? ""} reason={intent.Reason ?? ""} instrument={intent.Instrument} side={intent.Side} contracts={intent.ContractSize} reduce_only={intent.ReduceOnly}");
            break;
        case UpdateTPSLEvent tpsl:
            Console.WriteLine($"tpsl instrument={tpsl.Instrument} side={tpsl.Side} tp={tpsl.TakeProfitPrice} sl={tpsl.StopLossPrice}");
            break;
        case WithdrawEvent withdrawal:
            Console.WriteLine($"withdraw currency={withdrawal.Currency} amount={withdrawal.Amount}");
            break;
        case InfoEvent info:
            Console.WriteLine($"info level={info.Level} instrument={info.Instrument} stage={info.Stage} message=\"{info.Message}\"");
            break;
    }
}

await runTask;

static string Env(string name, string fallback) => Environment.GetEnvironmentVariable(name) ?? fallback;

static void LoadDotEnv(string path)
{
    if (!File.Exists(path)) return;
    foreach (var line in File.ReadAllLines(path))
    {
        var trimmed = line.Trim();
        if (trimmed.Length == 0 || trimmed.StartsWith('#')) continue;
        var index = trimmed.IndexOf('=');
        if (index <= 0) continue;
        Environment.SetEnvironmentVariable(trimmed[..index].Trim(), trimmed[(index + 1)..].Trim().Trim('"', '\''));
    }
}
