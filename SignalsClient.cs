using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;

namespace Grexie.Signals.Client;

/// <summary>Authenticates a websocket connection to Grexie Signals.</summary>
public readonly record struct SignalsWebSocketToken(string Value);

/// <summary>Source of typed Grexie Signals websocket events.</summary>
public interface ISignalsEventSource
{
    /// <summary>Yield typed websocket events until the stream closes or cancellation is requested.</summary>
    IAsyncEnumerable<SignalsEvent> EventsAsync(CancellationToken cancellationToken = default);
}

/// <summary>Transport that can open or reopen its websocket connection.</summary>
public interface ISignalsReconnectableClient
{
    /// <summary>Open or reopen the websocket connection.</summary>
    Task ConnectAsync(CancellationToken cancellationToken = default);
}

/// <summary>Typed websocket client for Grexie Signals subscriptions.</summary>
public sealed class SignalsClient : IAsyncDisposable, ISignalsManagerClient
{
    private readonly Uri _uri;
    private readonly SignalsWebSocketToken _token;
    private readonly JsonSerializerOptions _json = JsonOptions.Create();
    private ClientWebSocket _socket;
    private Channel<SignalsEvent> _receiveQueue = Channel.CreateUnbounded<SignalsEvent>();
    private readonly List<Channel<SignalsEvent>> _subscribers = new();
    private readonly object _subscriberLock = new();
    private CancellationTokenSource? _readLoopCts;
    private Task? _readLoopTask;
    private bool _streamsCompleted;
    private Exception? _completionError;

    /// <summary>Create a client using the production websocket endpoint.</summary>
    public SignalsClient(SignalsWebSocketToken token)
        : this(token, new Uri("wss://signals.grexie.com/ws"))
    {
    }

    /// <summary>Create a client using a complete websocket URI.</summary>
    /// <param name="token">Bearer token used for websocket authentication.</param>
    /// <param name="uri">Complete websocket URI.</param>
    public SignalsClient(SignalsWebSocketToken token, Uri uri)
    {
        _token = token;
        _uri = uri;
        _socket = CreateSocket();
    }

    /// <summary>Open the websocket connection.</summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (_socket.State == WebSocketState.Open)
        {
            return;
        }
        _readLoopCts?.Cancel();
        if (_readLoopTask is not null)
        {
            try
            {
                await _readLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }
        _readLoopCts?.Dispose();
        _socket.Dispose();
        _socket = CreateSocket();
        _receiveQueue = Channel.CreateUnbounded<SignalsEvent>();
        lock (_subscriberLock)
        {
            _subscribers.Clear();
        }
        await _socket.ConnectAsync(_uri, cancellationToken).ConfigureAwait(false);
        _streamsCompleted = false;
        _completionError = null;
        _readLoopCts = new CancellationTokenSource();
        _readLoopTask = Task.Run(() => ReadLoopAsync(_socket, _readLoopCts.Token));
    }

    /// <summary>Subscribe to one venue/instrument pair.</summary>
    /// <param name="venue">Venue code.</param>
    /// <param name="instrument">Instrument symbol.</param>
    /// <param name="cancellationToken">Cancellation token for the send.</param>
    public Task SubscribeAsync(string venue, string instrument, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "subscribe", venue, instrument }, cancellationToken);
    }

    /// <summary>Subscribe to one Bollinger-router basket.</summary>
    /// <param name="venue">Venue code.</param>
    /// <param name="instruments">Basket instruments.</param>
    /// <param name="risk">Initial router risk configuration.</param>
    /// <param name="profitWithdrawRatio">Top-level profit withdrawal ratio.</param>
    /// <param name="assets">Optional account snapshots.</param>
    /// <param name="positions">Optional position snapshots.</param>
    /// <param name="mode">Optional router mode.</param>
    /// <param name="cancellationToken">Cancellation token for the send.</param>
    public Task SubscribeBasketAsync(string venue, IReadOnlyList<string> instruments, RiskConfig? risk = null, double profitWithdrawRatio = 0, IReadOnlyList<AssetSnapshot>? assets = null, IReadOnlyList<Position>? positions = null, string? mode = null, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "subscribe", venue, instruments, mode, risk = NormalizeRisk(risk), profitWithdrawRatio, assets, positions }, cancellationToken);
    }

    /// <summary>Publish an account asset snapshot.</summary>
    /// <param name="subscriptionId">Server subscription id.</param>
    /// <param name="asset">Asset snapshot to publish.</param>
    /// <param name="cancellationToken">Cancellation token for the send.</param>
    public Task UpdateAssetAsync(long subscriptionId, AssetSnapshot asset, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "update-asset", subscriptionId, asset.Venue, asset.Currency, asset.Cash, asset.Available, asset.Used, asset.Equity, asset.MaxUsage }, cancellationToken);
    }

    /// <summary>Publish a venue position snapshot.</summary>
    /// <param name="subscriptionId">Server subscription id.</param>
    /// <param name="position">Position snapshot to publish.</param>
    /// <param name="cancellationToken">Cancellation token for the send.</param>
    public Task UpdatePositionAsync(long subscriptionId, Position position, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "update-position", subscriptionId, position.Venue, position.Instrument, side = position.Side?.ToString().ToLowerInvariant(), position.Status, size = Math.Abs(position.Size), position.EntryPrice, markPrice = position.LastPrice, position.Margin, position.Leverage, position.TakeProfitPrice, position.StopLossPrice }, cancellationToken);
    }

    /// <summary>Add an instrument to a live basket subscription.</summary>
    /// <param name="subscriptionId">Server subscription id.</param>
    /// <param name="instrument">Instrument symbol to add.</param>
    /// <param name="cancellationToken">Cancellation token for the send.</param>
    public Task AddInstrumentAsync(long subscriptionId, string instrument, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "add-instrument", subscriptionId, instrument }, cancellationToken);
    }

    /// <summary>Remove an instrument from a live basket subscription.</summary>
    /// <param name="subscriptionId">Server subscription id.</param>
    /// <param name="instrument">Instrument symbol to remove.</param>
    /// <param name="cancellationToken">Cancellation token for the send.</param>
    public Task RemoveInstrumentAsync(long subscriptionId, string instrument, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "remove-instrument", subscriptionId, instrument }, cancellationToken);
    }

    /// <summary>Send a runtime router config patch.</summary>
    /// <param name="subscriptionId">Server subscription id.</param>
    /// <param name="config">Runtime config patch to send.</param>
    /// <param name="cancellationToken">Cancellation token for the send.</param>
    public Task UpdateConfigAsync(long subscriptionId, RuntimeConfig config, CancellationToken cancellationToken = default)
    {
        var next = NormalizeRuntime(config);
        return SendAsync(new { type = "update-config", subscriptionId, next.MaxMarginRatio, next.MinLotHaircutRatio, next.MaxConcurrentPositions, next.MaxDrawdown, next.SwitchBuffer, next.MinLeverage, next.MaxLeverage, next.ProfitWithdrawRatio }, cancellationToken);
    }

    /// <summary>Schedule a withdrawal request for a router subscription.</summary>
    /// <param name="subscriptionId">Server subscription id.</param>
    /// <param name="currency">Settlement currency to withdraw.</param>
    /// <param name="amount">Currency amount to withdraw.</param>
    /// <param name="venue">Optional venue override.</param>
    /// <param name="reason">Optional user-facing reason.</param>
    /// <param name="cancellationToken">Cancellation token for the send.</param>
    public Task ScheduleWithdrawalAsync(long subscriptionId, string currency, double amount, string? venue = null, string? reason = null, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "schedule-withdrawal", subscriptionId, venue, currency, amount, reason }, cancellationToken);
    }

    /// <summary>Unsubscribe by server subscription id.</summary>
    public Task UnsubscribeAsync(long subscriptionId, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "unsubscribe", subscriptionId }, cancellationToken);
    }

    /// <summary>Unsubscribe by venue/instrument pair.</summary>
    public Task UnsubscribeInstrumentAsync(string venue, string instrument, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "unsubscribe", venue, instrument }, cancellationToken);
    }

    /// <summary>Receive the next typed websocket event.</summary>
    public async Task<SignalsEvent?> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _receiveQueue.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            return null;
        }
    }

    /// <summary>Yield an independent stream of typed websocket events.</summary>
    public async IAsyncEnumerable<SignalsEvent> EventsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateUnbounded<SignalsEvent>();
        var added = false;
        lock (_subscriberLock)
        {
            if (_streamsCompleted)
            {
                channel.Writer.TryComplete(_completionError);
            }
            else
            {
                _subscribers.Add(channel);
                added = true;
            }
        }
        try
        {
            await foreach (var ev in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return ev;
            }
        }
        finally
        {
            lock (_subscriberLock)
            {
                if (added)
                {
                    _subscribers.Remove(channel);
                }
            }
            channel.Writer.TryComplete();
        }
    }

    /// <summary>Close the websocket connection.</summary>
    public async ValueTask DisposeAsync()
    {
        _readLoopCts?.Cancel();
        var socket = _socket;
        if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
        {
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "closing", CancellationToken.None).ConfigureAwait(false);
        }
        if (_readLoopTask is not null)
        {
            try
            {
                await _readLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }
        CompleteStreams();
        _readLoopCts?.Dispose();
        socket.Dispose();
    }

    private Task SendAsync<T>(T payload, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(payload, _json);
        return _socket.SendAsync(Encoding.UTF8.GetBytes(json), WebSocketMessageType.Text, true, cancellationToken);
    }

    private static RiskConfig NormalizeRisk(RiskConfig? risk)
    {
        risk ??= new RiskConfig();
        var maxLeverage = Math.Max(0, double.IsFinite(risk.MaxLeverage) ? risk.MaxLeverage : 0);
        var minLeverage = Math.Max(0, double.IsFinite(risk.MinLeverage) ? risk.MinLeverage : 0);
        if (maxLeverage > 0 && minLeverage > maxLeverage) minLeverage = maxLeverage;
        return risk with
        {
            MaxMarginRatio = Clamp01(risk.MaxMarginRatio > 0 ? risk.MaxMarginRatio : 1),
            MinLotHaircutRatio = Math.Max(0, double.IsFinite(risk.MinLotHaircutRatio) ? risk.MinLotHaircutRatio : 0),
            MaxConcurrentPositions = Math.Max(0, risk.MaxConcurrentPositions),
            MaxDrawdown = Math.Max(0, double.IsFinite(risk.MaxDrawdown) ? risk.MaxDrawdown : 0),
            SwitchBuffer = Math.Max(0, double.IsFinite(risk.SwitchBuffer) ? risk.SwitchBuffer : 0),
            MinLeverage = minLeverage,
            MaxLeverage = maxLeverage,
            ProfitWithdrawRatio = Clamp01(risk.ProfitWithdrawRatio)
        };
    }

    private static RuntimeConfig NormalizeRuntime(RuntimeConfig config)
    {
        var maxLeverage = Math.Max(0, double.IsFinite(config.MaxLeverage) ? config.MaxLeverage : 0);
        var minLeverage = Math.Max(0, double.IsFinite(config.MinLeverage) ? config.MinLeverage : 0);
        if (maxLeverage > 0 && minLeverage > maxLeverage) minLeverage = maxLeverage;
        return config with
        {
            MaxMarginRatio = Clamp01(config.MaxMarginRatio),
            MinLotHaircutRatio = Math.Max(0, double.IsFinite(config.MinLotHaircutRatio) ? config.MinLotHaircutRatio : 0),
            MaxConcurrentPositions = Math.Max(0, config.MaxConcurrentPositions),
            MaxDrawdown = Math.Max(0, double.IsFinite(config.MaxDrawdown) ? config.MaxDrawdown : 0),
            SwitchBuffer = Math.Max(0, double.IsFinite(config.SwitchBuffer) ? config.SwitchBuffer : 0),
            MinLeverage = minLeverage,
            MaxLeverage = maxLeverage,
            ProfitWithdrawRatio = Clamp01(config.ProfitWithdrawRatio)
        };
    }

    private static double Clamp01(double value) => double.IsFinite(value) ? Math.Clamp(value, 0, 1) : 0;

    private ClientWebSocket CreateSocket()
    {
        var socket = new ClientWebSocket();
        if (!string.IsNullOrWhiteSpace(_token.Value))
        {
            socket.Options.SetRequestHeader("Authorization", $"Bearer {_token.Value}");
        }
        return socket;
    }

    private async Task ReadLoopAsync(ClientWebSocket socket, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var ev = await ReceiveFromSocketAsync(socket, cancellationToken).ConfigureAwait(false);
                if (ev is null) break;
                Publish(ev);
            }
            CompleteStreams();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            CompleteStreams();
        }
        catch (Exception ex)
        {
            CompleteStreams(ex);
        }
    }

    private async Task<SignalsEvent?> ReceiveFromSocketAsync(ClientWebSocket socket, CancellationToken cancellationToken)
    {
        while (true)
        {
            var buffer = new ArraySegment<byte>(new byte[64 * 1024]);
            using var stream = new MemoryStream();
            WebSocketReceiveResult result;
            do
            {
                result = await socket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    return null;
                }
                stream.Write(buffer.Array!, buffer.Offset, result.Count);
            }
            while (!result.EndOfMessage);
            var raw = Encoding.UTF8.GetString(stream.ToArray());
            if (SignalsEventParser.IsIgnored(raw))
            {
                continue;
            }
            return SignalsEventParser.Parse(raw);
        }
    }

    private void Publish(SignalsEvent ev)
    {
        _receiveQueue.Writer.TryWrite(ev);
        lock (_subscriberLock)
        {
            foreach (var subscriber in _subscribers.ToArray())
            {
                subscriber.Writer.TryWrite(ev);
            }
        }
    }

    private void CompleteStreams(Exception? error = null)
    {
        _streamsCompleted = true;
        _completionError = error;
        _receiveQueue.Writer.TryComplete(error);
        lock (_subscriberLock)
        {
            foreach (var subscriber in _subscribers.ToArray())
            {
                subscriber.Writer.TryComplete(error);
            }
            _subscribers.Clear();
        }
    }
}

/// <summary>Protocol event parser.</summary>
public static class SignalsEventParser
{
    private static readonly JsonSerializerOptions ParserOptions = JsonOptions.Create();

    /// <summary>Parse one websocket JSON message into a typed event.</summary>
    public static SignalsEvent Parse(string rawJson)
    {
        using var document = JsonDocument.Parse(rawJson);
        var root = document.RootElement;
        var type = root.GetString("type") ?? throw new InvalidOperationException("missing websocket event type");
        return type switch
        {
            "ready" => new ReadyEvent(root.GetString("message") ?? string.Empty),
            "subscribed" => new SubscribedEvent(root.GetInt64("subscriptionId"), root.GetString("venue") ?? string.Empty, root.GetString("instrument") ?? string.Empty),
            "unsubscribed" => new UnsubscribedEvent(root.GetNullableInt64("subscriptionId"), root.GetStringOrNull("venue"), root.GetStringOrNull("instrument"), root.GetStringOrNull("code"), root.GetStringOrNull("message")),
            "basket_updated" => new BasketUpdatedEvent(root.GetInt64("subscriptionId"), root.GetStringOrNull("venue"), root.GetStringOrNull("basketId"), root.GetStringOrNull("message")),
            "order_router_forwarded" => new OrderRouterForwardedEvent(root.GetInt64("subscriptionId"), root.GetStringOrNull("venue"), root.GetStringOrNull("basketId"), root.GetStringOrNull("message")),
            "info" => new InfoEvent(root.GetInt64("subscriptionId"), root.GetString("venue") ?? string.Empty, root.GetString("instrument") ?? string.Empty, NormalizeInfoLevel(root.GetStringOrNull("level")), root.GetStringOrNull("stage") ?? string.Empty, root.GetStringOrNull("message") ?? string.Empty, root.GetDateTimeOffsetOrNull("timestamp"), root.GetBoolOrDefault("replay"), root.GetDateTimeOffsetOrNull("replayedAt")),
            "backtest" => new BacktestEvent(root.GetInt64("subscriptionId"), root.GetString("venue") ?? string.Empty, root.GetString("instrument") ?? string.Empty, root.TryGetProperty("backtest", out var backtest) ? backtest.Clone() : JsonSerializer.Deserialize<JsonElement>("{}"), root.GetDateTimeOffsetOrNull("timestamp")),
            "signal" => ParseSignalEvent(root),
            "create-market-order" => new CreateMarketOrderEvent(root.GetInt64("subscriptionId"), root.GetStringOrNull("intentId"), root.GetStringOrNull("action"), root.GetStringOrNull("reason"), root.GetStringOrNull("venue"), root.GetStringOrNull("instrument") ?? string.Empty, root.GetStringOrNull("side") ?? string.Empty, root.GetStringOrNull("orderType"), root.GetDoubleOrDefault("contractSize"), root.GetDoubleOrDefault("leverage"), root.GetBoolOrDefault("reduceOnly"), root.GetDoubleOrDefault("takeProfitPrice"), root.GetDoubleOrDefault("stopLossPrice"), root.GetDoubleOrDefault("takeProfit"), root.GetDoubleOrDefault("stopLoss"), root.GetDateTimeOffsetOrNull("timestamp"), root.GetDoubleOrDefault("margin"), root.GetDoubleOrDefault("confidence")),
            "update-tpsl" => new UpdateTPSLEvent(root.GetInt64("subscriptionId"), root.GetStringOrNull("intentId"), root.GetStringOrNull("venue"), root.GetStringOrNull("instrument") ?? string.Empty, root.GetStringOrNull("side") ?? string.Empty, root.GetDoubleOrDefault("takeProfitPrice"), root.GetDoubleOrDefault("stopLossPrice"), root.GetDoubleOrDefault("takeProfit"), root.GetDoubleOrDefault("stopLoss"), root.GetDateTimeOffsetOrNull("timestamp")),
            "withdraw" => new WithdrawEvent(root.GetInt64("subscriptionId"), root.GetStringOrNull("intentId"), root.GetStringOrNull("venue"), root.GetStringOrNull("currency") ?? string.Empty, root.GetDoubleOrDefault("amount"), root.GetDateTimeOffsetOrNull("timestamp")),
            "error" => new ErrorEvent(root.GetStringOrNull("code"), root.GetStringOrNull("message")),
            _ => throw new InvalidOperationException($"unsupported websocket event type {type}")
        };
    }

    public static bool IsIgnored(string rawJson)
    {
        try
        {
            using var document = JsonDocument.Parse(rawJson);
            return document.RootElement.GetStringOrNull("type") == "basket_state";
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static string NormalizeInfoLevel(string? value)
    {
        return value?.Trim().ToLowerInvariant() switch
        {
            "error" => "error",
            "warn" => "warn",
            "debug" => "debug",
            _ => "info",
        };
    }

    private static SignalEvent ParseSignalEvent(JsonElement root)
    {
        var signal = root.TryGetProperty("signal", out var signalElement)
            ? signalElement.Deserialize<Signal>(ParserOptions) ?? new Signal()
            : new Signal();
        signal.Venue = string.IsNullOrWhiteSpace(signal.Venue) ? root.GetStringOrNull("venue") ?? string.Empty : signal.Venue;
        signal.Instrument = string.IsNullOrWhiteSpace(signal.Instrument) ? root.GetStringOrNull("instrument") ?? string.Empty : signal.Instrument;
        signal.Timestamp ??= root.GetDateTimeOffsetOrNull("timestamp");
        return new SignalEvent(root.GetInt64("subscriptionId"), root.GetStringOrNull("venue") ?? signal.Venue, root.GetStringOrNull("instrument") ?? signal.Instrument, signal, root.GetDateTimeOffsetOrNull("timestamp"), root.GetBoolOrDefault("replay"), root.GetDateTimeOffsetOrNull("replayedAt"));
    }
}

internal static class JsonOptions
{
    public static JsonSerializerOptions Create() => new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };
}

internal static class JsonElementExtensions
{
    public static string? GetStringOrNull(this JsonElement element, string property)
    {
        return element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;
    }

    public static string? GetString(this JsonElement element, string property) => element.GetStringOrNull(property);

    public static long GetInt64(this JsonElement element, string property)
    {
        return element.TryGetProperty(property, out var value) && value.TryGetInt64(out var number) ? number : 0;
    }

    public static long? GetNullableInt64(this JsonElement element, string property)
    {
        return element.TryGetProperty(property, out var value) && value.TryGetInt64(out var number) ? number : null;
    }

    public static bool GetBoolOrDefault(this JsonElement element, string property)
    {
        return element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.True;
    }

    public static double GetDoubleOrDefault(this JsonElement element, string property)
    {
        return element.TryGetProperty(property, out var value) && value.TryGetDouble(out var number) ? number : 0;
    }

    public static DateTimeOffset? GetDateTimeOffsetOrNull(this JsonElement element, string property)
    {
        return element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String && value.TryGetDateTimeOffset(out var timestamp) ? timestamp : null;
    }
}
