using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;

namespace Grexie.Signals.Client;

/// <summary>Authenticates a websocket connection to Grexie Signals.</summary>
public readonly record struct SignalsWebSocketToken(string Value);

/// <summary>Typed websocket client for Grexie Signals subscriptions.</summary>
public interface ISignalsEventSource
{
    IAsyncEnumerable<SignalsEvent> EventsAsync(CancellationToken cancellationToken = default);
}

public sealed class SignalsClient : IAsyncDisposable, ISignalsManagerClient
{
    private readonly Uri _uri;
    private readonly SignalsWebSocketToken _token;
    private readonly ClientWebSocket _socket = new();
    private readonly JsonSerializerOptions _json = JsonOptions.Create();
    private readonly Channel<SignalsEvent> _receiveQueue = Channel.CreateUnbounded<SignalsEvent>();
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
    public SignalsClient(SignalsWebSocketToken token, Uri uri)
    {
        _token = token;
        _uri = uri;
        if (!string.IsNullOrWhiteSpace(token.Value))
        {
            _socket.Options.SetRequestHeader("Authorization", $"Bearer {token.Value}");
        }
    }

    /// <summary>Open the websocket connection.</summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        await _socket.ConnectAsync(_uri, cancellationToken).ConfigureAwait(false);
        _streamsCompleted = false;
        _completionError = null;
        _readLoopCts = new CancellationTokenSource();
        _readLoopTask = Task.Run(() => ReadLoopAsync(_readLoopCts.Token));
    }

    /// <summary>Subscribe to one venue/instrument pair.</summary>
    public Task SubscribeAsync(string venue, string instrument, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "subscribe", venue, instrument }, cancellationToken);
    }

    /// <summary>Subscribe to one Bollinger-router basket.</summary>
    public Task SubscribeBasketAsync(string venue, IReadOnlyList<string> instruments, object? risk = null, double profitWithdrawRatio = 0, IReadOnlyList<AssetSnapshot>? assets = null, IReadOnlyList<Position>? positions = null, string? mode = null, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "subscribe", venue, instruments, mode, risk, profitWithdrawRatio, assets, positions }, cancellationToken);
    }

    public Task UpdateAssetAsync(long subscriptionId, AssetSnapshot asset, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "update-asset", subscriptionId, asset.Venue, asset.Currency, asset.Cash, asset.Available, asset.Used, asset.Equity, asset.MaxUsage }, cancellationToken);
    }

    public Task UpdatePositionAsync(long subscriptionId, Position position, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "update-position", subscriptionId, position.Venue, position.Instrument, side = position.Side?.ToString().ToLowerInvariant(), position.Status, size = Math.Abs(position.Size), position.EntryPrice, markPrice = position.LastPrice, position.Leverage, position.TakeProfitPrice, position.StopLossPrice }, cancellationToken);
    }

    public Task AddInstrumentAsync(long subscriptionId, string instrument, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "add-instrument", subscriptionId, instrument }, cancellationToken);
    }

    public Task RemoveInstrumentAsync(long subscriptionId, string instrument, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "remove-instrument", subscriptionId, instrument }, cancellationToken);
    }

    public Task UpdateConfigAsync(long subscriptionId, double profitWithdrawRatio, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "update-config", subscriptionId, profitWithdrawRatio }, cancellationToken);
    }

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
        if (_socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
        {
            await _socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "closing", CancellationToken.None).ConfigureAwait(false);
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
        _socket.Dispose();
    }

    private Task SendAsync<T>(T payload, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(payload, _json);
        return _socket.SendAsync(Encoding.UTF8.GetBytes(json), WebSocketMessageType.Text, true, cancellationToken);
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var ev = await ReceiveFromSocketAsync(cancellationToken).ConfigureAwait(false);
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

    private async Task<SignalsEvent?> ReceiveFromSocketAsync(CancellationToken cancellationToken)
    {
        var buffer = new ArraySegment<byte>(new byte[64 * 1024]);
        using var stream = new MemoryStream();
        WebSocketReceiveResult result;
        do
        {
            result = await _socket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                return null;
            }
            stream.Write(buffer.Array!, buffer.Offset, result.Count);
        }
        while (!result.EndOfMessage);
        return SignalsEventParser.Parse(Encoding.UTF8.GetString(stream.ToArray()));
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
            "info" => new InfoEvent(root.GetInt64("subscriptionId"), root.GetString("venue") ?? string.Empty, root.GetString("instrument") ?? string.Empty, root.GetStringOrNull("stage") ?? string.Empty, root.GetStringOrNull("message") ?? string.Empty, root.GetDateTimeOffsetOrNull("timestamp"), root.GetBoolOrDefault("replay"), root.GetDateTimeOffsetOrNull("replayedAt")),
            "backtest" => new BacktestEvent(root.GetInt64("subscriptionId"), root.GetString("venue") ?? string.Empty, root.GetString("instrument") ?? string.Empty, root.TryGetProperty("backtest", out var backtest) ? backtest.Clone() : JsonSerializer.Deserialize<JsonElement>("{}"), root.GetDateTimeOffsetOrNull("timestamp")),
            "signal" => ParseSignalEvent(root),
            "create-market-order" => new CreateMarketOrderEvent(root.GetInt64("subscriptionId"), root.GetStringOrNull("intentId"), root.GetStringOrNull("action"), root.GetStringOrNull("reason"), root.GetStringOrNull("venue"), root.GetStringOrNull("instrument") ?? string.Empty, root.GetStringOrNull("side") ?? string.Empty, root.GetStringOrNull("orderType"), root.GetDoubleOrDefault("contractSize"), root.GetDoubleOrDefault("leverage"), root.GetBoolOrDefault("reduceOnly"), root.GetDoubleOrDefault("takeProfitPrice"), root.GetDoubleOrDefault("stopLossPrice"), root.GetDoubleOrDefault("takeProfit"), root.GetDoubleOrDefault("stopLoss"), root.GetDateTimeOffsetOrNull("timestamp")),
            "update-tpsl" => new UpdateTPSLEvent(root.GetInt64("subscriptionId"), root.GetStringOrNull("intentId"), root.GetStringOrNull("venue"), root.GetStringOrNull("instrument") ?? string.Empty, root.GetStringOrNull("side") ?? string.Empty, root.GetDoubleOrDefault("takeProfitPrice"), root.GetDoubleOrDefault("stopLossPrice"), root.GetDoubleOrDefault("takeProfit"), root.GetDoubleOrDefault("stopLoss"), root.GetDateTimeOffsetOrNull("timestamp")),
            "withdraw" => new WithdrawEvent(root.GetInt64("subscriptionId"), root.GetStringOrNull("intentId"), root.GetStringOrNull("venue"), root.GetStringOrNull("currency") ?? string.Empty, root.GetDoubleOrDefault("amount"), root.GetDateTimeOffsetOrNull("timestamp")),
            "error" => new ErrorEvent(root.GetStringOrNull("code"), root.GetStringOrNull("message")),
            _ => throw new InvalidOperationException($"unsupported websocket event type {type}")
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
