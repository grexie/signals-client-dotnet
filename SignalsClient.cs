using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Grexie.Signals.Client;

/// <summary>Authenticates a websocket connection to Grexie Signals.</summary>
public readonly record struct SignalsWebSocketToken(string Value);

/// <summary>Typed websocket client for Grexie Signals subscriptions.</summary>
public sealed class SignalsClient : IAsyncDisposable
{
    private readonly Uri _uri;
    private readonly SignalsWebSocketToken _token;
    private readonly ClientWebSocket _socket = new();
    private readonly JsonSerializerOptions _json = JsonOptions.Create();

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
    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        return _socket.ConnectAsync(_uri, cancellationToken);
    }

    /// <summary>Subscribe to one venue/instrument pair.</summary>
    public Task SubscribeAsync(string venue, string instrument, CancellationToken cancellationToken = default)
    {
        return SendAsync(new { type = "subscribe", venue, instrument }, cancellationToken);
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

    /// <summary>Close the websocket connection.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
        {
            await _socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "closing", CancellationToken.None).ConfigureAwait(false);
        }
        _socket.Dispose();
    }

    private Task SendAsync<T>(T payload, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(payload, _json);
        return _socket.SendAsync(Encoding.UTF8.GetBytes(json), WebSocketMessageType.Text, true, cancellationToken);
    }
}

/// <summary>Protocol event parser.</summary>
public static class SignalsEventParser
{
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
            "signal" => ParseSignalEvent(root),
            "error" => new ErrorEvent(root.GetStringOrNull("code"), root.GetStringOrNull("message")),
            _ => throw new InvalidOperationException($"unsupported websocket event type {type}")
        };
    }

    private static SignalEvent ParseSignalEvent(JsonElement root)
    {
        var signal = root.TryGetProperty("signal", out var signalElement)
            ? signalElement.Deserialize<Signal>(JsonOptions.Create()) ?? new Signal()
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
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
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

    public static DateTimeOffset? GetDateTimeOffsetOrNull(this JsonElement element, string property)
    {
        return element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String && value.TryGetDateTimeOffset(out var timestamp) ? timestamp : null;
    }
}
