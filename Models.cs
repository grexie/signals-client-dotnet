namespace Grexie.Signals.Client;

/// <summary>Signal or position direction.</summary>
public enum Side
{
    Buy,
    Sell
}

/// <summary>One timeframe contribution to an aggregate signal.</summary>
public sealed record SignalComponent(
    string Timeframe,
    Side Side,
    double Confidence,
    double Weight,
    double SignedScore,
    double TakeProfit,
    double StopLoss,
    IReadOnlyList<double>? Probability = null);

/// <summary>Public signal payload sent by the Grexie Signals websocket.</summary>
public sealed record Signal
{
    public string Venue { get; set; } = string.Empty;
    public string Instrument { get; set; } = string.Empty;
    public string? Timeframe { get; set; }
    public double Confidence { get; set; }
    public Side Side { get; set; }
    public double TakeProfit { get; set; }
    public double StopLoss { get; set; }
    public double Score { get; set; }
    public IReadOnlyList<SignalComponent> Components { get; set; } = Array.Empty<SignalComponent>();
    public DateTimeOffset? Timestamp { get; set; }
    public double Price { get; set; }
}

/// <summary>Base type for all websocket events.</summary>
public abstract record SignalsEvent(string Type);

public sealed record ReadyEvent(string Message) : SignalsEvent("ready");
public sealed record SubscribedEvent(long SubscriptionId, string Venue, string Instrument) : SignalsEvent("subscribed");
public sealed record UnsubscribedEvent(long? SubscriptionId, string? Venue, string? Instrument, string? Code, string? Message) : SignalsEvent("unsubscribed");
public sealed record InfoEvent(long SubscriptionId, string Venue, string Instrument, string Stage, string Message, DateTimeOffset? Timestamp, bool Replay, DateTimeOffset? ReplayedAt) : SignalsEvent("info");
public sealed record SignalEvent(long SubscriptionId, string Venue, string Instrument, Signal Signal, DateTimeOffset? Timestamp, bool Replay, DateTimeOffset? ReplayedAt) : SignalsEvent("signal");
public sealed record ErrorEvent(string? Code, string? Message) : SignalsEvent("error");

/// <summary>Per-instrument fee and leverage overrides.</summary>
public sealed record InstrumentConfig
{
    public double? MakerFeeRate { get; init; }
    public double? TakerFeeRate { get; init; }
    public double? MinLeverage { get; init; }
    public double? MaxLeverage { get; init; }
}

/// <summary>Account state for one settlement currency.</summary>
public sealed record AssetSnapshot
{
    public required string Currency { get; init; }
    public double Cash { get; init; }
    public double Available { get; init; }
    public double Used { get; init; }
    public double Equity { get; init; }
    public DateTimeOffset UpdatedAt { get; init; } = DateTimeOffset.UtcNow;
}

/// <summary>Tracks cash, available, used, and equity by settlement currency.</summary>
public sealed class AssetManager
{
    private readonly Dictionary<string, AssetSnapshot> _assets = new();

    public void UpdateAsset(AssetSnapshot snapshot)
    {
        if (!string.IsNullOrWhiteSpace(snapshot.Currency)) _assets[snapshot.Currency] = snapshot;
    }

    public AssetSnapshot? Asset(string currency) => _assets.GetValueOrDefault(currency);
    public IReadOnlyList<AssetSnapshot> Assets() => _assets.Values.OrderBy(asset => asset.Currency).ToArray();
}

/// <summary>Exchange constraints for one venue/instrument.</summary>
public sealed record InstrumentMetadata
{
    public required string Venue { get; init; }
    public required string Instrument { get; init; }
    public string SettlementCurrency { get; init; } = "USDT";
    public double LotSize { get; init; }
    public double MinSize { get; init; }
    public double TickSize { get; init; }
    public double MaxLeverage { get; init; }
}

/// <summary>Tracks lot size, min size, tick size, settlement currency, and max leverage.</summary>
public sealed class InstrumentManager
{
    private readonly Dictionary<string, InstrumentMetadata> _instruments = new();

    public void UpdateInstrument(InstrumentMetadata metadata)
    {
        if (!string.IsNullOrWhiteSpace(metadata.Venue) && !string.IsNullOrWhiteSpace(metadata.Instrument)) _instruments[$"{metadata.Venue}:{metadata.Instrument}"] = metadata;
    }

    public InstrumentMetadata Instrument(string venue, string instrument) => _instruments.GetValueOrDefault($"{venue}:{instrument}") ?? new InstrumentMetadata { Venue = venue, Instrument = instrument };
    public bool ContainsInstrument(string venue, string instrument) => _instruments.ContainsKey($"{venue}:{instrument}");
    public IReadOnlyList<InstrumentMetadata> Instruments() => _instruments.Values.OrderBy(i => i.Venue).ThenBy(i => i.Instrument).ToArray();
}

/// <summary>Fee-aware position manager configuration.</summary>
public sealed record PositionManagerConfig
{
    public double PositionSize { get; init; } = 1.0;
    public double MinExpectedEdge { get; init; } = 0.0045;
    public double MinOrderDelta { get; init; } = 0.20;
    public TimeSpan RebalanceInterval { get; init; } = TimeSpan.FromHours(6);
    public double MakerFeeRate { get; init; } = 0.0002;
    public double TakerFeeRate { get; init; } = 0.0005;
    public double MinLeverage { get; init; } = 1.0;
    public double MaxLeverage { get; init; } = 1.0;
    public IReadOnlyDictionary<string, InstrumentConfig> Instruments { get; init; } = new Dictionary<string, InstrumentConfig>();

    /// <summary>Server-compatible execution-policy defaults.</summary>
    public static PositionManagerConfig ProductionDefaults() => new();
}

/// <summary>In-memory position state.</summary>
public sealed record Position
{
    public string Venue { get; init; } = string.Empty;
    public string Instrument { get; init; } = string.Empty;
    public double Size { get; set; }
    public double Confidence { get; set; }
    public double EntryPrice { get; set; }
    public double LastPrice { get; set; }
    public double TakeProfit { get; set; }
    public double StopLoss { get; set; }
    public double Leverage { get; set; } = 1.0;
    public double RealizedGross { get; set; }
    public double Fees { get; set; }
    public double RealizedPnL { get; set; }
    public DateTimeOffset? OpenedAt { get; set; }
    public DateTimeOffset? LastSignalAt { get; set; }
    public Side? Side => Size < 0 ? Grexie.Signals.Client.Side.Sell : Size > 0 ? Grexie.Signals.Client.Side.Buy : null;
    public double UnrealizedPnL => PriceMove() * Math.Abs(Size);

    internal double PriceMove()
    {
        if (EntryPrice <= 0 || LastPrice <= 0) return 0;
        return Size < 0 ? (EntryPrice - LastPrice) / EntryPrice : (LastPrice - EntryPrice) / EntryPrice;
    }
}

/// <summary>Target order recommendation emitted by <see cref="PositionManager"/>.</summary>
public sealed record Order
{
    public required string Venue { get; init; }
    public required string Instrument { get; init; }
    public required Side Side { get; init; }
    public required string Reason { get; init; }
    public required double SizeDelta { get; init; }
    public required double PreviousSize { get; init; }
    public required double TargetSize { get; init; }
    public required double Price { get; init; }
    public required double Confidence { get; init; }
    public required double ExpectedEdge { get; init; }
    public required double FeeRate { get; set; }
    public required double EstimatedFee { get; set; }
    public double EstimatedFeeValue { get; set; }
    public double Quantity { get; init; }
    public double Notional { get; init; }
    public string SettlementCurrency { get; init; } = "USDT";
    public double MinSize { get; init; }
    public double LotSize { get; init; }
    public double TickSize { get; init; }
    public required double Leverage { get; init; }
    public double Score { get; init; }
    public double TakeProfit { get; set; }
    public double StopLoss { get; set; }
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
    public long? SubscriptionId { get; set; }
    public bool Replay { get; set; }
}

/// <summary>Closed realized trade snapshot.</summary>
public sealed record ClosedTrade(string Venue, string Instrument, Side Side, double Size, double EntryPrice, double ExitPrice, double RealizedGross, double Fees, double RealizedPnL, DateTimeOffset ClosedAt);

public sealed record InstrumentPositionStats(string Venue, string Instrument, string SettlementCurrency, Side? Side, double Size, double Quantity, double Notional, double RealizedPnL, double UnrealizedPnL, double Fees, double RealizedPnLPercent, double UnrealizedPnLPercent, double TotalPnLPercent, double Leverage);

public sealed record CurrencyPositionStats
{
    public required string SettlementCurrency { get; init; }
    public double Equity { get; set; }
    public double Available { get; set; }
    public double Used { get; set; }
    public double RealizedPnL { get; set; }
    public double UnrealizedPnL { get; set; }
    public double Fees { get; set; }
    public double RealizedPnLPercent { get; set; }
    public double UnrealizedPnLPercent { get; set; }
    public double TotalPnLPercent { get; set; }
}

public sealed record PositionStats
{
    public double Equity { get; set; }
    public double Available { get; set; }
    public double Used { get; set; }
    public double RealizedPnL { get; set; }
    public double UnrealizedPnL { get; set; }
    public double Fees { get; set; }
    public double RealizedPnLPercent { get; set; }
    public double UnrealizedPnLPercent { get; set; }
    public double TotalPnLPercent { get; set; }
    public Dictionary<string, InstrumentPositionStats> ByInstrument { get; init; } = new();
    public Dictionary<string, CurrencyPositionStats> ByCurrency { get; init; } = new();
}
