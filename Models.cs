using System.Text.Json;

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
    public double TrailingStopActivation { get; set; }
    public double TrailingStopDistance { get; set; }
    public double TrailingStopMinProfit { get; set; }
    public double Score { get; set; }
    public IReadOnlyList<SignalComponent> Components { get; set; } = Array.Empty<SignalComponent>();
    public string? ModelVariant { get; set; }
    public string? ModelVersion { get; set; }
    public string? PredictionMode { get; set; }
    public string? ConfidenceMapping { get; set; }
    public double UpProbability { get; set; }
    public double DownProbability { get; set; }
    public double DirectionalEdge { get; set; }
    public double NormalizedEdge { get; set; }
    public double ExpectedValue { get; set; }
    public string? Regime { get; set; }
    public double RegimeConfidence { get; set; }
    public string? VolatilityState { get; set; }
    public string? SqueezeState { get; set; }
    public string? TrendState { get; set; }
    public double AtrPercent { get; set; }
    public double SignalTTL { get; set; }
    public DateTimeOffset? GeneratedAt { get; set; }
    public string? ArtifactID { get; set; }
    public string? ArtifactVersion { get; set; }
    public string? RejectedReason { get; set; }
    public bool ManagePositionsOnly { get; set; }
    public DateTimeOffset? Timestamp { get; set; }
    public double Price { get; set; }
}

/// <summary>Base type for all websocket events.</summary>
public abstract record SignalsEvent(string Type);

public sealed record ReadyEvent(string Message) : SignalsEvent("ready");
public sealed record SubscribedEvent(long SubscriptionId, string Venue, string Instrument) : SignalsEvent("subscribed");
public sealed record UnsubscribedEvent(long? SubscriptionId, string? Venue, string? Instrument, string? Code, string? Message) : SignalsEvent("unsubscribed");
public sealed record BasketUpdatedEvent(long SubscriptionId, string? Venue, string? BasketId, string? Message) : SignalsEvent("basket_updated");
public sealed record OrderRouterForwardedEvent(long SubscriptionId, string? Venue, string? BasketId, string? Message) : SignalsEvent("order_router_forwarded");
public sealed record InfoEvent(long SubscriptionId, string Venue, string Instrument, string Stage, string Message, DateTimeOffset? Timestamp, bool Replay, DateTimeOffset? ReplayedAt) : SignalsEvent("info");
public sealed record BacktestEvent(long SubscriptionId, string Venue, string Instrument, JsonElement Backtest, DateTimeOffset? Timestamp) : SignalsEvent("backtest");
public sealed record SignalEvent(long SubscriptionId, string Venue, string Instrument, Signal Signal, DateTimeOffset? Timestamp, bool Replay, DateTimeOffset? ReplayedAt) : SignalsEvent("signal");
public sealed record CreateMarketOrderEvent(long SubscriptionId, string? IntentId, string? Action, string? Reason, string? Venue, string Instrument, string Side, string? OrderType, double ContractSize, double Leverage, bool ReduceOnly, double TakeProfitPrice, double StopLossPrice, double TakeProfit, double StopLoss, DateTimeOffset? Timestamp, double Margin = 0, double Confidence = 0) : SignalsEvent("create-market-order");
public sealed record UpdateTPSLEvent(long SubscriptionId, string? IntentId, string? Venue, string Instrument, string Side, double TakeProfitPrice, double StopLossPrice, double TakeProfit, double StopLoss, DateTimeOffset? Timestamp) : SignalsEvent("update-tpsl");
public sealed record WithdrawEvent(long SubscriptionId, string? IntentId, string? Venue, string Currency, double Amount, DateTimeOffset? Timestamp) : SignalsEvent("withdraw");
public sealed record ErrorEvent(string? Code, string? Message) : SignalsEvent("error");

/// <summary>Account state for one settlement currency.</summary>
public sealed record AssetSnapshot
{
    public string Venue { get; init; } = string.Empty;
    public required string Currency { get; init; }
    public double Cash { get; init; }
    public double Available { get; init; }
    public double Used { get; init; }
    public double Equity { get; init; }
    public double MaxUsage { get; init; } = 1.0;
    public DateTimeOffset UpdatedAt { get; init; } = DateTimeOffset.UtcNow;
}

/// <summary>Current venue position snapshot for one instrument.</summary>
public sealed record Position
{
    public string Venue { get; init; } = string.Empty;
    public required string Instrument { get; init; }
    public string Status { get; init; } = string.Empty;
    public double Size { get; init; }
    public double Confidence { get; init; }
    public double EntryPrice { get; init; }
    public double LastPrice { get; init; }
    public double TakeProfit { get; init; }
    public double StopLoss { get; init; }
    public double TakeProfitPrice { get; init; }
    public double StopLossPrice { get; init; }
    public double TrailingStopActivation { get; init; }
    public double TrailingStopDistance { get; init; }
    public double TrailingStopMinProfit { get; init; }
    public double Margin { get; init; }
    public double Leverage { get; init; } = 1.0;
    public double MFE { get; init; }
    public double MAE { get; init; }
    public double RealizedGross { get; init; }
    public double Fees { get; init; }
    public double RealizedPnL { get; init; }
    public DateTimeOffset? OpenedAt { get; init; }
    public DateTimeOffset? LastSignalAt { get; init; }
    public Side? Side => Size < 0 ? Grexie.Signals.Client.Side.Sell : Size > 0 ? Grexie.Signals.Client.Side.Buy : null;
    public double UnrealizedPnL => PriceMove() * Math.Abs(Size) * (EntryPrice > 0 ? EntryPrice : 1);

    private double PriceMove()
    {
        if (EntryPrice <= 0 || LastPrice <= 0) return 0;
        return Size < 0 ? (EntryPrice - LastPrice) / EntryPrice : (LastPrice - EntryPrice) / EntryPrice;
    }
}

public sealed record RiskConfig
{
    /// <summary>Fraction of account cash the router may reserve for active positions.</summary>
    public double MaxMarginRatio { get; init; }
    /// <summary>Extra cash buffer applied to lot margin and fees before orders are allowed.</summary>
    public double MinLotHaircutRatio { get; init; }
    /// <summary>Maximum simultaneous active positions; zero leaves it unset.</summary>
    public int MaxConcurrentPositions { get; init; }
    /// <summary>Optional drawdown guard; zero leaves it unset.</summary>
    public double MaxDrawdown { get; init; }
    /// <summary>Router score buffer required before switching instruments.</summary>
    public double SwitchBuffer { get; init; }
    /// <summary>Minimum leverage the router may request; zero leaves it unset.</summary>
    public double MinLeverage { get; init; }
    /// <summary>Maximum leverage the router may request; zero leaves it unset.</summary>
    public double MaxLeverage { get; init; }
    /// <summary>Fraction of profits eligible for withdrawal events.</summary>
    public double ProfitWithdrawRatio { get; init; }
}

/// <summary>Runtime router risk patch sent after subscription.</summary>
public sealed record RuntimeConfig
{
    /// <summary>Runtime max margin ratio; zero means no change.</summary>
    public double MaxMarginRatio { get; init; }
    /// <summary>Runtime lot haircut ratio; zero means no change.</summary>
    public double MinLotHaircutRatio { get; init; }
    /// <summary>Runtime max concurrent positions; zero means no change.</summary>
    public int MaxConcurrentPositions { get; init; }
    /// <summary>Runtime max drawdown; zero means no change.</summary>
    public double MaxDrawdown { get; init; }
    /// <summary>Runtime switch buffer; zero means no change.</summary>
    public double SwitchBuffer { get; init; }
    /// <summary>Runtime min leverage; zero means no change.</summary>
    public double MinLeverage { get; init; }
    /// <summary>Runtime max leverage; zero means no change.</summary>
    public double MaxLeverage { get; init; }
    /// <summary>Current profit withdrawal ratio to send.</summary>
    public double ProfitWithdrawRatio { get; init; }
}
public sealed record WithdrawalRequest(string Currency, double Amount, string? Venue = null, string? Reason = null);
public sealed record SignalsManagerState(IReadOnlyList<AssetSnapshot>? Assets = null, IReadOnlyList<Position>? Positions = null);
public sealed record SignalsManagerConfig
{
    public string Venue { get; init; } = "okx";
    public IReadOnlyList<string> Instruments { get; init; } = Array.Empty<string>();
    public string? Mode { get; init; }
    public RiskConfig? Risk { get; init; }
    public double ProfitWithdrawRatio { get; init; }
}
