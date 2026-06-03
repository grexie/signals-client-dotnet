using System.Threading.Channels;

namespace Grexie.Signals.Client;

public interface ISignalsManagerClient : ISignalsEventSource
{
    Task SubscribeBasketAsync(string venue, IReadOnlyList<string> instruments, object? risk = null, double profitWithdrawRatio = 0, IReadOnlyList<AssetSnapshot>? assets = null, IReadOnlyList<Position>? positions = null, string? mode = null, CancellationToken cancellationToken = default);
    Task UpdateAssetAsync(long subscriptionId, AssetSnapshot asset, CancellationToken cancellationToken = default);
    Task UpdatePositionAsync(long subscriptionId, Position position, CancellationToken cancellationToken = default);
    Task AddInstrumentAsync(long subscriptionId, string instrument, CancellationToken cancellationToken = default);
    Task RemoveInstrumentAsync(long subscriptionId, string instrument, CancellationToken cancellationToken = default);
    Task UpdateConfigAsync(long subscriptionId, double profitWithdrawRatio, CancellationToken cancellationToken = default);
    Task ScheduleWithdrawalAsync(long subscriptionId, string currency, double amount, string? venue = null, string? reason = null, CancellationToken cancellationToken = default);
    Task UnsubscribeAsync(long subscriptionId, CancellationToken cancellationToken = default);
}

/// <summary>Owns one router basket subscription and forwards server-created intents.</summary>
public sealed class SignalsManager
{
    private const double FloatTolerance = 1e-9;
    private readonly ISignalsManagerClient _client;
    private SignalsManagerConfig _config;
    private long _subscriptionId;
    private readonly Dictionary<string, AssetSnapshot> _assets = new();
    private readonly Dictionary<string, Position> _positions = new();

    public SignalsManager(ISignalsManagerClient client, SignalsManagerState? state = null, SignalsManagerConfig? config = null)
    {
        _client = client;
        _config = Normalize(config ?? new SignalsManagerConfig());
        foreach (var asset in state?.Assets ?? Array.Empty<AssetSnapshot>()) RecordAsset(asset);
        foreach (var position in state?.Positions ?? Array.Empty<Position>()) RecordPosition(position);
    }

    public Channel<CreateMarketOrderEvent> Intents { get; } = Channel.CreateUnbounded<CreateMarketOrderEvent>();
    public Channel<UpdateTPSLEvent> ProtectionUpdates { get; } = Channel.CreateUnbounded<UpdateTPSLEvent>();
    public Channel<WithdrawEvent> Withdrawals { get; } = Channel.CreateUnbounded<WithdrawEvent>();
    public Channel<BacktestEvent> Backtests { get; } = Channel.CreateUnbounded<BacktestEvent>();
    public Channel<InfoEvent> Messages { get; } = Channel.CreateUnbounded<InfoEvent>();
    public Channel<SignalsEvent> Events { get; } = Channel.CreateUnbounded<SignalsEvent>();

    public long SubscriptionId => _subscriptionId;

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        await SubscribeAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await foreach (var ev in _client.EventsAsync(cancellationToken).ConfigureAwait(false))
            {
                await HandleEventAsync(ev, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            if (_subscriptionId > 0)
            {
                await _client.UnsubscribeAsync(_subscriptionId, cancellationToken).ConfigureAwait(false);
                _subscriptionId = 0;
            }
        }
    }

    public Task SubscribeAsync(CancellationToken cancellationToken = default)
    {
        return _client.SubscribeBasketAsync(_config.Venue, _config.Instruments, _config.Risk, _config.ProfitWithdrawRatio, Assets(), Positions(), _config.Mode, cancellationToken);
    }

    public async Task UpdateAssetAsync(AssetSnapshot asset, CancellationToken cancellationToken = default)
    {
        var next = RecordAsset(asset);
        if (next is not null && _subscriptionId > 0)
        {
            await _client.UpdateAssetAsync(_subscriptionId, next, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task UpdatePositionAsync(Position position, CancellationToken cancellationToken = default)
    {
        var next = RecordPosition(position);
        if (next is not null && _subscriptionId > 0)
        {
            await _client.UpdatePositionAsync(_subscriptionId, next, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task AddInstrumentAsync(string instrument, CancellationToken cancellationToken = default)
    {
        var normalized = NormalizeInstrument(instrument);
        if (string.IsNullOrEmpty(normalized)) return;
        _config = _config with { Instruments = _config.Instruments.Append(normalized).Select(NormalizeInstrument).Where(static item => item.Length > 0).Distinct().Order().ToArray() };
        if (_subscriptionId > 0) await _client.AddInstrumentAsync(_subscriptionId, normalized, cancellationToken).ConfigureAwait(false);
    }

    public async Task RemoveInstrumentAsync(string instrument, CancellationToken cancellationToken = default)
    {
        var normalized = NormalizeInstrument(instrument);
        _config = _config with { Instruments = _config.Instruments.Where(item => NormalizeInstrument(item) != normalized).ToArray() };
        if (_subscriptionId > 0) await _client.RemoveInstrumentAsync(_subscriptionId, normalized, cancellationToken).ConfigureAwait(false);
    }

    public async Task UpdateConfigAsync(RuntimeConfig config, CancellationToken cancellationToken = default)
    {
        _config = _config with { ProfitWithdrawRatio = Clamp01(config.ProfitWithdrawRatio) };
        if (_subscriptionId > 0) await _client.UpdateConfigAsync(_subscriptionId, _config.ProfitWithdrawRatio, cancellationToken).ConfigureAwait(false);
    }

    public Task ScheduleWithdrawalAsync(WithdrawalRequest withdrawal, CancellationToken cancellationToken = default)
    {
        if (_subscriptionId <= 0) throw new InvalidOperationException("basket is not subscribed");
        return _client.ScheduleWithdrawalAsync(_subscriptionId, withdrawal.Currency.ToUpperInvariant(), withdrawal.Amount, NormalizeVenue(withdrawal.Venue ?? _config.Venue), withdrawal.Reason, cancellationToken);
    }

    public async Task<bool> HandleEventAsync(SignalsEvent ev, CancellationToken cancellationToken = default)
    {
        if (!AcceptsEvent(ev)) return false;
        switch (ev)
        {
            case SubscribedEvent sub when sub.SubscriptionId > 0:
                _subscriptionId = sub.SubscriptionId;
                foreach (var asset in Assets()) await _client.UpdateAssetAsync(_subscriptionId, asset, cancellationToken).ConfigureAwait(false);
                foreach (var position in Positions()) await _client.UpdatePositionAsync(_subscriptionId, position, cancellationToken).ConfigureAwait(false);
                break;
            case UnsubscribedEvent unsub when unsub.SubscriptionId == _subscriptionId:
                _subscriptionId = 0;
                break;
            case CreateMarketOrderEvent intent:
                await Intents.Writer.WriteAsync(intent, cancellationToken).ConfigureAwait(false);
                break;
            case UpdateTPSLEvent tpsl:
                ApplyTPSL(tpsl);
                await ProtectionUpdates.Writer.WriteAsync(tpsl, cancellationToken).ConfigureAwait(false);
                break;
            case WithdrawEvent withdrawal:
                await Withdrawals.Writer.WriteAsync(withdrawal, cancellationToken).ConfigureAwait(false);
                break;
            case BacktestEvent backtest:
                await Backtests.Writer.WriteAsync(backtest, cancellationToken).ConfigureAwait(false);
                break;
            case InfoEvent info:
                await Messages.Writer.WriteAsync(info, cancellationToken).ConfigureAwait(false);
                break;
        }
        await Events.Writer.WriteAsync(ev, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public IReadOnlyList<AssetSnapshot> Assets() => _assets.Values.OrderBy(asset => asset.Currency).ToArray();

    public IReadOnlyList<Position> Positions() => _positions.Values.OrderBy(position => Key(position.Venue, position.Instrument)).ToArray();

    public SignalsManagerState State() => new(Assets(), Positions());

    public double AvailableOrderCash(string currency)
    {
        return _assets.TryGetValue(currency.ToUpperInvariant(), out var asset) ? Math.Max(0, asset.Available) * Clamp01(asset.MaxUsage > 0 ? asset.MaxUsage : 1) : 0;
    }

    private bool AcceptsEvent(SignalsEvent ev)
    {
        var subscriptionId = EventSubscriptionId(ev);
        if (_subscriptionId > 0 && subscriptionId > 0) return subscriptionId == _subscriptionId;
        return ev switch
        {
            SubscribedEvent sub => InstrumentInConfig(sub.Venue, sub.Instrument),
            InfoEvent info => InstrumentInConfig(info.Venue, info.Instrument),
            BacktestEvent backtest => InstrumentInConfig(backtest.Venue, backtest.Instrument),
            SignalEvent signal => InstrumentInConfig(signal.Venue, signal.Instrument),
            CreateMarketOrderEvent intent => InstrumentInConfig(intent.Venue ?? _config.Venue, intent.Instrument),
            UpdateTPSLEvent tpsl => InstrumentInConfig(tpsl.Venue ?? _config.Venue, tpsl.Instrument),
            _ => true
        };
    }

    private AssetSnapshot? RecordAsset(AssetSnapshot asset)
    {
        var currency = asset.Currency.Trim().ToUpperInvariant();
        if (string.IsNullOrEmpty(currency)) return null;
        var next = asset with
        {
            Venue = NormalizeVenue(string.IsNullOrWhiteSpace(asset.Venue) ? _config.Venue : asset.Venue),
            Currency = currency,
            MaxUsage = Clamp01(asset.MaxUsage > 0 ? asset.MaxUsage : 1)
        };
        _assets[currency] = next;
        return next;
    }

    private Position? RecordPosition(Position position)
    {
        var instrument = NormalizeInstrument(position.Instrument);
        if (string.IsNullOrEmpty(instrument)) return null;
        var next = position with
        {
            Venue = NormalizeVenue(string.IsNullOrWhiteSpace(position.Venue) ? _config.Venue : position.Venue),
            Instrument = instrument,
            Status = string.IsNullOrWhiteSpace(position.Status) ? Math.Abs(position.Size) > FloatTolerance ? "open" : "closed" : position.Status.Trim().ToLowerInvariant(),
            LastPrice = position.LastPrice > 0 ? position.LastPrice : position.EntryPrice
        };
        var key = Key(next.Venue, next.Instrument);
        if (next.Status == "closed" || Math.Abs(next.Size) <= FloatTolerance) _positions.Remove(key);
        else _positions[key] = next;
        return next;
    }

    private void ApplyTPSL(UpdateTPSLEvent ev)
    {
        var key = Key(ev.Venue ?? _config.Venue, ev.Instrument);
        if (!_positions.TryGetValue(key, out var position)) return;
        _positions[key] = position with
        {
            TakeProfit = ev.TakeProfit > 0 ? ev.TakeProfit : position.TakeProfit,
            StopLoss = ev.StopLoss > 0 ? ev.StopLoss : position.StopLoss,
            TakeProfitPrice = ev.TakeProfitPrice > 0 ? ev.TakeProfitPrice : position.TakeProfitPrice,
            StopLossPrice = ev.StopLossPrice > 0 ? ev.StopLossPrice : position.StopLossPrice
        };
    }

    private bool InstrumentInConfig(string venue, string instrument) => NormalizeVenue(venue) == _config.Venue && (string.IsNullOrWhiteSpace(instrument) || _config.Instruments.Contains(NormalizeInstrument(instrument)));
    private static long EventSubscriptionId(SignalsEvent ev) => ev switch
    {
        SubscribedEvent sub => sub.SubscriptionId,
        UnsubscribedEvent unsub => unsub.SubscriptionId ?? 0,
        InfoEvent info => info.SubscriptionId,
        BacktestEvent backtest => backtest.SubscriptionId,
        SignalEvent signal => signal.SubscriptionId,
        CreateMarketOrderEvent intent => intent.SubscriptionId,
        UpdateTPSLEvent tpsl => tpsl.SubscriptionId,
        WithdrawEvent withdrawal => withdrawal.SubscriptionId,
        _ => 0
    };
    private static SignalsManagerConfig Normalize(SignalsManagerConfig config) => config with { Venue = NormalizeVenue(config.Venue), Instruments = config.Instruments.Select(NormalizeInstrument).Where(static item => item.Length > 0).Distinct().Order().ToArray(), ProfitWithdrawRatio = Clamp01(config.ProfitWithdrawRatio) };
    private static string NormalizeVenue(string venue) => string.IsNullOrWhiteSpace(venue) ? "okx" : venue.Trim().ToLowerInvariant();
    private static string NormalizeInstrument(string instrument) => (instrument ?? string.Empty).Trim().ToUpperInvariant();
    private static string Key(string venue, string instrument) => $"{NormalizeVenue(venue)}:{NormalizeInstrument(instrument)}";
    private static double Clamp01(double value) => double.IsFinite(value) ? Math.Clamp(value, 0, 1) : 0;
}
