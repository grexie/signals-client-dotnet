namespace Grexie.Signals.Client;

/// <summary>In-memory, fee-aware production-style position manager.</summary>
public sealed class PositionManager
{
    private readonly SignalsClient? _client;
    private readonly PositionManagerConfig _config;
    private readonly AssetManager _assets = new();
    private readonly InstrumentManager _instruments = new();
    private readonly Dictionary<string, Position> _positions = new();
    private readonly List<ClosedTrade> _closedTrades = new();

    public PositionManager(SignalsClient? client = null, PositionManagerConfig? config = null)
    {
        _client = client;
        _config = Normalize(config ?? PositionManagerConfig.ProductionDefaults());
    }

    /// <summary>Consume attached client events and yield order recommendations.</summary>
    public async IAsyncEnumerable<Order> RunAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_client is null) throw new InvalidOperationException("position manager has no SignalsClient");
        await foreach (var ev in _client.EventsAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var order in HandleEvent(ev))
            {
                yield return order;
            }
        }
    }

    public void AddPosition(Position position) => _positions[Key(position.Venue, position.Instrument)] = position;
    public void UpdatePosition(Position position) => AddPosition(position);
    public AssetManager AssetManager => _assets;
    public InstrumentManager InstrumentManager => _instruments;
    public IReadOnlyList<Position> Positions() => _positions.Values.OrderBy(p => p.Venue).ThenBy(p => p.Instrument).ToArray();
    public IReadOnlyList<ClosedTrade> ClosedTrades() => _closedTrades.ToArray();

    public PositionStats Stats()
    {
        var stats = new PositionStats();
        foreach (var asset in _assets.Assets())
        {
            stats.Equity += asset.Equity;
            stats.Available += asset.Available;
            stats.Used += asset.Used;
            stats.ByCurrency[asset.Currency] = new CurrencyPositionStats { SettlementCurrency = asset.Currency, Equity = asset.Equity, Available = asset.Available, Used = asset.Used };
        }
        foreach (var (key, position) in _positions)
        {
            var metadata = _instruments.Instrument(position.Venue, position.Instrument);
            var asset = _assets.Asset(metadata.SettlementCurrency);
            var equity = PositiveOr(asset?.Equity ?? 0, (asset?.Cash ?? 0) + (asset?.Used ?? 0), 1);
            var price = RoundToTick(PositiveOr(position.LastPrice, position.EntryPrice), metadata.TickSize);
            var notionalRaw = Math.Abs(position.Size) * equity * PositiveOr(position.Leverage, MinLeverage(key), 1);
            var quantity = price > 0 ? RoundDownToStep(notionalRaw / price, metadata.LotSize) : 0;
            var notional = quantity * price;
            var realized = position.RealizedPnL * equity;
            var unrealized = position.UnrealizedPnL * equity;
            var fees = position.Fees * equity;
            stats.ByInstrument[key] = new InstrumentPositionStats(position.Venue, position.Instrument, metadata.SettlementCurrency, position.Side, position.Size, quantity, notional, realized, unrealized, fees, position.RealizedPnL, position.UnrealizedPnL, position.RealizedPnL + position.UnrealizedPnL, position.Leverage);
            stats.RealizedPnL += realized;
            stats.UnrealizedPnL += unrealized;
            stats.Fees += fees;
            if (!stats.ByCurrency.TryGetValue(metadata.SettlementCurrency, out var currency))
            {
                currency = new CurrencyPositionStats { SettlementCurrency = metadata.SettlementCurrency, Equity = equity };
                stats.ByCurrency[metadata.SettlementCurrency] = currency;
            }
            currency.RealizedPnL += realized;
            currency.UnrealizedPnL += unrealized;
            currency.Fees += fees;
            if (currency.Equity > 0)
            {
                currency.RealizedPnLPercent = currency.RealizedPnL / currency.Equity;
                currency.UnrealizedPnLPercent = currency.UnrealizedPnL / currency.Equity;
                currency.TotalPnLPercent = (currency.RealizedPnL + currency.UnrealizedPnL) / currency.Equity;
            }
        }
        if (stats.Equity <= 0) stats.Equity = 1;
        stats.RealizedPnLPercent = stats.RealizedPnL / stats.Equity;
        stats.UnrealizedPnLPercent = stats.UnrealizedPnL / stats.Equity;
        stats.TotalPnLPercent = (stats.RealizedPnL + stats.UnrealizedPnL) / stats.Equity;
        return stats;
    }

    public IReadOnlyList<Order> ClosePosition(string venue, string instrument)
    {
        var key = Key(venue, instrument);
        if (!_positions.TryGetValue(key, out var position) || Math.Abs(position.Size) <= 1e-9) return Array.Empty<Order>();
        var delta = -position.Size;
        var order = OrderForDelta(key, position, delta, 0, 0, "closing", position.Confidence);
        if (!OrderMeetsInstrumentMinimum(order)) return Array.Empty<Order>();
        ApplyDelta(key, delta, PositiveOr(position.LastPrice, position.EntryPrice), TakerFeeRate(key));
        return new[] { order };
    }

    public IReadOnlyList<Order> HandleEvent(SignalsEvent ev)
    {
        if (ev is not SignalEvent signalEvent) return Array.Empty<Order>();
        if (signalEvent.Replay) return Array.Empty<Order>();
        var orders = HandleSignal(signalEvent.Signal).ToArray();
        foreach (var order in orders)
        {
            order.SubscriptionId = signalEvent.SubscriptionId;
            order.Replay = signalEvent.Replay;
        }
        return orders;
    }

    public IReadOnlyList<Order> HandleSignal(Signal signal)
    {
        if (string.IsNullOrWhiteSpace(signal.Venue) || string.IsNullOrWhiteSpace(signal.Instrument)) return Array.Empty<Order>();
        if (!_instruments.ContainsInstrument(signal.Venue, signal.Instrument)) return Array.Empty<Order>();
        var key = Key(signal.Venue, signal.Instrument);
        var targetSign = SideSign(signal.Side);
        var targetConfidence = Clamp01(signal.Confidence);
        if (targetSign == 0 || targetConfidence <= 0) return Array.Empty<Order>();
        var edge = FeeAdjustedExpectedEdge(signal, TakerFeeRate(key));
        if (_config.MinExpectedEdge > 0 && edge < _config.MinExpectedEdge) return Array.Empty<Order>();

        var now = signal.Timestamp ?? DateTimeOffset.UtcNow;
        var targetSize = targetSign * _config.PositionSize * targetConfidence;
        var minDelta = EffectiveMinOrderDelta();
        if (!_positions.TryGetValue(key, out var position))
        {
            if (Math.Abs(targetSize) < minDelta) return Array.Empty<Order>();
            position = new Position { Venue = signal.Venue, Instrument = signal.Instrument, EntryPrice = signal.Price, LastPrice = signal.Price, OpenedAt = now };
            _positions[key] = position;
        }
        else
        {
            var isFlip = Sign(position.Size) != 0 && Sign(position.Size) != targetSign;
            if (!isFlip && _config.RebalanceInterval > TimeSpan.Zero && position.LastSignalAt is not null && now < position.LastSignalAt + _config.RebalanceInterval) return Array.Empty<Order>();
            if (!isFlip && minDelta > 0 && Math.Abs(targetSize - position.Size) < minDelta) return Array.Empty<Order>();
        }

        position.Confidence = targetConfidence;
        position.LastSignalAt = now;
        if (signal.Price > 0)
        {
            position.LastPrice = signal.Price;
            if (position.EntryPrice <= 0) position.EntryPrice = signal.Price;
        }
        if (position.TakeProfit <= 0 || position.StopLoss <= 0 || position.Side != signal.Side)
        {
            position.TakeProfit = signal.TakeProfit;
            position.StopLoss = signal.StopLoss;
        }
        else
        {
            position.TakeProfit = BlendRisk(position.TakeProfit, signal.TakeProfit, 0.5);
            position.StopLoss = BlendRisk(position.StopLoss, signal.StopLoss, 0.5);
        }
        position.Leverage = SelectLeverage(key, targetConfidence, edge, signal.Score);
        return Rebalance(new Dictionary<string, double> { [key] = targetSign }, new Dictionary<string, SignalContext>
        {
            [key] = new(targetConfidence, signal.Score, edge, signal.TakeProfit, signal.StopLoss)
        });
    }

    private IReadOnlyList<Order> Rebalance(Dictionary<string, double> sideOverrides, Dictionary<string, SignalContext> contexts)
    {
        var weights = new Dictionary<string, double>();
        var sides = new Dictionary<string, double>();
        var totalWeight = 0.0;
        foreach (var (key, position) in _positions)
        {
            var hasOverride = sideOverrides.ContainsKey(key);
            var weight = Clamp01(position.Confidence);
            if (!hasOverride && weight <= 0) weight = ConfidenceFromSize(position);
            var side = sideOverrides.TryGetValue(key, out var overrideSide) ? overrideSide : Sign(position.Size);
            weights[key] = weight;
            sides[key] = side;
            if (weight > 1e-9 && side != 0) totalWeight += weight;
        }
        var usedBudget = Math.Min(_config.PositionSize, totalWeight);
        var orders = new List<Order>();
        foreach (var key in _positions.Keys.ToArray())
        {
            var position = _positions[key];
            var targetSize = totalWeight > 0 ? sides[key] * usedBudget * weights[key] / totalWeight : 0;
            var delta = targetSize - position.Size;
            if (Math.Abs(delta) <= 1e-9) continue;
            var isFlip = Math.Abs(position.Size) > 1e-9 && Math.Abs(targetSize) > 1e-9 && Sign(position.Size) != Sign(targetSize);
            var isOpening = Math.Abs(position.Size) <= 1e-9 && Math.Abs(targetSize) > 1e-9;
            var isClosing = Math.Abs(targetSize) <= 1e-9 && Math.Abs(position.Size) > 1e-9;
            if (!(isFlip || isOpening || isClosing) && Math.Abs(delta) < EffectiveMinOrderDelta()) continue;
            var context = contexts.GetValueOrDefault(key);
            var order = OrderForDelta(key, position, delta, context.ExpectedEdge, context.Score, OrderReason(position, targetSize), context.Confidence);
            order.TakeProfit = context.TakeProfit;
            order.StopLoss = context.StopLoss;
            if (!OrderMeetsInstrumentMinimum(order)) continue;
            orders.Add(order);
            ApplyDelta(key, delta, PositiveOr(position.LastPrice, position.EntryPrice), TakerFeeRate(key));
            if (_positions.TryGetValue(key, out var current)) current.Confidence = weights[key];
        }
        return orders;
    }

    private Order OrderForDelta(string key, Position position, double delta, double edge, double score, string reason, double confidence)
    {
        var feeRate = TakerFeeRate(key);
        var metadata = _instruments.Instrument(position.Venue, position.Instrument);
        var asset = _assets.Asset(metadata.SettlementCurrency);
        var leverage = SelectLeverage(key, confidence, edge, score);
        var equity = PositiveOr(asset?.Equity ?? 0, (asset?.Cash ?? 0) + (asset?.Used ?? 0), 1);
        var price = RoundToTick(PositiveOr(position.LastPrice, position.EntryPrice), metadata.TickSize);
        var notional = Math.Abs(delta) * equity * leverage;
        var quantity = price > 0 ? RoundDownToStep(notional / price, metadata.LotSize) : 0;
        notional = quantity * price;
        return new Order
        {
            Venue = position.Venue,
            Instrument = position.Instrument,
            Side = delta < 0 ? Side.Sell : Side.Buy,
            Reason = reason,
            SizeDelta = delta,
            PreviousSize = position.Size,
            TargetSize = position.Size + delta,
            Price = price,
            Confidence = confidence,
            Score = score,
            ExpectedEdge = edge,
            FeeRate = feeRate,
            EstimatedFee = Math.Abs(delta) * feeRate,
            EstimatedFeeValue = notional * feeRate,
            Quantity = quantity,
            Notional = notional,
            SettlementCurrency = metadata.SettlementCurrency,
            MinSize = metadata.MinSize,
            LotSize = metadata.LotSize,
            TickSize = metadata.TickSize,
            Leverage = leverage
        };
    }

    private void ApplyDelta(string key, double delta, double price, double feeRate)
    {
        if (!_positions.TryGetValue(key, out var position)) return;
        if (position.Size == 0 || Sign(position.Size) == Sign(delta))
        {
            var nextAbs = Math.Abs(position.Size) + Math.Abs(delta);
            if (price > 0)
            {
                position.EntryPrice = nextAbs > 0 && Math.Abs(position.Size) > 1e-9 && position.EntryPrice > 0 ? (position.EntryPrice * Math.Abs(position.Size) + price * Math.Abs(delta)) / nextAbs : price;
                position.LastPrice = price;
            }
            var fee = Math.Abs(delta) * feeRate;
            position.Fees += fee;
            position.RealizedPnL -= fee;
            position.Size += delta;
            return;
        }
        if (price > 0) position.LastPrice = price;
        var closing = Math.Min(Math.Abs(position.Size), Math.Abs(delta));
        var gross = position.PriceMove() * closing;
        var feeClose = closing * feeRate;
        position.RealizedGross += gross;
        position.Fees += feeClose;
        position.RealizedPnL += gross - feeClose;
        var closed = new ClosedTrade(position.Venue, position.Instrument, position.Side ?? Side.Buy, closing, position.EntryPrice, price, position.RealizedGross, position.Fees, position.RealizedPnL, DateTimeOffset.UtcNow);
        var remaining = Math.Abs(delta) - closing;
        if (remaining <= 1e-9)
        {
            position.Size += delta;
            if (Math.Abs(position.Size) <= 1e-9)
            {
                _positions.Remove(key);
                _closedTrades.Add(closed);
            }
            return;
        }
        _closedTrades.Add(closed);
        position.Size = Sign(delta) * remaining;
        position.EntryPrice = price;
        position.LastPrice = price;
        position.Confidence = 0;
        position.RealizedGross = 0;
        position.Fees = remaining * feeRate;
        position.RealizedPnL = -position.Fees;
    }

    private double EffectiveMinOrderDelta() => Math.Max(_config.MinOrderDelta, 0) * Math.Max(_config.PositionSize, 0);
    private double MakerFeeRate(string key) => _config.Instruments.TryGetValue(key, out var cfg) && cfg.MakerFeeRate is not null ? cfg.MakerFeeRate.Value : _config.MakerFeeRate;
    private double TakerFeeRate(string key) => _config.Instruments.TryGetValue(key, out var cfg) && cfg.TakerFeeRate is not null ? cfg.TakerFeeRate.Value : _config.TakerFeeRate;
    private double MinLeverage(string key) => _config.Instruments.TryGetValue(key, out var cfg) && cfg.MinLeverage is not null ? cfg.MinLeverage.Value : _config.MinLeverage;
    private double MaxLeverage(string key)
    {
        var configured = _config.Instruments.TryGetValue(key, out var cfg) && cfg.MaxLeverage is not null ? cfg.MaxLeverage.Value : _config.MaxLeverage;
        var parts = key.Split(':', 2);
        var metadataMax = parts.Length == 2 ? _instruments.Instrument(parts[0], parts[1]).MaxLeverage : 0;
        return metadataMax > 0 && configured > 0 ? Math.Min(configured, metadataMax) : configured;
    }
    private static bool OrderMeetsInstrumentMinimum(Order order) => order.Reason is "closing" or "flip" || (order.MinSize <= 0 || order.Quantity >= order.MinSize);
    private double SelectLeverage(string key, double confidence, double edge, double score)
    {
        var min = MinLeverage(key);
        var max = Math.Max(MaxLeverage(key), min);
        if (Math.Abs(max - min) <= double.Epsilon) return min;
        var edgeScore = Clamp01(edge / Math.Max(_config.MinExpectedEdge * 3, 0.001));
        var quality = Clamp01(Clamp01(confidence) * 0.65 + edgeScore * 0.25 + Math.Min(Math.Abs(score), 1) * 0.10);
        return min + (max - min) * quality;
    }

    private double ConfidenceFromSize(Position position) => _config.PositionSize <= 0 ? Clamp01(Math.Abs(position.Size)) : Clamp01(Math.Abs(position.Size) / _config.PositionSize);
    private static PositionManagerConfig Normalize(PositionManagerConfig config) => config with { PositionSize = Math.Clamp(config.PositionSize, 0, 1), MinExpectedEdge = Math.Max(config.MinExpectedEdge, 0), MinOrderDelta = Math.Clamp(config.MinOrderDelta, 0, 1), MakerFeeRate = Math.Max(config.MakerFeeRate, 0), TakerFeeRate = Math.Max(config.TakerFeeRate, 0), MinLeverage = Math.Max(config.MinLeverage, 0), MaxLeverage = Math.Max(config.MaxLeverage, 0) };
    private static string Key(string venue, string instrument) => $"{venue}:{instrument}";
    private static double Clamp01(double value) => Math.Clamp(value, 0, 1);
    private static double SideSign(Side side) => side == Side.Buy ? 1 : -1;
    private static double Sign(double value) => value < 0 ? -1 : value > 0 ? 1 : 0;
    private static double PositiveOr(double a, double b, double c = 0) => a > 0 ? a : b > 0 ? b : Math.Max(c, 0);
    private static double RoundDownToStep(double value, double step) => value <= 0 || step <= 0 ? value : Math.Floor(value / step) * step;
    private static double RoundToTick(double value, double tick) => value <= 0 || tick <= 0 ? value : Math.Round(value / tick) * tick;
    private static double ExpectedEdge(Signal signal) => Clamp01(signal.Confidence) * Math.Max(signal.TakeProfit, 0) - (1 - Clamp01(signal.Confidence)) * Math.Max(signal.StopLoss, 0);
    private static double FeeAdjustedExpectedEdge(Signal signal, double takerFeeRate) => ExpectedEdge(signal) - 2 * takerFeeRate;
    private static string OrderReason(Position position, double targetSize) => Math.Abs(position.Size) <= 1e-9 ? "opening" : Math.Abs(targetSize) <= 1e-9 ? "closing" : Sign(position.Size) != Sign(targetSize) ? "flip" : "rebalance";
    private static double BlendRisk(double current, double incoming, double gate) => current <= 0 ? incoming : incoming <= 0 ? current : current * (1 - Clamp01(gate)) + incoming * Clamp01(gate);

    private readonly record struct SignalContext(double Confidence, double Score, double ExpectedEdge, double TakeProfit, double StopLoss);
}
