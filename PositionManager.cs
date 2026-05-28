namespace Grexie.Signals.Client;

/// <summary>In-memory, fee-aware production-style position manager.</summary>
public sealed class PositionManager
{
    private readonly ISignalsEventSource? _client;
    private PositionManagerConfig _config;
    private readonly AssetManager _assets = new();
    private readonly InstrumentManager _instruments = new();
    private readonly Dictionary<string, Position> _positions = new();
    private readonly List<ClosedTrade> _closedTrades = new();

    public PositionManager(ISignalsEventSource? client = null, PositionManagerConfig? config = null)
    {
        _client = client;
        _config = Normalize(config ?? PositionManagerConfig.ProductionDefaults());
        HydrateState(_config.InitialState);
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

    public void AddPosition(Position position)
    {
        if (position.Leverage <= 0) position.Leverage = MinLeverage(Key(position.Venue, position.Instrument));
        _positions[Key(position.Venue, position.Instrument)] = position;
        Persist();
    }
    public void UpdatePosition(Position position) => AddPosition(position);
    public void ReplacePositions(IEnumerable<Position> positions)
    {
        _positions.Clear();
        foreach (var position in positions)
        {
            if (string.IsNullOrWhiteSpace(position.Venue) || string.IsNullOrWhiteSpace(position.Instrument) || Math.Abs(position.Size) <= 1e-9) continue;
            if (position.Leverage <= 0) position.Leverage = MinLeverage(Key(position.Venue, position.Instrument));
            _positions[Key(position.Venue, position.Instrument)] = position;
        }
        Persist();
    }
    public AssetManager AssetManager => _assets;
    public InstrumentManager InstrumentManager => _instruments;
    public IReadOnlyList<Position> Positions() => _positions.Values.OrderBy(p => p.Venue).ThenBy(p => p.Instrument).ToArray();
    public IReadOnlyList<ClosedTrade> ClosedTrades() => _closedTrades.ToArray();
    public PositionManagerState State() => new(Positions(), ClosedTrades());

    public void UpdateConfig(PositionManagerConfig config)
    {
        _config = Normalize(config with
        {
            Instruments = config.Instruments.Count > 0 ? config.Instruments : _config.Instruments,
            InitialState = null,
            Persist = config.Persist ?? _config.Persist
        });
    }

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
            var equity = PositiveOr(PositiveOr(asset?.Equity ?? 0, (asset?.Cash ?? 0) + (asset?.Used ?? 0), asset?.Cash ?? 0), 1);
            var price = RoundToTick(PositiveOr(position.LastPrice, position.EntryPrice), metadata.TickSize);
            var contractNotional = InstrumentContractNotional(price, metadata);
            var quantity = contractNotional > 0 ? RoundDownToStep(Math.Abs(position.Size), metadata.LotSize) : Math.Abs(position.Size);
            var notional = quantity * contractNotional;
            var realized = position.RealizedPnL;
            var unrealized = PositionUnrealizedPnL(key, position);
            var fees = position.Fees;
            stats.ByInstrument[key] = new InstrumentPositionStats(position.Venue, position.Instrument, metadata.SettlementCurrency, position.Side, position.Size, quantity, notional, realized, unrealized, fees, RatioOrZero(position.RealizedPnL, equity), RatioOrZero(unrealized, equity), RatioOrZero(position.RealizedPnL + unrealized, equity), position.Leverage);
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
        ApplyDelta(key, order.SizeDelta, PositiveOr(position.LastPrice, position.EntryPrice), TakerFeeRate(key), "closing");
        Persist();
        return new[] { order };
    }

    public IReadOnlyList<Order> UpdatePrice(string venue, string instrument, double price)
    {
        if (price <= 0) return Array.Empty<Order>();
        var key = Key(venue, instrument);
        if (!_positions.TryGetValue(key, out var position) || Math.Abs(position.Size) <= 1e-9) return Array.Empty<Order>();

        position.LastPrice = price;
        position.UpdateExcursion();
        var reason = ExitReason(position, price);
        if (reason.Length == 0)
        {
            Persist();
            return Array.Empty<Order>();
        }

        var feeRate = reason == "take_profit" ? MakerFeeRate(key) : TakerFeeRate(key);
        var order = OrderForDelta(key, position, -position.Size, 0, 0, reason, position.Confidence);
        order.FeeRate = feeRate;
        order.EstimatedFee = FeeValueForNotional(order.Notional, feeRate);
        order.EstimatedFeeValue = order.Notional * feeRate;
        if (!OrderMeetsInstrumentMinimum(order))
        {
            Persist();
            return Array.Empty<Order>();
        }

        ApplyDelta(key, order.SizeDelta, price, feeRate, reason);
        Persist();
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
        if (_config.MinExpectedEdge > 0 && edge < _config.MinExpectedEdge && !signal.ManagePositionsOnly) return Array.Empty<Order>();
        var (trailingStopActivation, trailingStopDistance, trailingStopMinProfit) = TrailingConfigForSignal(key, signal);

        var now = signal.Timestamp ?? DateTimeOffset.UtcNow;
        var portfolioBudget = MaxPortfolioMarginBudget();
        var minDelta = EffectiveMinOrderDelta();
        if (!_positions.TryGetValue(key, out var position) || Math.Abs(position.Size) <= 1e-9)
        {
            if (signal.ManagePositionsOnly) return Array.Empty<Order>();
            if (portfolioBudget < minDelta || !MeetsMinimumPositionSize(portfolioBudget)) return Array.Empty<Order>();
            if (!_positions.TryGetValue(key, out position))
            {
                position = new Position { Venue = signal.Venue, Instrument = signal.Instrument, EntryPrice = signal.Price, LastPrice = signal.Price, OpenedAt = now };
                _positions[key] = position;
            }
        }
        else
        {
            var isFlip = Sign(position.Size) != 0 && Sign(position.Size) != targetSign;
            var belowMinimum = !MeetsMinimumPositionSize(PositionMargin(key, position));
            if (!isFlip && !belowMinimum && _config.RebalanceInterval > TimeSpan.Zero && position.LastSignalAt is not null && now < position.LastSignalAt + _config.RebalanceInterval) return Array.Empty<Order>();
        }

        if (signal.ManagePositionsOnly && Sign(position.Size) == 0) return Array.Empty<Order>();
        var contextConfidence = targetConfidence;
        var overrideSide = targetSign;
        if (signal.ManagePositionsOnly)
        {
            if (Sign(position.Size) != targetSign) overrideSide = 0;
            else contextConfidence = Math.Min(contextConfidence, Clamp01(position.Confidence));
        }

        position.Confidence = contextConfidence;
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
        if (trailingStopActivation > 0 && trailingStopDistance > 0)
        {
            position.TrailingStopActivation = trailingStopActivation;
            position.TrailingStopDistance = trailingStopDistance;
            position.TrailingStopMinProfit = trailingStopMinProfit;
        }
        position.Leverage = SelectLeverage(key, contextConfidence, edge, signal.Score);
        var orders = Rebalance(new Dictionary<string, double> { [key] = overrideSide }, new Dictionary<string, SignalContext>
        {
            [key] = new(contextConfidence, signal.Score, edge, signal.TakeProfit, signal.StopLoss, trailingStopActivation, trailingStopDistance, trailingStopMinProfit, signal.ManagePositionsOnly)
        });
        Persist();
        return orders;
    }

    private void HydrateState(PositionManagerState? state)
    {
        if (state is null) return;
        _positions.Clear();
        foreach (var position in state.Positions)
        {
            if (string.IsNullOrWhiteSpace(position.Venue) || string.IsNullOrWhiteSpace(position.Instrument) || Math.Abs(position.Size) <= 1e-9) continue;
            if (position.Leverage <= 0) position.Leverage = MinLeverage(Key(position.Venue, position.Instrument));
            _positions[Key(position.Venue, position.Instrument)] = position;
        }
        _closedTrades.Clear();
        _closedTrades.AddRange(state.ClosedTrades);
    }

    private void Persist() => _config.Persist?.Invoke(State());

    private IReadOnlyList<Order> Rebalance(Dictionary<string, double> sideOverrides, Dictionary<string, SignalContext> contexts)
    {
        var portfolioBudget = MaxPortfolioMarginBudget();
        if (portfolioBudget <= 0 || _positions.Count == 0) return Array.Empty<Order>();
        var weights = new Dictionary<string, double>();
        var sides = new Dictionary<string, double>();
        foreach (var (key, position) in _positions)
        {
            var hasOverride = sideOverrides.ContainsKey(key);
            var weight = Clamp01(position.Confidence);
            if (!hasOverride && weight <= 0) weight = Clamp01(PositionMargin(key, position) / portfolioBudget);
            var side = sideOverrides.TryGetValue(key, out var overrideSide) ? overrideSide : Sign(position.Size);
            weights[key] = weight;
            sides[key] = side;
        }
        var keys = _positions.Keys.OrderBy(key => key).ToArray();
        var targets = AllocateTargetSizes(keys, weights, sides, contexts);
        var reductions = new List<RebalanceCandidate>();
        var openings = new List<RebalanceCandidate>();
        foreach (var key in keys)
        {
            var position = _positions[key];
            var targetSize = targets.GetValueOrDefault(key);
            if (Math.Abs(position.Size) > 1e-9 && !MeetsMinimumPositionSize(PositionMargin(key, position)))
            {
                targetSize = 0;
            }
            else if (targetSize != 0 && !MeetsMinimumPositionSize(MarginForQuantity(key, position, targetSize)))
            {
                if (Math.Abs(position.Size) <= 1e-9)
                {
                    position.Confidence = weights[key];
                    continue;
                }
                targetSize = 0;
            }
            var context = contexts.GetValueOrDefault(key);
            if (context.ManagePositionsOnly) targetSize = ManagePositionsOnlyTargetSize(position.Size, targetSize);
            var delta = targetSize - position.Size;
            var isFlip = IsFlipTarget(position.Size, targetSize);
            if (isFlip) delta = -position.Size;
            if (Math.Abs(delta) <= 1e-9)
            {
                position.Confidence = weights[key];
                continue;
            }
            var isOpening = Math.Abs(position.Size) <= 1e-9 && Math.Abs(targetSize) > 1e-9;
            var isClosing = Math.Abs(targetSize) <= 1e-9 && Math.Abs(position.Size) > 1e-9;
            if (!(isFlip || isOpening || isClosing) && MarginForQuantity(key, position, delta) < EffectiveMinOrderDelta())
            {
                position.Confidence = weights[key];
                continue;
            }
            var candidate = new RebalanceCandidate(key, position, delta, weights[key], context, OrderReason(position, targetSize));
            if (IsExposureReduction(position.Size, position.Size + delta))
            {
                reductions.Add(candidate);
            }
            else
            {
                openings.Add(candidate);
            }
        }
        return reductions.Count > 0 ? MaterializeRebalanceOrders(reductions, false) : MaterializeRebalanceOrders(openings, true);
    }

    private Dictionary<string, double> AllocateTargetSizes(
        IReadOnlyList<string> keys,
        IReadOnlyDictionary<string, double> weights,
        IReadOnlyDictionary<string, double> sides,
        IReadOnlyDictionary<string, SignalContext> contexts)
    {
        var targets = new Dictionary<string, double>();
        var portfolioBudget = MaxPortfolioMarginBudget();
        if (portfolioBudget <= 0) return targets;
        var active = keys.Where(key => weights.GetValueOrDefault(key) > 1e-9 && sides.GetValueOrDefault(key) != 0).ToHashSet();
        while (active.Count > 0)
        {
            var totalWeight = active.Sum(key => weights.GetValueOrDefault(key));
            if (totalWeight <= 1e-9) break;
            var dropped = "";
            var droppedWeight = double.PositiveInfinity;
            foreach (var key in keys)
            {
                if (!active.Contains(key) || !_positions.TryGetValue(key, out var position)) continue;
                var desiredBudget = portfolioBudget * weights.GetValueOrDefault(key) / totalWeight;
                if (ExecutableAllocationForBudget(key, position, desiredBudget, contexts.GetValueOrDefault(key)).Margin > 1e-9) continue;
                var weight = weights.GetValueOrDefault(key);
                if (weight < droppedWeight || (Math.Abs(weight - droppedWeight) <= 1e-9 && (dropped.Length == 0 || string.CompareOrdinal(key, dropped) < 0)))
                {
                    dropped = key;
                    droppedWeight = weight;
                }
            }
            if (dropped.Length == 0) break;
            active.Remove(dropped);
        }
        if (active.Count == 0) return targets;

        var totalActiveWeight = active.Sum(key => weights.GetValueOrDefault(key));
        if (totalActiveWeight <= 1e-9) return targets;
        var allocated = 0.0;
        foreach (var key in keys)
        {
            if (!active.Contains(key) || !_positions.TryGetValue(key, out var position)) continue;
            var desiredBudget = portfolioBudget * weights.GetValueOrDefault(key) / totalActiveWeight;
            var executable = ExecutableAllocationForBudget(key, position, desiredBudget, contexts.GetValueOrDefault(key));
            if (executable.Margin <= 1e-9) continue;
            if (!MeetsMinimumPositionSize(executable.Margin)) continue;
            targets[key] = sides.GetValueOrDefault(key) * executable.Quantity;
            allocated += executable.Margin + executable.Fee;
        }

        var free = portfolioBudget - allocated;
        if (free <= 1e-9) return targets;
        foreach (var key in keys.OrderByDescending(key => weights.GetValueOrDefault(key)).ThenBy(key => key))
        {
            if (!active.Contains(key) || free <= 1e-9 || !_positions.TryGetValue(key, out var position)) continue;
            var step = ExecutableLotStepCost(key, position, contexts.GetValueOrDefault(key));
            var stepCost = step.Margin + step.Fee;
            if (stepCost <= 1e-9)
            {
                var executable = ExecutableAllocationForBudget(key, position, free, contexts.GetValueOrDefault(key));
                if (executable.Quantity > 1e-9 && MeetsMinimumPositionSize(executable.Margin)) targets[key] = targets.GetValueOrDefault(key) + sides.GetValueOrDefault(key) * executable.Quantity;
                break;
            }
            var steps = Math.Floor((free + 1e-9) / stepCost);
            if (steps <= 0) continue;
            var next = targets.GetValueOrDefault(key) + sides.GetValueOrDefault(key) * steps * step.Quantity;
            var nextMargin = step.Quantity > 0 ? Math.Abs(next) * step.Margin / step.Quantity : 0;
            if (!MeetsMinimumPositionSize(nextMargin)) continue;
            targets[key] = next;
            free -= steps * stepCost;
        }
        return targets;
    }

    private IReadOnlyList<Order> MaterializeRebalanceOrders(IReadOnlyList<RebalanceCandidate> candidates, bool capOpenings)
    {
        var orders = new List<Order>();
        var openingExposureByCurrency = new Dictionary<string, double>();
        foreach (var candidate in candidates)
        {
            var delta = candidate.Delta;
            if (candidate.Context.ManagePositionsOnly && !IsExposureReduction(candidate.Position.Size, candidate.Position.Size + delta))
            {
                if (_positions.TryGetValue(candidate.Key, out var current)) current.Confidence = candidate.Weight;
                continue;
            }
            if (capOpenings && !IsExposureReduction(candidate.Position.Size, candidate.Position.Size + delta))
            {
                var metadata = _instruments.Instrument(candidate.Position.Venue, candidate.Position.Instrument);
                var used = openingExposureByCurrency.GetValueOrDefault(metadata.SettlementCurrency);
                var available = AvailableExposureBudget(metadata.SettlementCurrency) - used;
                if (available <= 1e-9)
                {
                    if (_positions.TryGetValue(candidate.Key, out var current)) current.Confidence = candidate.Weight;
                    continue;
                }
                delta = CapOpeningDeltaToBudget(candidate.Key, candidate.Position, delta, candidate.Context, available);
                if (Math.Abs(delta) <= 1e-9)
                {
                    if (_positions.TryGetValue(candidate.Key, out var current)) current.Confidence = candidate.Weight;
                    continue;
                }
            }
            var order = OrderForDelta(candidate.Key, candidate.Position, delta, candidate.Context.ExpectedEdge, candidate.Context.Score, candidate.Reason, candidate.Context.Confidence);
            order.TakeProfit = candidate.Context.TakeProfit;
            order.StopLoss = candidate.Context.StopLoss;
            order.TrailingStopActivation = candidate.Context.TrailingStopActivation;
            order.TrailingStopDistance = candidate.Context.TrailingStopDistance;
            order.TrailingStopMinProfit = candidate.Context.TrailingStopMinProfit;
            if (!OrderMeetsInstrumentMinimum(order))
            {
                if (_positions.TryGetValue(candidate.Key, out var current)) current.Confidence = candidate.Weight;
                continue;
            }
            orders.Add(order);
            if (capOpenings && !IsExposureReduction(order.PreviousSize, order.TargetSize))
            {
                openingExposureByCurrency[order.SettlementCurrency] = openingExposureByCurrency.GetValueOrDefault(order.SettlementCurrency) + OrderBudgetCost(order);
            }
            ApplyDelta(candidate.Key, order.SizeDelta, PositiveOr(candidate.Position.LastPrice, candidate.Position.EntryPrice), TakerFeeRate(candidate.Key), candidate.Reason);
            if (_positions.TryGetValue(candidate.Key, out var updated))
            {
                updated.Confidence = candidate.Weight;
                if (candidate.Context.TrailingStopActivation > 0 && candidate.Context.TrailingStopDistance > 0)
                {
                    updated.TrailingStopActivation = candidate.Context.TrailingStopActivation;
                    updated.TrailingStopDistance = candidate.Context.TrailingStopDistance;
                    updated.TrailingStopMinProfit = candidate.Context.TrailingStopMinProfit;
                }
            }
        }
        return orders;
    }

    private double AvailableExposureBudget(string currency)
    {
        var portfolioBudget = AvailablePortfolioBudget();
        var asset = _assets.Asset(currency);
        if (asset is null) return portfolioBudget;
        if (asset.Available <= 0) return 0;
        var budget = Math.Max(0, asset.Available);
        if (_config.AvailableMarginBuffer > 0) budget *= 1 - _config.AvailableMarginBuffer;
        return Math.Min(budget, portfolioBudget);
    }

    private double AvailablePortfolioBudget()
    {
        var maxBudget = MaxPortfolioMarginBudget();
        var used = _positions.Sum(item => PositionMargin(item.Key, item.Value));
        return Math.Max(0, maxBudget - used);
    }

    private double MaxPortfolioMarginBudget()
    {
        var capital = PortfolioCapital();
        return capital <= 0 || _config.MaxMarginRatio <= 0 ? 0 : capital * _config.MaxMarginRatio;
    }

    private double PortfolioCapital()
    {
        var capital = _assets.Assets().Sum(asset => PositiveOr(asset.Equity, asset.Cash + asset.Used, asset.Cash));
        return capital > 0 ? capital : 1;
    }

    private double PositionMargin(string key, Position position) => Math.Abs(position.Size) <= 1e-9 ? 0 : MarginForQuantity(key, position, position.Size);

    private double MarginForQuantity(string key, Position position, double quantity)
    {
        if (Math.Abs(quantity) <= 1e-9) return 0;
        var metadata = _instruments.Instrument(position.Venue, position.Instrument);
        var price = RoundToTick(PositiveOr(position.LastPrice, position.EntryPrice), metadata.TickSize);
        var contractNotional = InstrumentContractNotional(price, metadata);
        var leverage = PositiveOr(position.Leverage, MinLeverage(key), 1);
        return contractNotional <= 0 || leverage <= 0 ? 0 : Math.Abs(quantity) * contractNotional / leverage;
    }

    private double PositionUnrealizedPnL(string key, Position position) => Math.Abs(position.Size) <= 1e-9 || position.EntryPrice <= 0 || position.LastPrice <= 0 ? 0 : RealizedGrossForQuantity(key, position, Math.Abs(position.Size), position.LastPrice);

    private double RealizedGrossForQuantity(string key, Position position, double quantity, double exitPrice)
    {
        if (quantity <= 1e-9 || position.EntryPrice <= 0 || exitPrice <= 0) return 0;
        var metadata = _instruments.Instrument(position.Venue, position.Instrument);
        var priceMove = position.Size < 0 ? position.EntryPrice - exitPrice : exitPrice - position.EntryPrice;
        return priceMove * quantity * PositiveOr(metadata.ContractValue, 1) * PositiveOr(metadata.ContractMultiplier, 1);
    }

    private double FeeForQuantity(string key, Position position, double quantity, double price, double feeRate)
    {
        if (quantity <= 1e-9 || price <= 0 || feeRate <= 0) return 0;
        var metadata = _instruments.Instrument(position.Venue, position.Instrument);
        return quantity * InstrumentContractNotional(price, metadata) * feeRate;
    }

    private ExecutableAllocation ExecutableAllocationForBudget(string key, Position position, double budget, SignalContext context)
    {
        if (budget <= 1e-9) return default;
        var metadata = _instruments.Instrument(position.Venue, position.Instrument);
        var price = RoundToTick(PositiveOr(position.LastPrice, position.EntryPrice), metadata.TickSize);
        var leverage = SelectLeverage(key, context.Confidence > 0 ? context.Confidence : position.Confidence, context.ExpectedEdge, context.Score);
        var contractNotional = InstrumentContractNotional(price, metadata);
        if (contractNotional <= 0 || leverage <= 0) return default;
        var feeRate = TakerFeeRate(key);
        var maxMargin = budget;
        if (metadata.LotSize <= 0)
        {
            var feeMultiplier = 1 + leverage * feeRate;
            if (feeMultiplier > 0) maxMargin = budget / feeMultiplier;
        }
        var quantity = RoundDownToStep(maxMargin * leverage / contractNotional, metadata.LotSize);
        while (quantity > 1e-9)
        {
            if (metadata.MinSize > 0 && quantity < metadata.MinSize) return default;
            var margin = quantity * contractNotional / leverage;
            var fee = quantity * contractNotional * feeRate;
            if (margin + fee <= budget + 1e-9) return new ExecutableAllocation(quantity, margin, fee);
            if (metadata.LotSize <= 0) return default;
            quantity = RoundDownToStep(quantity - metadata.LotSize, metadata.LotSize);
        }
        return default;
    }

    private ExecutableAllocation ExecutableLotStepCost(string key, Position position, SignalContext context)
    {
        var metadata = _instruments.Instrument(position.Venue, position.Instrument);
        if (metadata.LotSize <= 0) return default;
        var price = RoundToTick(PositiveOr(position.LastPrice, position.EntryPrice), metadata.TickSize);
        var leverage = SelectLeverage(key, context.Confidence > 0 ? context.Confidence : position.Confidence, context.ExpectedEdge, context.Score);
        var contractNotional = InstrumentContractNotional(price, metadata);
        if (contractNotional <= 0 || leverage <= 0) return default;
        return new ExecutableAllocation(
            metadata.LotSize,
            metadata.LotSize * contractNotional / leverage,
            metadata.LotSize * contractNotional * TakerFeeRate(key));
    }

    private double CapOpeningDeltaToBudget(string key, Position position, double delta, SignalContext context, double budget)
    {
        if (Math.Abs(delta) <= 1e-9 || budget <= 1e-9) return 0;
        var executable = ExecutableAllocationForBudget(key, position, budget, context);
        if (executable.Margin <= 1e-9) return 0;
        if (!MeetsMinimumPositionSize(executable.Margin)) return 0;
        if (executable.Quantity < Math.Abs(delta)) return CapExecutableDeltaWithBufferedCost(key, position, Sign(delta) * executable.Quantity, context, budget);
        var order = OrderForDelta(key, position, delta, context.ExpectedEdge, context.Score, "budget-check", context.Confidence);
        return OrderBudgetCost(order) > budget + 1e-9 ? CapExecutableDeltaWithBufferedCost(key, position, Sign(delta) * executable.Quantity, context, budget) : delta;
    }

    private double CapExecutableDeltaWithBufferedCost(string key, Position position, double delta, SignalContext context, double budget)
    {
        if (Math.Abs(delta) <= 1e-9 || budget <= 1e-9) return 0;
        var metadata = _instruments.Instrument(position.Venue, position.Instrument);
        var quantityStep = metadata.LotSize > 0 ? metadata.LotSize : 0;
        var candidate = Math.Abs(delta);
        while (candidate > 1e-9)
        {
            var order = OrderForDelta(key, position, Sign(delta) * candidate, context.ExpectedEdge, context.Score, "budget-check", context.Confidence);
            if (OrderBudgetCost(order) <= budget + 1e-9) return Sign(delta) * candidate;
            if (quantityStep <= 1e-9) return CapContinuousOpeningDeltaToBudget(key, position, delta, context, budget);
            candidate -= quantityStep;
        }
        return 0;
    }

    private double CapContinuousOpeningDeltaToBudget(string key, Position position, double delta, SignalContext context, double budget)
    {
        var low = 0.0;
        var high = Math.Abs(delta);
        for (var i = 0; i < 64; i++)
        {
            var mid = (low + high) / 2;
            if (mid <= 1e-9) break;
            var order = OrderForDelta(key, position, Sign(delta) * mid, context.ExpectedEdge, context.Score, "budget-check", context.Confidence);
            if (OrderBudgetCost(order) <= budget + 1e-9) low = mid; else high = mid;
        }
        return low <= 1e-9 ? 0 : Sign(delta) * low;
    }

    private Order OrderForDelta(string key, Position position, double delta, double edge, double score, string reason, double confidence)
    {
        var feeRate = TakerFeeRate(key);
        var metadata = _instruments.Instrument(position.Venue, position.Instrument);
        var leverage = SelectLeverage(key, confidence, edge, score);
        var price = RoundToTick(PositiveOr(position.LastPrice, position.EntryPrice), metadata.TickSize);
        var requestedAbsDelta = Math.Abs(delta);
        var contractNotional = InstrumentContractNotional(price, metadata);
        var closesToZero = Math.Abs(position.Size) > 1e-9 && Math.Abs(position.Size + delta) <= 1e-9;
        var quantity = contractNotional > 0 && !closesToZero ? RoundDownToStep(requestedAbsDelta, metadata.LotSize) : requestedAbsDelta;
        var notional = quantity * contractNotional;
        var margin = leverage > 0 ? notional / leverage : 0;
        var executableDelta = Sign(delta) * quantity;
        var reduceOnly = IsExposureReduction(position.Size, position.Size + executableDelta);
        return new Order
        {
            Venue = position.Venue,
            Instrument = position.Instrument,
            Side = delta < 0 ? Side.Sell : Side.Buy,
            Reason = reason,
            SizeDelta = executableDelta,
            PreviousSize = position.Size,
            TargetSize = position.Size + executableDelta,
            Price = price,
            Confidence = confidence,
            Score = score,
            ExpectedEdge = edge,
            FeeRate = feeRate,
            EstimatedFee = FeeValueForNotional(notional, feeRate),
            EstimatedFeeValue = notional * feeRate,
            Margin = margin,
            Quantity = quantity,
            Notional = notional,
            SettlementCurrency = metadata.SettlementCurrency,
            MinSize = metadata.MinSize,
            LotSize = metadata.LotSize,
            TickSize = metadata.TickSize,
            Leverage = leverage,
            TrailingStopActivation = position.TrailingStopActivation,
            TrailingStopDistance = position.TrailingStopDistance,
            TrailingStopMinProfit = position.TrailingStopMinProfit,
            ReduceOnly = reduceOnly
        };
    }

    private void ApplyDelta(string key, double delta, double price, double feeRate, string reason)
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
            var fee = FeeForQuantity(key, position, Math.Abs(delta), price, feeRate);
            position.Fees += fee;
            position.RealizedPnL -= fee;
            position.Size += delta;
            position.ResetExcursion();
            return;
        }
        if (price > 0) position.LastPrice = price;
        position.UpdateExcursion();
        var closing = Math.Min(Math.Abs(position.Size), Math.Abs(delta));
        var gross = RealizedGrossForQuantity(key, position, closing, price);
        var feeClose = FeeForQuantity(key, position, closing, price, feeRate);
        position.RealizedGross += gross;
        position.Fees += feeClose;
        position.RealizedPnL += gross - feeClose;
        var closed = new ClosedTrade(position.Venue, position.Instrument, position.Side ?? Side.Buy, closing, position.EntryPrice, price, position.PriceMove(), position.RealizedGross, position.Fees, position.RealizedPnL, position.MFE, position.MAE, reason, DateTimeOffset.UtcNow);
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
        position.Fees = FeeForQuantity(key, position, remaining, price, feeRate);
        position.RealizedPnL = -position.Fees;
        position.ResetExcursion();
    }

    private double EffectiveMinOrderDelta() => Math.Max(_config.MinOrderDelta, 0) * MaxPortfolioMarginBudget();
    private double MinimumPositionSize() => _config.MinPositionSizeRatio <= 0 ? 0 : _config.MinPositionSizeRatio * PortfolioCapital();
    private bool MeetsMinimumPositionSize(double size)
    {
        var minimum = MinimumPositionSize();
        return minimum <= 0 || Math.Abs(size) + 1e-9 >= minimum;
    }
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
    private (double Activation, double Distance, double MinProfit) TrailingConfigForSignal(string key, Signal signal)
    {
        var activation = signal.TrailingStopActivation;
        var distance = signal.TrailingStopDistance;
        var minProfit = signal.TrailingStopMinProfit;
        if ((activation <= 0 || distance <= 0) && _config.Instruments.TryGetValue(key, out var cfg))
        {
            activation = cfg.TrailingStopActivation ?? 0;
            distance = cfg.TrailingStopDistance ?? 0;
            minProfit = cfg.TrailingStopMinProfit ?? 0;
        }
        if (activation <= 0 || distance <= 0) return (0, 0, 0);
        var feeFloor = 2 * TakerFeeRate(key);
        minProfit = Math.Max(minProfit, feeFloor);
        if (activation < minProfit + 1e-9) activation = minProfit + Math.Min(distance, feeFloor);
        return (Math.Max(activation, 0), Math.Max(distance, 0), Math.Max(minProfit, 0));
    }
    private static bool OrderMeetsInstrumentMinimum(Order order) => order.Quantity > 0 && (order.Reason is "closing" or "flip" || order.MinSize <= 0 || order.Quantity >= order.MinSize);
    private double SelectLeverage(string key, double confidence, double edge, double score)
    {
        var min = MinLeverage(key);
        var max = Math.Max(MaxLeverage(key), min);
        if (Math.Abs(max - min) <= double.Epsilon) return min;
        var edgeScore = Clamp01(edge / Math.Max(_config.MinExpectedEdge * 3, 0.001));
        var quality = Clamp01(Clamp01(confidence) * 0.65 + edgeScore * 0.25 + Math.Min(Math.Abs(score), 1) * 0.10);
        return min + (max - min) * quality;
    }

    private static PositionManagerConfig Normalize(PositionManagerConfig config)
    {
        var maxMarginRatio = config.MaxMarginRatio;
        if (maxMarginRatio <= 0) maxMarginRatio = config.PositionSize > 0 && config.PositionSize <= 1 ? config.PositionSize : 1;
        return config with
        {
            MaxMarginRatio = Math.Clamp(maxMarginRatio, 0, 1),
            PositionSize = Math.Max(config.PositionSize, 0),
            MinExpectedEdge = Math.Max(config.MinExpectedEdge, 0),
            MinOrderDelta = Math.Clamp(config.MinOrderDelta, 0, 1),
            MinPositionSizeRatio = Math.Clamp(config.MinPositionSizeRatio, 0, 1),
            MakerFeeRate = Math.Max(config.MakerFeeRate, 0),
            TakerFeeRate = Math.Max(config.TakerFeeRate, 0),
            MinLeverage = Math.Max(config.MinLeverage, 0),
            MaxLeverage = Math.Max(config.MaxLeverage, 0),
            AvailableMarginBuffer = Math.Clamp(config.AvailableMarginBuffer, 0, 0.95),
            ExecutableMarginBuffer = Math.Clamp(config.ExecutableMarginBuffer, 0, 0.05),
            Instruments = config.Instruments.ToDictionary(item => item.Key, item => Normalize(item.Value))
        };
    }
    private static InstrumentConfig Normalize(InstrumentConfig config) => config with
    {
        MakerFeeRate = config.MakerFeeRate is null ? null : Math.Max(config.MakerFeeRate.Value, 0),
        TakerFeeRate = config.TakerFeeRate is null ? null : Math.Max(config.TakerFeeRate.Value, 0),
        MinLeverage = config.MinLeverage is null ? null : Math.Max(config.MinLeverage.Value, 0),
        MaxLeverage = config.MaxLeverage is null ? null : Math.Max(config.MaxLeverage.Value, 0),
        TrailingStopActivation = config.TrailingStopActivation is null ? null : Math.Max(config.TrailingStopActivation.Value, 0),
        TrailingStopDistance = config.TrailingStopDistance is null ? null : Math.Max(config.TrailingStopDistance.Value, 0),
        TrailingStopMinProfit = config.TrailingStopMinProfit is null ? null : Math.Max(config.TrailingStopMinProfit.Value, 0)
    };
    private static string Key(string venue, string instrument) => $"{venue}:{instrument}";
    private static double Clamp01(double value) => Math.Clamp(value, 0, 1);
    private static double SideSign(Side side) => side == Side.Buy ? 1 : -1;
    private static double Sign(double value) => value < 0 ? -1 : value > 0 ? 1 : 0;
    private static double PositiveOr(double a, double b, double c = 0) => a > 0 ? a : b > 0 ? b : Math.Max(c, 0);
    private static double RoundDownToStep(double value, double step) => value <= 0 || step <= 0 ? value : Math.Floor(value / step) * step;
    private static double RoundToTick(double value, double tick) => value <= 0 || tick <= 0 ? value : Math.Round(value / tick) * tick;
    private static double ExpectedEdge(Signal signal) => Clamp01(signal.Confidence) * Math.Max(signal.TakeProfit, 0) - (1 - Clamp01(signal.Confidence)) * Math.Max(signal.StopLoss, 0);
    private static double FeeAdjustedExpectedEdge(Signal signal, double takerFeeRate) => ExpectedEdge(signal) - 2 * takerFeeRate;
    private static string ExitReason(Position position, double price) => price <= 0 ? "" : position.TakeProfitTriggered(price) ? "take_profit" : position.StopLossTriggered(price) ? "stop_loss" : position.TrailingStopTriggered() ? "trailing_stop" : "";
    private static double OrderBudgetCost(Order order) => Math.Max(0, order.Margin) + Math.Max(0, order.EstimatedFee);
    private static double FeeValueForNotional(double notional, double feeRate) => notional <= 0 || feeRate <= 0 ? 0 : notional * feeRate;
    private static double InstrumentContractNotional(double price, InstrumentMetadata metadata) => price <= 0 ? 0 : price * PositiveOr(metadata.ContractValue, 1) * PositiveOr(metadata.ContractMultiplier, 1);
    private static double RatioOrZero(double numerator, double denominator) => denominator > 0 ? numerator / denominator : 0;
    private static string OrderReason(Position position, double targetSize) => Math.Abs(position.Size) <= 1e-9 ? "opening" : Math.Abs(targetSize) <= 1e-9 ? "closing" : Sign(position.Size) != Sign(targetSize) ? "flip" : "rebalance";
    private static bool IsFlipTarget(double previousSize, double targetSize) => Math.Abs(previousSize) > 1e-9 && Math.Abs(targetSize) > 1e-9 && Sign(previousSize) != Sign(targetSize);
    private static double ManagePositionsOnlyTargetSize(double previousSize, double targetSize) => Math.Abs(previousSize) <= 1e-9 ? 0 : Math.Abs(targetSize) <= 1e-9 ? 0 : Sign(previousSize) != Sign(targetSize) ? 0 : Math.Abs(targetSize) > Math.Abs(previousSize) ? previousSize : targetSize;
    private static bool IsExposureReduction(double previousSize, double targetSize) => Math.Abs(previousSize) > 1e-9 && (Math.Abs(targetSize) <= 1e-9 || Sign(previousSize) != Sign(targetSize) || Math.Abs(targetSize) < Math.Abs(previousSize) - 1e-9);
    private static double BlendRisk(double current, double incoming, double gate) => current <= 0 ? incoming : incoming <= 0 ? current : current * (1 - Clamp01(gate)) + incoming * Clamp01(gate);

    private readonly record struct SignalContext(double Confidence, double Score, double ExpectedEdge, double TakeProfit, double StopLoss, double TrailingStopActivation, double TrailingStopDistance, double TrailingStopMinProfit, bool ManagePositionsOnly);
    private readonly record struct ExecutableAllocation(double Quantity, double Margin, double Fee);
    private readonly record struct RebalanceCandidate(string Key, Position Position, double Delta, double Weight, SignalContext Context, string Reason);
}
