using Xunit;

namespace Grexie.Signals.Client.Tests;

public sealed class PositionManagerTests
{
    [Fact]
    public void OpensAndFlips()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 0.10,
            MinExpectedEdge = 0,
            MinOrderDelta = 0.20,
            MaxLeverage = 5
        });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "BTC-USDT-SWAP" });

        var buy = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 0.8,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Score = 0.5,
            Price = 100
        });

        Assert.Single(buy);
        Assert.Equal("opening", buy[0].Reason);
        Assert.Equal(Side.Buy, buy[0].Side);
        Assert.Equal(0.10, OrderBudgetCost(buy[0]), 9);

        var sell = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Sell,
            Confidence = 0.9,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Score = -0.6,
            Price = 99
        });

        Assert.Single(sell);
        Assert.Equal(Side.Sell, sell[0].Side);
        Assert.Equal("flip", sell[0].Reason);
        Assert.Equal(0, sell[0].TargetSize, 9);
        Assert.Equal(-buy[0].TargetSize, sell[0].SizeDelta, 9);

        var openShort = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Sell,
            Confidence = 0.9,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Score = -0.6,
            Price = 99
        });
        Assert.Single(openShort);
        Assert.Equal(Side.Sell, openShort[0].Side);
        Assert.Equal("opening", openShort[0].Reason);
    }

    [Fact]
    public void ConfidenceIsAllocationWeight()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 0.10,
            MinExpectedEdge = 0,
            MinOrderDelta = 0.20
        });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "DOGE-USDT-SWAP" });

        var orders = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "DOGE-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 0.15,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 0.2
        });

        Assert.Single(orders);
        Assert.Equal(0.10, OrderBudgetCost(orders[0]), 9);
    }

    [Fact]
    public void QuantizesEmittedTargetSizeToExecutableLots()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 0.50,
            MinExpectedEdge = 0,
            MinOrderDelta = 0
        });
        manager.AssetManager.UpdateAsset(new AssetSnapshot { Currency = "USDT", Equity = 1000, Available = 1000 });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            SettlementCurrency = "USDT",
            LotSize = 1,
            MinSize = 1,
            TickSize = 0.1
        });

        var orders = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 0.15,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 333
        });

        Assert.Single(orders);
        Assert.Equal(1, orders[0].Quantity, 9);
        Assert.Equal(1, orders[0].SizeDelta, 9);
        Assert.Equal(1, orders[0].TargetSize, 9);
    }

    [Fact]
    public void IgnoresUnconfiguredSignals()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 0.10,
            MinExpectedEdge = 0,
            MinOrderDelta = 0
        });

        var signal = new Signal
        {
            Venue = "okx",
            Instrument = "SOL-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 1,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 100
        };
        Assert.Empty(manager.HandleSignal(signal));
        Assert.Empty(manager.Positions());

        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "SOL-USDT-SWAP" });
        Assert.Single(manager.HandleSignal(signal));
    }

    [Fact]
    public void IgnoresReplaySignalEvents()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 0.10,
            MinExpectedEdge = 0,
            MinOrderDelta = 0
        });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "BTC-USDT-SWAP" });
        var signal = new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 1,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 100
        };
        var replay = new SignalEvent(3, "okx", "BTC-USDT-SWAP", signal, null, true, null);
        Assert.Empty(manager.HandleEvent(replay));
        Assert.Empty(manager.Positions());

        var live = replay with { Replay = false };
        Assert.Single(manager.HandleEvent(live));
    }

    [Fact]
    public void LeverageAdaptsWithConfidenceEdgeAndScoreInsideCaps()
    {
        static double LeverageFor(string instrument, double confidence, double takeProfit, double score)
        {
            var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
            {
                MaxMarginRatio = 1,
                MinExpectedEdge = 0,
                MinOrderDelta = 0,
                MinLeverage = 1,
                MaxLeverage = 5
            });
            manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = instrument });
            var orders = manager.HandleSignal(new Signal
            {
                Venue = "okx",
                Instrument = instrument,
                Side = Side.Buy,
                Confidence = confidence,
                TakeProfit = takeProfit,
                StopLoss = 0,
                Score = score,
                Price = 100
            });
            return orders[0].Leverage;
        }

        var low = LeverageFor("LOW-USDT-SWAP", 0.2, 0, 0);
        var scored = LeverageFor("SCORE-USDT-SWAP", 0.2, 0, 1);
        var high = LeverageFor("HIGH-USDT-SWAP", 1, 0.02, 1);
        Assert.True(low >= 1);
        Assert.True(high <= 5);
        Assert.True(scored > low);
        Assert.Equal(5, high, 9);
    }

    [Fact]
    public void UpdateConfigKeepsStateAndChangesLeverage()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 1,
            MinExpectedEdge = 0,
            MinOrderDelta = 0,
            RebalanceInterval = TimeSpan.FromHours(1),
            MinLeverage = 5,
            MaxLeverage = 5
        });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "BTC-USDT-SWAP" });
        var opening = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 1,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Score = 1,
            Price = 100
        });
        Assert.Equal(5, opening[0].Leverage, 9);

        manager.UpdateConfig(PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 1,
            MinExpectedEdge = 0,
            MinOrderDelta = 0,
            RebalanceInterval = TimeSpan.FromHours(1),
            MinLeverage = 1,
            MaxLeverage = 1
        });
        Assert.Single(manager.Positions());
        var closing = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Sell,
            Confidence = 1,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Score = -1,
            Price = 99
        });
        Assert.True(closing[0].ReduceOnly);
        Assert.Equal(1, closing[0].Leverage, 9);
    }

    [Fact]
    public void CreatesConcreteOrdersWithAssetAndInstrumentMetadata()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 0.10,
            MinExpectedEdge = 0,
            MinOrderDelta = 0,
            MaxLeverage = 5
        });
        manager.AssetManager.UpdateAsset(new AssetSnapshot { Currency = "USDT", Cash = 1000, Available = 900, Used = 100, Equity = 1000 });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            SettlementCurrency = "USDT",
            LotSize = 0.001,
            MinSize = 0.002,
            TickSize = 0.1,
            MaxLeverage = 2
        });

        var orders = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 1,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 100.07
        });

        Assert.Single(orders);
        Assert.Equal(100.1, orders[0].Price, 9);
        Assert.Equal("USDT", orders[0].SettlementCurrency);
        Assert.True(orders[0].Leverage <= 2);
        Assert.True(orders[0].Quantity > 0);
        Assert.True(orders[0].Notional > 0);
        Assert.True(orders[0].EstimatedFeeValue > 0);
    }

    [Fact]
    public void RejectsBelowInstrumentMinSizeAndReportsStats()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 0.01,
            MinExpectedEdge = 0,
            MinOrderDelta = 0
        });
        manager.AssetManager.UpdateAsset(new AssetSnapshot { Currency = "USDT", Equity = 10 });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            SettlementCurrency = "USDT",
            LotSize = 0.001,
            MinSize = 1,
            TickSize = 0.1
        });

        Assert.Empty(manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 1,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 100
        }));

        manager.AssetManager.UpdateAsset(new AssetSnapshot { Currency = "USDT", Cash = 1000, Available = 800, Used = 200, Equity = 1000 });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata
        {
            Venue = "okx",
            Instrument = "ETH-USDT-SWAP",
            SettlementCurrency = "USDT",
            LotSize = 0.01,
            MinSize = 0.01,
            TickSize = 0.01
        });
        manager.AddPosition(new Position
        {
            Venue = "okx",
            Instrument = "ETH-USDT-SWAP",
            Size = 0.10,
            Confidence = 0.8,
            EntryPrice = 100,
            LastPrice = 110,
            Leverage = 2,
            RealizedPnL = 0.01,
            Fees = 0.001
        });

        var stats = manager.Stats();
        Assert.Equal(1000, stats.Equity);
        Assert.Equal(800, stats.Available);
        Assert.Equal("USDT", stats.ByInstrument["okx:ETH-USDT-SWAP"].SettlementCurrency);
        Assert.True(stats.ByInstrument["okx:ETH-USDT-SWAP"].Quantity > 0);
        Assert.True(stats.TotalPnLPercent > 0);
    }

    [Fact]
    public void PhasesReductionsBeforeOpenings()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 0.20,
            MinExpectedEdge = 0,
            MinOrderDelta = 0
        });
        manager.AssetManager.UpdateAsset(new AssetSnapshot { Currency = "USDT", Cash = 1000, Available = 1000, Equity = 1000 });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "BTC-USDT-SWAP", SettlementCurrency = "USDT" });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "ETH-USDT-SWAP", SettlementCurrency = "USDT" });
        manager.AddPosition(new Position
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Size = 2,
            Confidence = 1,
            EntryPrice = 100,
            LastPrice = 100
        });

        var reductions = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "ETH-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 1,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 100
        });

        Assert.Single(reductions);
        Assert.Equal("BTC-USDT-SWAP", reductions[0].Instrument);
        Assert.Equal(Side.Sell, reductions[0].Side);
        Assert.Equal((100 / (1 + reductions[0].Leverage * reductions[0].FeeRate)) / reductions[0].Price, reductions[0].TargetSize, 9);

        var openings = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "ETH-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 1,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 100
        });

        Assert.Single(openings);
        Assert.Equal("ETH-USDT-SWAP", openings[0].Instrument);
        Assert.Equal(Side.Buy, openings[0].Side);
    }

    [Fact]
    public void CapsOpeningsToAvailableExposure()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 0.20,
            MinExpectedEdge = 0,
            MinOrderDelta = 0
        });
        manager.AssetManager.UpdateAsset(new AssetSnapshot { Currency = "USDT", Cash = 1000, Available = 50, Equity = 1000 });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "BTC-USDT-SWAP", SettlementCurrency = "USDT" });

        var orders = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 1,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 100
        });

        Assert.Single(orders);
        Assert.True(OrderBudgetCost(orders[0]) <= 50 + 1e-9);
        Assert.True(orders[0].Margin < 50);
    }

    [Fact]
    public void TrailingStopClosesAfterFavorableGiveback()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 1,
            MinExpectedEdge = 0,
            MinOrderDelta = 0
        });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "BTC-USDT-SWAP" });

        var orders = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 1,
            TakeProfit = 0.50,
            StopLoss = 0.20,
            TrailingStopActivation = 0.02,
            TrailingStopDistance = 0.01,
            TrailingStopMinProfit = 0.001,
            Price = 100
        });

        var open = Assert.Single(orders);
        Assert.Equal(0.02, open.TrailingStopActivation, 9);
        Assert.Empty(manager.UpdatePrice("okx", "BTC-USDT-SWAP", 103));

        var close = Assert.Single(manager.UpdatePrice("okx", "BTC-USDT-SWAP", 101.8));
        Assert.Equal("trailing_stop", close.Reason);
        Assert.Empty(manager.Positions());
        var closed = Assert.Single(manager.ClosedTrades());
        Assert.Equal("trailing_stop", closed.ExitReason);
        Assert.True(closed.MFE >= 0.03 - 1e-9);
        Assert.True(closed.RealizedPnL > 0);
    }

    [Fact]
    public void PersistsAndHydratesTrailingStopState()
    {
        var snapshots = new List<PositionManagerState>();
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 1,
            MinExpectedEdge = 0,
            MinOrderDelta = 0,
            Persist = snapshots.Add
        });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "BTC-USDT-SWAP" });
        manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 1,
            TakeProfit = 0.50,
            StopLoss = 0.20,
            TrailingStopActivation = 0.02,
            TrailingStopDistance = 0.01,
            TrailingStopMinProfit = 0.001,
            Price = 100
        });
        manager.UpdatePrice("okx", "BTC-USDT-SWAP", 104);

        var latest = snapshots.Last();
        var position = Assert.Single(latest.Positions);
        Assert.Equal(0.02, position.TrailingStopActivation, 9);
        Assert.True(position.MFE > 0.039);

        var rehydrated = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with { InitialState = latest });
        Assert.Equal(position.MFE, Assert.Single(rehydrated.Positions()).MFE);
    }

    [Fact]
    public void TrailingActivationIsAtLeastBreakeven()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 1,
            MinExpectedEdge = 0,
            MinOrderDelta = 0,
            TakerFeeRate = 0.0005,
            Instruments = new Dictionary<string, InstrumentConfig>
            {
                ["okx:BTC-USDT-SWAP"] = new()
                {
                    TrailingStopActivation = 0.0001,
                    TrailingStopDistance = 0.01
                }
            }
        });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "BTC-USDT-SWAP" });

        var order = Assert.Single(manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 1,
            TakeProfit = 0.50,
            StopLoss = 0.20,
            Price = 100
        }));

        Assert.Equal(0.001, order.TrailingStopMinProfit, 9);
        Assert.Equal(0.002, order.TrailingStopActivation, 9);
    }

    [Fact]
    public void CapsOpeningsToRemainingPortfolioBudgetWithoutAssetSnapshots()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 1,
            MinExpectedEdge = 0,
            MinOrderDelta = 0,
            RebalanceInterval = TimeSpan.FromHours(6),
            MinLeverage = 1,
            MaxLeverage = 1
        });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "BTC-USDT-SWAP", SettlementCurrency = "USDT" });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "ETH-USDT-SWAP", SettlementCurrency = "USDT" });

        var first = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "BTC-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 0.51,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 100,
            Timestamp = DateTimeOffset.Parse("2026-05-27T00:00:00Z")
        });
        Assert.Single(first);

        manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "ETH-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 0.51,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 100,
            Timestamp = DateTimeOffset.Parse("2026-05-27T00:01:00Z")
        });

        var total = manager.Positions().Sum(position => Math.Abs(position.Size));
        Assert.True(total <= 0.01 + 1e-9, $"total={total}");
    }

    [Fact]
    public void ClosesPositionBelowMinimumPositionSizeRatio()
    {
        var lastSignalAt = DateTimeOffset.UtcNow.AddMinutes(-1);
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            MaxMarginRatio = 1,
            MinPositionSizeRatio = 0.01,
            MinExpectedEdge = 0,
            MinOrderDelta = 0,
            RebalanceInterval = TimeSpan.FromHours(6)
        });
        manager.AssetManager.UpdateAsset(new AssetSnapshot { Currency = "USDT", Cash = 1000, Available = 0.5, Used = 999.5, Equity = 1000 });
        manager.InstrumentManager.UpdateInstrument(new InstrumentMetadata { Venue = "okx", Instrument = "DUST-USDT-SWAP", SettlementCurrency = "USDT", LotSize = 0.1, MinSize = 0.1 });
        manager.AddPosition(new Position
        {
            Venue = "okx",
            Instrument = "DUST-USDT-SWAP",
            Size = 0.005,
            Confidence = 0.5,
            EntryPrice = 100,
            LastPrice = 100,
            LastSignalAt = lastSignalAt
        });

        var orders = manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "DUST-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 1,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 100,
            Timestamp = lastSignalAt.AddMinutes(1)
        });

        var order = Assert.Single(orders);
        Assert.Equal(Side.Sell, order.Side);
        Assert.Equal("closing", order.Reason);
        Assert.Equal(0, order.TargetSize, 9);
        Assert.Equal(-0.005, order.SizeDelta, 9);
        Assert.Equal(0.005, order.Quantity, 9);
    }

    private static double OrderBudgetCost(Order order) => Math.Max(0, order.Margin) + Math.Max(0, order.EstimatedFee);
}
