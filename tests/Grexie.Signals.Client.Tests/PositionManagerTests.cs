using Xunit;

namespace Grexie.Signals.Client.Tests;

public sealed class PositionManagerTests
{
    [Fact]
    public void OpensAndFlips()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            PositionSize = 0.10,
            MinExpectedEdge = 0,
            MinOrderDelta = 0.20,
            MaxLeverage = 5
        });

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
        Assert.Equal(0.10, buy[0].TargetSize, 9);

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
        Assert.True(sell[0].SizeDelta < -0.19);
    }

    [Fact]
    public void ScalesMinOrderDeltaByPositionSize()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            PositionSize = 0.10,
            MinExpectedEdge = 0,
            MinOrderDelta = 0.20
        });

        Assert.Empty(manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "DOGE-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 0.15,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 0.2
        }));

        Assert.Single(manager.HandleSignal(new Signal
        {
            Venue = "okx",
            Instrument = "DOGE-USDT-SWAP",
            Side = Side.Buy,
            Confidence = 0.25,
            TakeProfit = 0.02,
            StopLoss = 0.004,
            Price = 0.2
        }));
    }

    [Fact]
    public void CreatesConcreteOrdersWithAssetAndInstrumentMetadata()
    {
        var manager = new PositionManager(config: PositionManagerConfig.ProductionDefaults() with
        {
            PositionSize = 0.10,
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
            PositionSize = 0.01,
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
}
