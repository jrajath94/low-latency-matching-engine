"""Tests for the low-latency matching engine.

Covers limit orders, market orders, IOC/FOK order types, order book
management, price-time priority, cancel/modify, and edge cases.
"""
import pytest

from low_latency_matching_engine.matching import (
    MatchingEngine,
    Order,
    OrderBook,
    OrderStatus,
    OrderType,
    PriceLevel,
    Side,
    Trade,
)


# --- Fixtures ---

@pytest.fixture
def engine() -> MatchingEngine:
    """A fresh matching engine instance."""
    return MatchingEngine()


@pytest.fixture
def engine_with_book(engine: MatchingEngine) -> MatchingEngine:
    """Engine pre-loaded with resting buy and sell orders."""
    # Bids: 100.0 (100), 99.0 (200)
    engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 100.0, 100.0)
    engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 200.0, 99.0)
    # Asks: 101.0 (150), 102.0 (300)
    engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 150.0, 101.0)
    engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 300.0, 102.0)
    return engine


# --- Order Creation Tests ---

class TestOrderSubmission:
    """Tests for basic order submission."""

    def test_submit_limit_buy(self, engine: MatchingEngine) -> None:
        """Submitting a limit buy creates an order with correct fields."""
        order = engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 100.0, 150.0)
        assert order.order_id == 1
        assert order.side == Side.BUY
        assert order.quantity == 100.0
        assert order.price == 150.0

    def test_submit_limit_sell(self, engine: MatchingEngine) -> None:
        """Submitting a limit sell creates a resting order."""
        order = engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 50.0, 200.0)
        assert order.side == Side.SELL
        assert order.status == OrderStatus.NEW

    def test_order_ids_increment(self, engine: MatchingEngine) -> None:
        """Each order gets a unique incrementing ID."""
        o1 = engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 10.0, 100.0)
        o2 = engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 10.0, 200.0)
        assert o2.order_id == o1.order_id + 1

    def test_invalid_quantity_raises(self, engine: MatchingEngine) -> None:
        """Zero or negative quantity raises ValueError."""
        with pytest.raises(ValueError, match="positive"):
            engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 0, 100.0)

    def test_invalid_price_for_limit_raises(
        self, engine: MatchingEngine
    ) -> None:
        """Zero price on a limit order raises ValueError."""
        with pytest.raises(ValueError, match="positive"):
            engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 100.0, 0.0)

    @pytest.mark.parametrize("qty", [-1.0, -100.0, 0.0])
    def test_invalid_quantities_parametrized(
        self, engine: MatchingEngine, qty: float
    ) -> None:
        """Various invalid quantities all raise ValueError."""
        with pytest.raises(ValueError):
            engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, qty, 100.0)


# --- Matching Tests ---

class TestMatching:
    """Tests for order matching logic."""

    def test_crossing_orders_produce_trade(
        self, engine: MatchingEngine
    ) -> None:
        """A buy at 100 matched against a sell at 100 produces a trade."""
        engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 50.0, 100.0)
        engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 50.0, 100.0)
        assert engine.trade_count == 1
        trade = engine.trades[0]
        assert trade.quantity == 50.0
        assert trade.price == 100.0

    def test_partial_fill(self, engine: MatchingEngine) -> None:
        """Larger aggressor partially fills against smaller resting order."""
        sell = engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 50.0, 100.0)
        buy = engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 100.0, 100.0)
        assert engine.trade_count == 1
        assert buy.status == OrderStatus.PARTIALLY_FILLED
        assert buy.remaining_quantity == 50.0
        assert sell.status == OrderStatus.FILLED

    def test_price_time_priority(self, engine: MatchingEngine) -> None:
        """Earlier order at same price gets filled first (time priority)."""
        s1 = engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 50.0, 100.0)
        s2 = engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 50.0, 100.0)
        engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 50.0, 100.0)
        # First sell should be filled, second should still be resting
        assert s1.status == OrderStatus.FILLED
        assert s2.status == OrderStatus.NEW

    def test_price_priority_best_price_first(
        self, engine: MatchingEngine
    ) -> None:
        """Buy aggressor matches against lowest ask first."""
        engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 50.0, 102.0)
        engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 50.0, 101.0)
        engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 50.0, 102.0)
        # Should match at 101.0 (best ask) not 102.0
        assert engine.trade_count == 1
        assert engine.trades[0].price == 101.0

    def test_no_match_when_prices_dont_cross(
        self, engine: MatchingEngine
    ) -> None:
        """Buy at 99 vs sell at 100 should not match."""
        engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 50.0, 100.0)
        engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 50.0, 99.0)
        assert engine.trade_count == 0

    def test_multiple_fills_sweep(self, engine: MatchingEngine) -> None:
        """Large aggressor sweeps through multiple price levels."""
        engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 50.0, 100.0)
        engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 50.0, 101.0)
        buy = engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 100.0, 101.0)
        assert engine.trade_count == 2
        assert buy.status == OrderStatus.FILLED


# --- Market Order Tests ---

class TestMarketOrders:
    """Tests for market order execution."""

    def test_market_buy_fills_against_asks(
        self, engine_with_book: MatchingEngine
    ) -> None:
        """Market buy sweeps available asks."""
        order = engine_with_book.submit_order(
            "AAPL", Side.BUY, OrderType.MARKET, 150.0
        )
        assert order.status == OrderStatus.FILLED
        assert engine_with_book.trade_count == 1

    def test_market_order_no_liquidity_cancelled(
        self, engine: MatchingEngine
    ) -> None:
        """Market order with no opposite side gets cancelled."""
        order = engine.submit_order("AAPL", Side.BUY, OrderType.MARKET, 100.0)
        assert order.status == OrderStatus.CANCELLED

    def test_market_sell_fills_against_bids(
        self, engine_with_book: MatchingEngine
    ) -> None:
        """Market sell sweeps available bids."""
        order = engine_with_book.submit_order(
            "AAPL", Side.SELL, OrderType.MARKET, 100.0
        )
        assert order.status == OrderStatus.FILLED


# --- IOC Order Tests ---

class TestIOCOrders:
    """Tests for Immediate-or-Cancel orders."""

    def test_ioc_full_fill(self, engine_with_book: MatchingEngine) -> None:
        """IOC order fully fills when liquidity is available."""
        order = engine_with_book.submit_order(
            "AAPL", Side.BUY, OrderType.IOC, 100.0, 101.0
        )
        assert order.status == OrderStatus.FILLED

    def test_ioc_partial_fill_then_cancel(
        self, engine_with_book: MatchingEngine
    ) -> None:
        """IOC order partially fills then remainder is cancelled."""
        order = engine_with_book.submit_order(
            "AAPL", Side.BUY, OrderType.IOC, 200.0, 101.0
        )
        # Only 150 available at 101, rest cancelled
        assert order.filled_quantity == 150.0
        assert order.status == OrderStatus.CANCELLED

    def test_ioc_no_match_cancelled(
        self, engine_with_book: MatchingEngine
    ) -> None:
        """IOC order with no matching price is cancelled immediately."""
        order = engine_with_book.submit_order(
            "AAPL", Side.BUY, OrderType.IOC, 100.0, 90.0
        )
        assert order.status == OrderStatus.CANCELLED
        assert order.filled_quantity == 0.0


# --- FOK Order Tests ---

class TestFOKOrders:
    """Tests for Fill-or-Kill orders."""

    def test_fok_full_fill(self, engine_with_book: MatchingEngine) -> None:
        """FOK order fills when full quantity is available."""
        order = engine_with_book.submit_order(
            "AAPL", Side.BUY, OrderType.FOK, 150.0, 101.0
        )
        assert order.status == OrderStatus.FILLED

    def test_fok_rejected_insufficient_quantity(
        self, engine_with_book: MatchingEngine
    ) -> None:
        """FOK order rejected when full quantity not available."""
        order = engine_with_book.submit_order(
            "AAPL", Side.BUY, OrderType.FOK, 500.0, 101.0
        )
        assert order.status == OrderStatus.REJECTED
        assert order.filled_quantity == 0.0


# --- Cancel and Modify Tests ---

class TestCancelModify:
    """Tests for order cancellation and modification."""

    def test_cancel_resting_order(
        self, engine_with_book: MatchingEngine
    ) -> None:
        """Cancelling a resting order changes its status."""
        order = engine_with_book.cancel_order(1)
        assert order.status == OrderStatus.CANCELLED

    def test_cancel_nonexistent_raises(
        self, engine: MatchingEngine
    ) -> None:
        """Cancelling unknown order_id raises KeyError."""
        with pytest.raises(KeyError, match="not found"):
            engine.cancel_order(999)

    def test_cancel_filled_order_raises(
        self, engine: MatchingEngine
    ) -> None:
        """Cancelling a filled order raises ValueError."""
        engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 50.0, 100.0)
        engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 50.0, 100.0)
        # Order 1 (sell) should be filled
        with pytest.raises(ValueError, match="Cannot cancel"):
            engine.cancel_order(1)

    def test_modify_price(self, engine_with_book: MatchingEngine) -> None:
        """Modifying price on a resting order works."""
        order = engine_with_book.modify_order(1, new_price=98.0)
        assert order.price == 98.0

    def test_modify_quantity(self, engine_with_book: MatchingEngine) -> None:
        """Modifying quantity on a resting order works."""
        order = engine_with_book.modify_order(1, new_quantity=50.0)
        assert order.quantity == 50.0

    def test_modify_filled_raises(self, engine: MatchingEngine) -> None:
        """Modifying a filled order raises ValueError."""
        engine.submit_order("AAPL", Side.SELL, OrderType.LIMIT, 50.0, 100.0)
        engine.submit_order("AAPL", Side.BUY, OrderType.LIMIT, 50.0, 100.0)
        with pytest.raises(ValueError, match="Cannot modify"):
            engine.modify_order(1, new_price=99.0)

    def test_modify_nonexistent_raises(
        self, engine: MatchingEngine
    ) -> None:
        """Modifying unknown order_id raises KeyError."""
        with pytest.raises(KeyError, match="not found"):
            engine.modify_order(999, new_price=100.0)


# --- OrderBook Tests ---

class TestOrderBook:
    """Tests for order book state."""

    def test_best_bid_ask(self, engine_with_book: MatchingEngine) -> None:
        """Best bid and ask reflect top-of-book prices."""
        book = engine_with_book.get_book("AAPL")
        assert book is not None
        assert book.best_bid == 100.0
        assert book.best_ask == 101.0

    def test_spread(self, engine_with_book: MatchingEngine) -> None:
        """Spread is ask - bid."""
        book = engine_with_book.get_book("AAPL")
        assert book is not None
        assert book.spread == pytest.approx(1.0)

    def test_empty_book_spread_is_none(
        self, engine: MatchingEngine
    ) -> None:
        """Empty book has None spread."""
        book = OrderBook("TEST")
        assert book.spread is None

    def test_depth(self, engine_with_book: MatchingEngine) -> None:
        """Bid and ask depths reflect price level counts."""
        book = engine_with_book.get_book("AAPL")
        assert book is not None
        assert book.bid_depth == 2
        assert book.ask_depth == 2

    def test_get_bids_returns_sorted(
        self, engine_with_book: MatchingEngine
    ) -> None:
        """get_bids returns levels sorted best-first (descending)."""
        book = engine_with_book.get_book("AAPL")
        assert book is not None
        bids = book.get_bids()
        prices = [b[0] for b in bids]
        assert prices == sorted(prices, reverse=True)

    def test_get_asks_returns_sorted(
        self, engine_with_book: MatchingEngine
    ) -> None:
        """get_asks returns levels sorted best-first (ascending)."""
        book = engine_with_book.get_book("AAPL")
        assert book is not None
        asks = book.get_asks()
        prices = [a[0] for a in asks]
        assert prices == sorted(prices)
