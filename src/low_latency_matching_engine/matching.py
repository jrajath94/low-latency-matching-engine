"""Low-latency order matching engine with price-time priority.

Implements a full limit order book with support for limit orders,
market orders, IOC (Immediate-or-Cancel), FOK (Fill-or-Kill),
cancel, and modify operations. Uses sorted data structures for
efficient price-level management.
"""
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)

# --- Constants ---
PRICE_PRECISION: int = 8
QUANTITY_PRECISION: int = 8
MAX_ORDER_ID: int = 2**63 - 1


class Side(Enum):
    """Order side."""
    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    """Supported order types."""
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    IOC = "IOC"        # Immediate-or-Cancel
    FOK = "FOK"        # Fill-or-Kill


class OrderStatus(Enum):
    """Lifecycle status of an order."""
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


@dataclass
class Order:
    """Represents an order submitted to the matching engine.

    Args:
        order_id: Unique identifier.
        symbol: Trading pair or instrument.
        side: BUY or SELL.
        order_type: LIMIT, MARKET, IOC, or FOK.
        price: Limit price (0.0 for market orders).
        quantity: Original order quantity.
        remaining_quantity: Unfilled quantity (starts equal to quantity).
        status: Current order status.
        timestamp_ns: Submission time in nanoseconds.
    """
    order_id: int
    symbol: str
    side: Side
    order_type: OrderType
    price: float
    quantity: float
    remaining_quantity: float = 0.0
    status: OrderStatus = OrderStatus.NEW
    timestamp_ns: int = 0

    def __post_init__(self) -> None:
        """Set remaining_quantity and timestamp if not provided."""
        if self.remaining_quantity == 0.0:
            self.remaining_quantity = self.quantity
        if self.timestamp_ns == 0:
            self.timestamp_ns = time.time_ns()

    @property
    def filled_quantity(self) -> float:
        """Quantity that has been filled."""
        return self.quantity - self.remaining_quantity

    @property
    def is_fully_filled(self) -> bool:
        """Whether the order is completely filled."""
        return self.remaining_quantity <= 0.0


@dataclass
class Trade:
    """Represents an executed trade between two orders.

    Args:
        trade_id: Unique trade identifier.
        symbol: Instrument traded.
        price: Execution price.
        quantity: Executed quantity.
        buy_order_id: ID of the buy-side order.
        sell_order_id: ID of the sell-side order.
        timestamp_ns: Execution time in nanoseconds.
    """
    trade_id: int
    symbol: str
    price: float
    quantity: float
    buy_order_id: int
    sell_order_id: int
    timestamp_ns: int = 0

    def __post_init__(self) -> None:
        """Set timestamp if not provided."""
        if self.timestamp_ns == 0:
            self.timestamp_ns = time.time_ns()


@dataclass
class PriceLevel:
    """A single price level in the order book.

    Orders at the same price are queued in time-priority (FIFO).

    Args:
        price: The price for this level.
        orders: FIFO queue of orders at this price.
    """
    price: float
    orders: deque[Order] = field(default_factory=deque)

    @property
    def total_quantity(self) -> float:
        """Sum of remaining quantities at this level."""
        return sum(o.remaining_quantity for o in self.orders)

    @property
    def order_count(self) -> int:
        """Number of resting orders at this level."""
        return len(self.orders)

    def add_order(self, order: Order) -> None:
        """Append an order to this price level's queue.

        Args:
            order: The order to add.
        """
        self.orders.append(order)

    def remove_order(self, order_id: int) -> Optional[Order]:
        """Remove a specific order from this level.

        Args:
            order_id: ID of the order to remove.

        Returns:
            The removed order, or None if not found.
        """
        for i, order in enumerate(self.orders):
            if order.order_id == order_id:
                del self.orders[i]
                return order
        return None


class OrderBook:
    """Manages buy and sell price levels for a single instrument.

    Buy side is sorted descending by price (best bid first).
    Sell side is sorted ascending by price (best ask first).

    Args:
        symbol: The instrument symbol.
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        # Sorted lists of PriceLevel — buy descending, sell ascending
        self._bids: list[PriceLevel] = []
        self._asks: list[PriceLevel] = []
        # Fast lookup: order_id -> (side, price)
        self._order_index: dict[int, tuple[Side, float]] = {}

    @property
    def best_bid(self) -> Optional[float]:
        """Highest bid price, or None if no bids."""
        if not self._bids:
            return None
        return self._bids[0].price

    @property
    def best_ask(self) -> Optional[float]:
        """Lowest ask price, or None if no asks."""
        if not self._asks:
            return None
        return self._asks[0].price

    @property
    def spread(self) -> Optional[float]:
        """Bid-ask spread, or None if either side is empty."""
        bid = self.best_bid
        ask = self.best_ask
        if bid is None or ask is None:
            return None
        return round(ask - bid, PRICE_PRECISION)

    @property
    def bid_depth(self) -> int:
        """Number of distinct bid price levels."""
        return len(self._bids)

    @property
    def ask_depth(self) -> int:
        """Number of distinct ask price levels."""
        return len(self._asks)

    def add_order(self, order: Order) -> None:
        """Add a resting order to the book.

        Args:
            order: The order to insert at the appropriate level.
        """
        levels = self._bids if order.side == Side.BUY else self._asks
        self._insert_into_levels(levels, order)
        self._order_index[order.order_id] = (order.side, order.price)

    def remove_order(self, order_id: int) -> Optional[Order]:
        """Remove an order from the book by ID.

        Args:
            order_id: The order to remove.

        Returns:
            The removed Order, or None if not found.
        """
        lookup = self._order_index.pop(order_id, None)
        if lookup is None:
            return None
        side, price = lookup
        levels = self._bids if side == Side.BUY else self._asks
        return self._remove_from_levels(levels, order_id, price)

    def get_bids(self, depth: int = 10) -> list[tuple[float, float]]:
        """Get top-of-book bid levels.

        Args:
            depth: Number of price levels to return.

        Returns:
            List of (price, total_quantity) tuples, best-first.
        """
        return [
            (lvl.price, lvl.total_quantity)
            for lvl in self._bids[:depth]
        ]

    def get_asks(self, depth: int = 10) -> list[tuple[float, float]]:
        """Get top-of-book ask levels.

        Args:
            depth: Number of price levels to return.

        Returns:
            List of (price, total_quantity) tuples, best-first.
        """
        return [
            (lvl.price, lvl.total_quantity)
            for lvl in self._asks[:depth]
        ]

    def _insert_into_levels(
        self,
        levels: list[PriceLevel],
        order: Order,
    ) -> None:
        """Insert order into the sorted level list.

        Args:
            levels: The bid or ask level list.
            order: Order to insert.
        """
        target_price = order.price
        is_buy = order.side == Side.BUY
        for i, level in enumerate(levels):
            if abs(level.price - target_price) < 1e-12:
                level.add_order(order)
                return
            if is_buy and level.price < target_price:
                new_level = PriceLevel(price=target_price)
                new_level.add_order(order)
                levels.insert(i, new_level)
                return
            if not is_buy and level.price > target_price:
                new_level = PriceLevel(price=target_price)
                new_level.add_order(order)
                levels.insert(i, new_level)
                return
        # Append at end
        new_level = PriceLevel(price=target_price)
        new_level.add_order(order)
        levels.append(new_level)

    def _remove_from_levels(
        self,
        levels: list[PriceLevel],
        order_id: int,
        price: float,
    ) -> Optional[Order]:
        """Remove an order from the appropriate price level.

        Args:
            levels: The bid or ask level list.
            order_id: ID of the order to remove.
            price: Price of the order (for fast level lookup).

        Returns:
            The removed order, or None.
        """
        for i, level in enumerate(levels):
            if abs(level.price - price) < 1e-12:
                order = level.remove_order(order_id)
                if level.order_count == 0:
                    levels.pop(i)
                return order
        return None

    def _clean_empty_top(self, side: Side) -> None:
        """Remove empty levels from the top of a side.

        Args:
            side: Which side to clean.
        """
        levels = self._bids if side == Side.BUY else self._asks
        while levels and levels[0].order_count == 0:
            levels.pop(0)


class MatchingEngine:
    """Core matching engine with price-time priority.

    Supports limit orders, market orders, IOC, and FOK.
    Maintains an order book per symbol and produces trades.

    Args:
        None — engine is stateless aside from its books.
    """

    def __init__(self) -> None:
        self._books: dict[str, OrderBook] = {}
        self._orders: dict[int, Order] = {}
        self._trades: list[Trade] = []
        self._next_order_id: int = 1
        self._next_trade_id: int = 1

    @property
    def trade_count(self) -> int:
        """Total number of executed trades."""
        return len(self._trades)

    @property
    def trades(self) -> list[Trade]:
        """Read-only list of all executed trades."""
        return list(self._trades)

    def submit_order(
        self,
        symbol: str,
        side: Side,
        order_type: OrderType,
        quantity: float,
        price: float = 0.0,
    ) -> Order:
        """Submit a new order to the engine.

        Args:
            symbol: Instrument symbol.
            side: BUY or SELL.
            order_type: LIMIT, MARKET, IOC, or FOK.
            quantity: Order quantity (must be positive).
            price: Limit price (required for LIMIT/IOC/FOK, ignored for MARKET).

        Returns:
            The submitted Order with its assigned ID and status.

        Raises:
            ValueError: If quantity is non-positive or price is invalid.
        """
        self._validate_order_params(order_type, quantity, price)
        order = self._create_order(symbol, side, order_type, quantity, price)
        self._orders[order.order_id] = order
        book = self._get_or_create_book(symbol)
        logger.info(
            "Order %d: %s %s %.4f @ %.4f (%s)",
            order.order_id, side.value, symbol,
            quantity, price, order_type.value,
        )
        self._process_order(order, book)
        return order

    def cancel_order(self, order_id: int) -> Order:
        """Cancel a resting order.

        Args:
            order_id: The ID of the order to cancel.

        Returns:
            The cancelled order.

        Raises:
            KeyError: If order_id not found.
            ValueError: If order is already filled or cancelled.
        """
        order = self._orders.get(order_id)
        if order is None:
            raise KeyError(f"Order not found: {order_id}")
        if order.status in (OrderStatus.FILLED, OrderStatus.CANCELLED):
            raise ValueError(
                f"Cannot cancel order {order_id}: status={order.status.value}"
            )
        book = self._books.get(order.symbol)
        if book is not None:
            book.remove_order(order_id)
        order.status = OrderStatus.CANCELLED
        logger.info("Cancelled order %d", order_id)
        return order

    def modify_order(
        self,
        order_id: int,
        new_quantity: Optional[float] = None,
        new_price: Optional[float] = None,
    ) -> Order:
        """Modify a resting order's quantity or price.

        Cancel-and-replace semantics: the order loses time priority
        if the price changes.

        Args:
            order_id: The order to modify.
            new_quantity: New quantity (must be >= filled quantity).
            new_price: New limit price.

        Returns:
            The modified (or replacement) order.

        Raises:
            KeyError: If order not found.
            ValueError: On invalid modifications.
        """
        order = self._orders.get(order_id)
        if order is None:
            raise KeyError(f"Order not found: {order_id}")
        self._validate_modify(order, new_quantity, new_price)
        # Cancel old order
        book = self._books.get(order.symbol)
        if book is not None:
            book.remove_order(order_id)
        # Determine new values
        qty = new_quantity if new_quantity is not None else order.quantity
        prc = new_price if new_price is not None else order.price
        remaining = qty - order.filled_quantity
        order.quantity = qty
        order.remaining_quantity = remaining
        order.price = prc
        order.timestamp_ns = time.time_ns()
        order.status = (
            OrderStatus.PARTIALLY_FILLED
            if order.filled_quantity > 0
            else OrderStatus.NEW
        )
        # Re-process into the book
        if book is not None:
            self._process_order(order, book)
        logger.info("Modified order %d", order_id)
        return order

    def get_order(self, order_id: int) -> Optional[Order]:
        """Look up an order by ID.

        Args:
            order_id: The order identifier.

        Returns:
            The Order if found, else None.
        """
        return self._orders.get(order_id)

    def get_book(self, symbol: str) -> Optional[OrderBook]:
        """Get the order book for a symbol.

        Args:
            symbol: Instrument symbol.

        Returns:
            OrderBook if it exists, else None.
        """
        return self._books.get(symbol)

    # --- Private methods ---

    def _validate_order_params(
        self,
        order_type: OrderType,
        quantity: float,
        price: float,
    ) -> None:
        """Validate order submission parameters.

        Args:
            order_type: The order type.
            quantity: Requested quantity.
            price: Requested price.

        Raises:
            ValueError: On invalid parameters.
        """
        if quantity <= 0:
            raise ValueError(f"Quantity must be positive, got {quantity}")
        if order_type != OrderType.MARKET and price <= 0:
            raise ValueError(
                f"Price must be positive for {order_type.value}, got {price}"
            )

    def _create_order(
        self,
        symbol: str,
        side: Side,
        order_type: OrderType,
        quantity: float,
        price: float,
    ) -> Order:
        """Create and register a new Order object.

        Args:
            symbol: Instrument symbol.
            side: BUY or SELL.
            order_type: Order type.
            quantity: Order quantity.
            price: Limit price.

        Returns:
            The newly created Order.
        """
        order_id = self._next_order_id
        self._next_order_id += 1
        return Order(
            order_id=order_id,
            symbol=symbol,
            side=side,
            order_type=order_type,
            price=price,
            quantity=quantity,
        )

    def _get_or_create_book(self, symbol: str) -> OrderBook:
        """Get existing book or create a new one.

        Args:
            symbol: Instrument symbol.

        Returns:
            The OrderBook for the symbol.
        """
        if symbol not in self._books:
            self._books[symbol] = OrderBook(symbol)
        return self._books[symbol]

    def _process_order(self, order: Order, book: OrderBook) -> None:
        """Route order to the appropriate handler.

        Args:
            order: The order to process.
            book: The order book for the instrument.
        """
        if order.order_type == OrderType.MARKET:
            self._match_market_order(order, book)
        elif order.order_type == OrderType.FOK:
            self._match_fok_order(order, book)
        elif order.order_type == OrderType.IOC:
            self._match_ioc_order(order, book)
        else:
            self._match_limit_order(order, book)

    def _match_limit_order(self, order: Order, book: OrderBook) -> None:
        """Match a limit order, then rest any remainder.

        Args:
            order: The limit order.
            book: The order book.
        """
        self._match_against_book(order, book)
        if not order.is_fully_filled:
            book.add_order(order)
            if order.filled_quantity > 0:
                order.status = OrderStatus.PARTIALLY_FILLED

    def _match_market_order(self, order: Order, book: OrderBook) -> None:
        """Match a market order — sweep available liquidity.

        Args:
            order: The market order.
            book: The order book.
        """
        self._match_against_book(order, book)
        if not order.is_fully_filled:
            order.status = OrderStatus.CANCELLED
            logger.warning(
                "Market order %d partially filled (no more liquidity)",
                order.order_id,
            )

    def _match_ioc_order(self, order: Order, book: OrderBook) -> None:
        """Match IOC order — fill what's available, cancel rest.

        Args:
            order: The IOC order.
            book: The order book.
        """
        self._match_against_book(order, book)
        if not order.is_fully_filled:
            order.status = OrderStatus.CANCELLED

    def _match_fok_order(self, order: Order, book: OrderBook) -> None:
        """Match FOK order — fill entirely or reject.

        Args:
            order: The FOK order.
            book: The order book.
        """
        available = self._check_available_quantity(order, book)
        if available < order.quantity:
            order.status = OrderStatus.REJECTED
            logger.info(
                "FOK order %d rejected: need %.4f, available %.4f",
                order.order_id, order.quantity, available,
            )
            return
        self._match_against_book(order, book)

    def _check_available_quantity(
        self,
        order: Order,
        book: OrderBook,
    ) -> float:
        """Check how much quantity is available for matching.

        Args:
            order: The incoming order.
            book: The order book.

        Returns:
            Total matchable quantity on the opposite side.
        """
        levels = (
            book._asks if order.side == Side.BUY else book._bids
        )
        available = 0.0
        for level in levels:
            if not self._price_matches(order, level.price):
                break
            available += level.total_quantity
            if available >= order.quantity:
                return available
        return available

    def _match_against_book(
        self,
        order: Order,
        book: OrderBook,
    ) -> None:
        """Execute matching logic against resting orders.

        Walks the opposite side of the book from best price,
        matching until the incoming order is filled or no
        more matchable levels remain.

        Args:
            order: The incoming (aggressive) order.
            book: The order book.
        """
        levels = (
            book._asks if order.side == Side.BUY else book._bids
        )
        while levels and order.remaining_quantity > 0:
            top_level = levels[0]
            if not self._price_matches(order, top_level.price):
                break
            self._match_at_level(order, top_level, book)
            if top_level.order_count == 0:
                levels.pop(0)

    def _price_matches(self, aggressor: Order, resting_price: float) -> bool:
        """Check if the aggressor order can trade at the resting price.

        Args:
            aggressor: The incoming order.
            resting_price: Price of the resting order level.

        Returns:
            True if prices are compatible for execution.
        """
        if aggressor.order_type == OrderType.MARKET:
            return True
        if aggressor.side == Side.BUY:
            return aggressor.price >= resting_price
        return aggressor.price <= resting_price

    def _match_at_level(
        self,
        aggressor: Order,
        level: PriceLevel,
        book: OrderBook,
    ) -> None:
        """Match against all orders at a single price level.

        Args:
            aggressor: The incoming order.
            level: The price level to match against.
            book: The order book (for index cleanup).
        """
        while level.orders and aggressor.remaining_quantity > 0:
            resting = level.orders[0]
            trade_qty = min(
                aggressor.remaining_quantity,
                resting.remaining_quantity,
            )
            trade = self._execute_trade(
                aggressor, resting, level.price, trade_qty
            )
            self._trades.append(trade)
            self._update_order_after_fill(aggressor, trade_qty)
            self._update_order_after_fill(resting, trade_qty)
            if resting.is_fully_filled:
                level.orders.popleft()
                book._order_index.pop(resting.order_id, None)

    def _execute_trade(
        self,
        aggressor: Order,
        resting: Order,
        price: float,
        quantity: float,
    ) -> Trade:
        """Create a Trade record from a match.

        Args:
            aggressor: The incoming order.
            resting: The resting order.
            price: Execution price.
            quantity: Execution quantity.

        Returns:
            The executed Trade.
        """
        buy_id = (
            aggressor.order_id
            if aggressor.side == Side.BUY
            else resting.order_id
        )
        sell_id = (
            aggressor.order_id
            if aggressor.side == Side.SELL
            else resting.order_id
        )
        trade_id = self._next_trade_id
        self._next_trade_id += 1
        trade = Trade(
            trade_id=trade_id,
            symbol=aggressor.symbol,
            price=price,
            quantity=quantity,
            buy_order_id=buy_id,
            sell_order_id=sell_id,
        )
        logger.info(
            "Trade %d: %.4f @ %.4f (buy=%d, sell=%d)",
            trade.trade_id, quantity, price, buy_id, sell_id,
        )
        return trade

    def _update_order_after_fill(
        self,
        order: Order,
        fill_quantity: float,
    ) -> None:
        """Update order state after a fill.

        Args:
            order: The order that was (partially) filled.
            fill_quantity: How much was filled.
        """
        order.remaining_quantity -= fill_quantity
        if order.is_fully_filled:
            order.status = OrderStatus.FILLED
        else:
            order.status = OrderStatus.PARTIALLY_FILLED

    def _validate_modify(
        self,
        order: Order,
        new_quantity: Optional[float],
        new_price: Optional[float],
    ) -> None:
        """Validate order modification parameters.

        Args:
            order: The existing order.
            new_quantity: Proposed new quantity.
            new_price: Proposed new price.

        Raises:
            ValueError: On invalid modifications.
        """
        if order.status in (OrderStatus.FILLED, OrderStatus.CANCELLED):
            raise ValueError(
                f"Cannot modify order {order.order_id}: "
                f"status={order.status.value}"
            )
        if new_quantity is not None and new_quantity < order.filled_quantity:
            raise ValueError(
                f"New quantity {new_quantity} less than "
                f"filled quantity {order.filled_quantity}"
            )
        if new_price is not None and new_price <= 0:
            raise ValueError(f"Price must be positive, got {new_price}")
