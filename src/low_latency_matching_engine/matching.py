from collections import deque
from typing import List, Tuple

class MatchingEngine:
    def __init__(self):
        self.buy_orders: deque = deque()
        self.sell_orders: deque = deque()
        self.trades: List[Tuple] = []
    
    def submit_order(self, price: float, quantity: int, side: str) -> None:
        """Submit order for matching."""
        if side == 'BUY':
            self.buy_orders.append((price, quantity))
        else:
            self.sell_orders.append((price, quantity))
        self._match_orders()
    
    def _match_orders(self) -> None:
        """Match buy and sell orders."""
        while self.buy_orders and self.sell_orders:
            buy_price, buy_qty = self.buy_orders[0]
            sell_price, sell_qty = self.sell_orders[0]
            
            if buy_price >= sell_price:
                trade_qty = min(buy_qty, sell_qty)
                self.trades.append((buy_price, sell_price, trade_qty))
                
                if buy_qty == trade_qty:
                    self.buy_orders.popleft()
                else:
                    self.buy_orders[0] = (buy_price, buy_qty - trade_qty)
                
                if sell_qty == trade_qty:
                    self.sell_orders.popleft()
                else:
                    self.sell_orders[0] = (sell_price, sell_qty - trade_qty)
            else:
                break
