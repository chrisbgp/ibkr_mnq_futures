import copy
from src.portfolio.position import Position
import pandas as pd
from typing import List, Dict, Optional
from ibapi.order import Order
from ibapi.contract import Contract
import os
from src.portfolio.position import Position
from src.api.ibkr_api import IBConnection
from src.configuration import Configuration
import logging
import time
from src.db.database import Database
from src.api.api_utils import get_current_contract, order_from_dict
from src.utilities.utils import trading_day_start_time_ts


class PortfolioManager:

    def __init__(self, config: Configuration, api: IBConnection, db: Database):
        self.config = config
        self.api = api
        self.db = db

        self.positions: List[Position] = []
        # List of orders. Each inner list now typically contains a single limit order.
        # bool indicates if the order's filled/cancelled state has been processed to update positions/check resubmission.
        self.orders: List[List[(Order, bool)]] = []
        self.order_statuses: Dict[int, Dict] = {}          #order id -> order status

    def _get_order_status(self, order_id: int):
        """Get the order status for a given order id. Required to persist order
        statuses after the API or app disconnects. Check the API first, then check the local status. 
        Filled orders are not stored in the API after restart but unfilled are."""
        if order_id in self.api.order_statuses:
            return self.api.order_statuses[order_id]
        elif order_id in self.order_statuses:
            return self.order_statuses[order_id]
        else:
            logging.error(f"Order {order_id} not found in API or local order statuses")
            return None
        
    def update_positions(self):
        """Update the positions from the API."""
        logging.info(f"{self.__class__.__name__}: Updating positions from orders.")
        logging.debug(f"{self.__class__.__name__}: There are {self._total_orders()} orders")

        for bracket_order in self.orders:
            for order, _ in bracket_order:
                logging.info(f"Order: {str(order)}")
        
        self.api.request_open_orders()

        if len(self.orders) > 0:
            filled_count, cancelled_count, pending_count = self._get_order_status_count()
            logging.debug(f"Order statuses: {filled_count} filled, {cancelled_count} cancelled, {pending_count} pending")

        for bracket_idx, bracket_order in enumerate(self.orders):
            
            for order_idx, (order, already_handled) in enumerate(bracket_order):
                
                order_status = self._get_order_status(order.orderId)
                
                if order_status['status'] == 'Filled' and not already_handled:

                    order_details = self.api.get_open_order(order.orderId)
                    contract = order_details['contract']
                    # Handle filled BUY orders (Limit or Market)
                    if order.action == 'BUY' and (order.orderType == 'LMT' or order.orderType == 'MKT'):

                        if len(self.positions) == 0:
                            logging.info(f"{order.orderType} Buy order filled, creating new position.")

                            position = Position(
                            ticker=contract.symbol,
                            security=contract.secType,
                            currency=contract.currency,
                            expiry=contract.lastTradeDateOrContractMonth,
                            contract_id=contract.conId,
                            quantity=int(order.totalQuantity),
                            avg_price=order_status['avg_fill_price'],
                            timezone=self.config.timezone,
                            )

                            self.positions.append(position)
                            self.db.add_position(position)

                            self.db.update_order_status(order.orderId, order_status)

                            self.orders[bracket_idx][order_idx] = (order, True)

                        else:
                            logging.info(f"{order.orderType} Buy order filled, updating position.")

                            position = copy.deepcopy(self.positions[-1])

                            total_quantity = position.quantity + int(order.totalQuantity)
                            avg_price = position.quantity * position.avg_price
                            avg_price += int(order.totalQuantity) * order_status['avg_fill_price'] 
                            avg_price /= total_quantity

                            position.quantity = total_quantity
                            position.avg_price = avg_price

                            self.orders[bracket_idx][order_idx] = (order, True)
                            
                            self.positions.append(position)
                            self.db.add_position(position)

                            self.db.update_order_status(order.orderId, order_status)

                    # Handle filled SELL orders (Limit, Market or Stop)
                    elif order.action == 'SELL' and (order.orderType == 'LMT' or order.orderType == 'MKT' or order.orderType == 'STP'):

                        logging.info(f"{order.orderType} Sell order filled, updating position.")

                        position = copy.deepcopy(self.positions[-1])

                        total_quantity = position.quantity - int(order.totalQuantity)
                        avg_price = position.quantity * position.avg_price
                        avg_price += int(order.totalQuantity) * order_status['avg_fill_price'] 
                        avg_price /= (position.quantity + int(order.totalQuantity))

                        position.quantity = total_quantity
                        position.avg_price = avg_price
                        
                        self.orders[bracket_idx][order_idx] = (order, True)

                        self.positions.append(position)
                        self.db.add_position(position)

                        self.db.update_order_status(order.orderId, order_status)

                    else:

                        raise TypeError(f"Order type {order.orderType} with action {order.action} is not supported.")
        
        msg = f"{self.__class__.__name__}: Finished updating positions from orders."
        msg += f" Currently {len(self.positions)} position(s)."

        logging.info(msg)
        if len(self.positions) > 0:
            for position in self.positions:
                logging.info(str(position))

        self.db.print_all_entries()

    def daily_pnl(self):
        """Update the daily PnL. The daily pnl is made up from the PnL of all filled orders."""
        index_pnl = 0

        for bracket_order in self.orders:
            # a backet order only hits realized pnl if 2 out of 3 orders are filled
            # We need to check the status of each order and then sum the pnl of the filled orders

            filled_count = 0
            current_order_pnl = 0

            for order, _ in bracket_order:

                order_status = self._get_order_status(order.orderId)

                if order_status['status'] == 'Filled':
                    
                    if order.orderType == 'STP' or order.orderType == 'LMT' and order.action == 'SELL':

                        current_order_pnl += order_status['avg_fill_price'] * int(order_status['filled'])

                    elif order.orderType == 'MKT' and order.action == 'BUY':

                        current_order_pnl -= order_status['avg_fill_price'] * int(order_status['filled'])

                    elif order.orderType == 'MKT' and order.action == 'SELL':

                        current_order_pnl += order_status['avg_fill_price'] * int(order_status['filled'])

                    else:

                        raise TypeError(f"Order type {order.orderType} with action {order.action} is not supported.")
                    
                    filled_count += 1
                
            if filled_count >= 2:
                index_pnl += current_order_pnl

        return index_pnl * self.config.mnq_point_value

    def place_bracket_order(self, contract: Contract = None):
        """Place a bracket order"""
        logging.debug("Placing bracket order.")
        contract = self.get_current_contract() if contract is None else contract

        mid_price = self.api.get_latest_mid_price(contract)

        if mid_price is None:
            logging.error(f"No mid price found for contract {contract.symbol}. Cannot place bracket order.")
            return
        
        # Calculate limit price (using the take_profit logic as a placeholder for limit price calculation)
        # TODO: Adjust this logic if a different limit price calculation is needed.
        limit_price = mid_price + (self.config.take_profit_ticks * self.config.mnq_tick_size)
        limit_price = round(limit_price / self.config.mnq_tick_size) * self.config.mnq_tick_size

        logging.debug(f"Calculated LMT price: {limit_price}")

        # Call the (renamed but functionally different) create_bracket_order to get a single limit order list
        # Pass 0 for stop_loss_price as it's ignored by the modified API function.
        limit_order_list = self.api.create_bracket_order(
            "BUY",
            self.config.number_of_contracts,
            limit_price,
            0) # stop_loss_price is ignored

        self.api.place_orders(limit_order_list, contract)

        # Check if the single order status was received
        if all(self._get_order_status(order.orderId) for order in limit_order_list):
            self._handle_successful_bracket_order(limit_order_list)
        else:
            self._handle_failed_bracket_order(limit_order_list)

    def _handle_successful_bracket_order(self, order_list: List[Order]):
        """Handle a successful limit order submission. This is called when the order was accepted by the API."""
        logging.info("Limit order was accepted by the API.")
        # Append as a list containing the single order tuple
        self.orders.append(list(zip(order_list, [False] * len(order_list))))
        self.db.add_order(order_list) # Add the single order to DB

        for order in order_list:
            order_id = order.orderId
            status = self._get_order_status(order_id)
            self.db.add_order_status(order_id, status)

        self.update_positions()

    def _handle_failed_bracket_order(self, order_list: List[Order]):
        """Handle a failed limit order submission. This is called when the order status callback is not received promptly."""
        logging.error("Order callback not received for the limit order.")
        logging.warning(f"Pausing for {self.config.timeout} seconds before rechecking order status.")

        time.sleep(self.config.timeout)

        logging.warning("Checking order status again after pause.")

        limit_order = order_list[0] # There's only one order
        order_status = self._get_order_status(limit_order.orderId)

        if order_status:
            logging.info(f"Order {limit_order.orderId} status received after pause: {order_status['status']}")
            # If status is now received, handle as successful
            self._handle_successful_bracket_order(order_list)
        else:
            logging.error(f"Order callback still not received for order {limit_order.orderId}. Attempting cancellation.")
            try:
                self.api.cancel_order(limit_order.orderId)
                # Re-check status after cancellation attempt
                time.sleep(1) # Short pause to allow cancellation status to potentially propagate
                final_status = self._get_order_status(limit_order.orderId)
                if final_status and final_status['status'] == 'Cancelled':
                    logging.info(f"Limit order {limit_order.orderId} was cancelled successfully after initial failure.")
                    # Add the cancelled order to DB for record keeping
                    self.orders.append(list(zip(order_list, [True]))) # Mark as handled (cancelled)
                    self.db.add_order(order_list)
                    self.db.add_order_status(limit_order.orderId, final_status)
                elif final_status:
                     logging.error(f"Failed to cancel order {limit_order.orderId}. Final status: {final_status['status']}. Manual intervention likely required.")
                else:
                     logging.error(f"Failed to cancel order {limit_order.orderId} and could not retrieve final status. Manual intervention likely required.")

            except Exception as e:
                logging.error(f"Error during cancellation attempt for order {limit_order.orderId}: {e}")
                logging.error(f"Manual intervention likely required for order {limit_order.orderId}.")

    def has_pending_orders(self):
        for bracket_order in self.orders:

            for order, _ in bracket_order:
                # Check status only if it exists
                order_status_data = self._get_order_status(order.orderId)
                if not order_status_data:
                    logging.warning(f"Could not get status for order {order.orderId} while checking for pending orders.")
                    continue # Skip if status is unavailable

                order_status = order_status_data['status']

                # Check if the order is a Limit order and is still pending (not Filled or Cancelled)
                if (order.orderType == 'LMT' and
                    order_status != 'Filled' and
                    order_status != 'Cancelled'):
                    # Found a pending order
                    return True

        return False # No pending orders found

    def current_position_quantity(self):
        return self.positions[-1].quantity if len(self.positions) > 0 else 0

    def check_cancelled_market_order(self):
        """Check for cancelled limit orders and resubmit them if required."""
        # Note: Function name kept as requested, but logic changed for LMT orders.
        logging.debug("Checking for cancelled limit orders.")

        found_cancelled_order = False
        orders_to_resubmit = [] # Collect orders to resubmit outside the loop

        for bracket_idx, bracket_order in enumerate(self.orders):
            for order_idx, (order, already_handled) in enumerate(bracket_order):
                # Check status only if it exists
                order_status_data = self._get_order_status(order.orderId)
                if not order_status_data:
                    logging.warning(f"Could not get status for order {order.orderId} while checking for cancellations.")
                    continue # Skip if status is unavailable

                order_status = order_status_data['status']

                if (not already_handled and
                    order.orderType == 'LMT' and # Check for LMT instead of MKT
                    order_status == "Cancelled"):

                    logging.warning(f"Order type: {order.orderType}, id:{order.orderId}, was cancelled.")
                    found_cancelled_order = True

                    if self.config.resubmit_cancelled_order:
                        logging.info(f"Marking order type: {order.orderType}, id:{order.orderId} for resubmission.")
                        # Mark as handled to prevent repeated attempts in this cycle
                        self.orders[bracket_idx][order_idx] = (order, True)
                        # Get contract details needed for resubmission
                        # Need to request open orders if not already done recently
                        self.api.request_open_orders()
                        time.sleep(0.5) # Give time for open orders to potentially populate
                        order_details = self.api.get_open_order(order.orderId)
                        if order_details and 'contract' in order_details:
                             orders_to_resubmit.append(order_details['contract'])
                        else:
                             logging.error(f"Could not get contract details for cancelled order {order.orderId}. Cannot resubmit.")

                    else:
                        logging.info(f"Not resubmitting cancelled order type: {order.orderType}, id:{order.orderId}.")
                        # Mark as handled even if not resubmitting
                        self.orders[bracket_idx][order_idx] = (order, True)

        # Resubmit collected orders outside the iteration
        for contract in orders_to_resubmit:
             logging.info(f"Resubmitting order for contract {contract.symbol} {contract.lastTradeDateOrContractMonth}")
             self.place_bracket_order(contract) # place_bracket_order now places a limit order

        if not found_cancelled_order:
            logging.debug("No cancelled limit orders found.")
        
    def get_current_contract(self): 
        """Get the current contract"""
        return get_current_contract(
            self.config.ticker,
            self.config.exchange,
            self.config.currency,
            self.config.roll_contract_days_before,
            self.config.timezone)
    
    def _get_order_status_count(self):
        filled_count = 0
        cancelled_count = 0
        total_orders = 0
        
        for bracket_order in self.orders:

            total_orders += len(bracket_order)

            for order, _ in bracket_order:

                status = self._get_order_status(order.orderId)['status']
                if status == 'Filled':
                    filled_count += 1
                elif status == 'Cancelled':
                    cancelled_count += 1

        pending_count = total_orders - filled_count - cancelled_count
        return filled_count, cancelled_count, pending_count

    def cancel_all_orders(self):
        """Cancel all unfilled and non-cancelled orders."""
        logging.info("Cancelling all active orders.")
        
        for bracket_order in self.orders:

            for order, _ in bracket_order:
            
                order_status = self._get_order_status(order.orderId)
                
                if order_status['status'] not in ['Filled', 'Cancelled']:
                    logging.info(f"Cancelling order {order.orderId} of type {order.orderType}")
                    self.api.cancel_order(order.orderId)

    def close_all_positions(self):
        """Close all open positions by issuing market sell orders."""
        logging.info("Closing all open positions.")

        if len(self.positions) == 0:
            logging.info("No positions to close.")
            return
        
        # The last position entry is the current position
        position = self.positions[-1]

        if position.quantity > 0:
            logging.info(f"Closing position for {position.ticker} with quantity {position.quantity}")
            matching_position = self.api.get_matching_position(position)

            if matching_position is None:
                msg = f"Position {position.ticker} with quantity {position.quantity} not found in IBKR. Cannot close."
                logging.error(msg)
                return

            native_contract_quantity = int(matching_position['position'])
            if position.quantity > native_contract_quantity:
                msg = f"Trying to close position {position.ticker} with quantity {position.quantity}."
                msg += f" Only {native_contract_quantity} contracts are found on IBKR. Cannot close local position."
                logging.error(msg)
                return
            
            contract = Contract()
            contract.symbol = position.ticker
            contract.secType = position.security
            contract.currency = position.currency
            contract.exchange = self.config.exchange
            contract.lastTradeDateOrContractMonth = position.expiry

            # Place the order
            order_id, _ = self.api.place_market_order(contract, "SELL", position.quantity)
            order_details = self.api.get_open_order(order_id)
            self.orders.append([(order_details['order'], False)])

            self.db.add_order(order_details['order'])

            order_id = order_details['order'].orderId
            self.db.add_order_status(order_id, self._get_order_status(order_id))

            self.update_positions()

        elif position.quantity == 0:
            msg = f"Position {position.ticker} with quantity {position.quantity}. No positions to close"
            logging.info(msg)
        else:
            msg = f"Trying to close position {position.ticker} with quantity {position.quantity}. None handled scenario."
            logging.error(msg)
            raise NotImplementedError(msg)
    
    def _total_orders(self):
        return sum(len(bracket_order) for bracket_order in self.orders)
    
    def clear_orders_statuses_positions(self):
        """Clear all orders and positions. This is called when the trading day
        has ended and we need to clear the orders and positions for the next day.
        """
        logging.debug("Clearing orders, order statuses and positions.")
        self.orders = []
        self.positions = []
        self.order_statuses = {}

    def populate_from_db(self, check_state: bool = True):
        """Populate the orders from the database. Only orders created after the 
        trading day start time are loaded. By loading orders and setting 
        already_handled to False, we can ensure that the orders are processed 
        again and dont have to load the positions from the database.
        """
        logging.info("PortfolioManager: Populating orders from database.")
        self.db.print_all_entries()

        raw_orders_and_positions = self.db.get_all_orders_and_positions()
        raw_orders = raw_orders_and_positions['orders']
        raw_positions = raw_orders_and_positions['positions']

        trading_day_start = trading_day_start_time_ts(self.config.trading_start_time, self.config.timezone)

        logging.debug(f"PortfolioManager: Raw orders found: {len(raw_orders_and_positions['orders'])}")
        loaded_orders = []
        for order in raw_orders:
            time_created = pd.to_datetime(order['created_timestamp'])
            if time_created > trading_day_start:
                loaded_orders.append(order_from_dict(order))

        logging.debug(f"Loaded {len(loaded_orders)} orders from database.")


        logging.info("PortfolioManager: Populating order statuses from database.")

        raw_order_statuses = self.db.get_all_order_statuses()
        logging.debug(f"PortfolioManager: Raw order statuses found: {len(raw_order_statuses)}")

        for order_id, status in raw_order_statuses.items():
            time_last_modified = pd.to_datetime(status['last_modified'])

            if time_last_modified > trading_day_start:
                self.order_statuses[order_id] = status

        logging.debug(f"Loaded {len(self.order_statuses)} order statuses from database.")

        # Set whether a position has been logged from filled orders
        filled_flags = []
        for order in loaded_orders:
            order_status = self._get_order_status(order.orderId)
            if order_status['status'] == 'Filled':
                filled_flags.append(True)
            else:
                filled_flags.append(False)

        # Store each loaded order as a single-item list
        single_orders = []
        for order, filled_flag in zip(loaded_orders, filled_flags):
            single_orders.append([(order, filled_flag)])

        self.orders = single_orders

        logging.info("PortfolioManager: Populating positions from database.")
        logging.debug(f"PortfolioManager: Raw positions found: {len(raw_positions)}")

        for position in raw_positions:
            time_created = pd.to_datetime(position['created_timestamp'])

            if time_created > trading_day_start:
                self.positions.append(Position.from_dict(position))

        logging.debug(f"Loaded {len(self.positions)} positions from database.")

        # Check that the latest position from the DB actually still exists in IBKR
        if check_state:
            if len(self.positions) > 0:
                latest_db_position = self.positions[-1]
                matching_position = self.api.get_matching_position(latest_db_position)

                if matching_position is None:
                    msg = f"Inconsistent DB state: Position {latest_db_position.ticker} with quantity {latest_db_position.quantity}"
                    msg += f" from DB not found in IBKR. Reinitializing database and portfolio state."
                    logging.error(msg)

                    self.cancel_all_orders()
                    self.clear_orders_statuses_positions()
                    self.db.reinitialize()
                    self.db.print_all_entries()

                elif latest_db_position.quantity > int(matching_position['position']):
                    msg = f"Inconsistent DB state: Position {latest_db_position.ticker} has {latest_db_position.quantity} contracts."
                    msg += f" Only {int(matching_position['position'])} contracts are found on IBKR."
                    msg += f" Reinitializing database and portfolio state."
                    logging.error(msg)
                    
                    self.cancel_all_orders()
                    self.clear_orders_statuses_positions()
                    self.db.reinitialize()
                    self.db.print_all_entries()
                else:
                    logging.info("DB state consistent with IBKR.")

        return len(loaded_orders), len(self.order_statuses), len(self.positions)



