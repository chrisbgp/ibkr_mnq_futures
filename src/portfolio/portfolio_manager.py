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
        # List of orders. Each inner list now typically contains a single limit order tuple: (Order, Optional[Contract], bool_handled_flag).
        # bool_handled_flag indicates if the order's filled/cancelled state has been processed.
        self.orders: List[List[(Order, Optional[Contract], bool)]] = []
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

        for bracket_order_items in self.orders: # Renamed for clarity, iterates over List[(Order, Contract, bool)]
            for order, _, _ in bracket_order_items: # Corrected unpacking for 3-element tuple
                logging.info(f"Order: {str(order)}")
        
        self.api.request_open_orders() # Ensures self.api.open_orders is populated for other uses if needed

        if len(self.orders) > 0:
            filled_count, cancelled_count, pending_count = self._get_order_status_count()
            logging.debug(f"Order statuses: {filled_count} filled, {cancelled_count} cancelled, {pending_count} pending")

        for bracket_idx, bracket_order_items in enumerate(self.orders):
            
            for order_idx, (order, contract_obj, already_handled) in enumerate(bracket_order_items):
                
                order_status = self._get_order_status(order.orderId)
                
                if order_status['status'] == 'Filled' and not already_handled:
                    if contract_obj is None:
                        logging.warning(f"Order {order.orderId} is filled but contract details are missing. Cannot update position from this order. This might happen for orders loaded from DB without full contract info.")
                        # Mark as handled to avoid reprocessing this warning, or decide on other error handling
                        self.orders[bracket_idx][order_idx] = (order, contract_obj, True)
                        continue

                    # Use the contract_obj stored with the order
                    # Handle filled BUY orders (Limit or Market)
                    if order.action == 'BUY' and (order.orderType == 'LMT' or order.orderType == 'MKT'):

                        if len(self.positions) == 0:
                            logging.info(f"{order.orderType} Buy order filled, creating new position.")

                            position = Position(
                            ticker=contract_obj.symbol,
                            security=contract_obj.secType,
                            currency=contract_obj.currency,
                            expiry=contract_obj.lastTradeDateOrContractMonth,
                            contract_id=contract_obj.conId,
                            quantity=int(order.totalQuantity),
                            avg_price=order_status['avg_fill_price'],
                            timezone=self.config.timezone,
                            )

                            self.positions.append(position)
                            self.db.add_position(position)

                            self.db.update_order_status(order.orderId, order_status)

                            self.orders[bracket_idx][order_idx] = (order, contract_obj, True)

                        else:
                            logging.info(f"{order.orderType} Buy order filled, updating position.")

                            position = copy.deepcopy(self.positions[-1])

                            total_quantity = position.quantity + int(order.totalQuantity)
                            avg_price = position.quantity * position.avg_price
                            avg_price += int(order.totalQuantity) * order_status['avg_fill_price'] 
                            avg_price /= total_quantity

                            position.quantity = total_quantity
                            position.avg_price = avg_price

                            self.orders[bracket_idx][order_idx] = (order, contract_obj, True)
                            
                            self.positions.append(position)
                            self.db.add_position(position)

                            self.db.update_order_status(order.orderId, order_status)

                    # Handle filled SELL orders (Limit, Market or Stop)
                    elif order.action == 'SELL' and (order.orderType == 'LMT' or order.orderType == 'MKT' or order.orderType == 'STP'):

                        logging.info(f"{order.orderType} Sell order filled, updating position.")

                        position = copy.deepcopy(self.positions[-1])

                        new_quantity = position.quantity - int(order.totalQuantity)
                        
                        # For a SELL, the avg_price (cost basis per share) of the position does not change.
                        # PnL is realized, but the cost basis of remaining shares is the same.
                        # If new_quantity is 0, this position object represents the closed state.
                        # The original avg_price is retained on this Position object.
                        
                        position.quantity = new_quantity
                        # position.avg_price remains position.avg_price from self.positions[-1]
                        
                        self.orders[bracket_idx][order_idx] = (order, contract_obj, True)

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

            for order, _, _ in bracket_order: # Adjusted to unpack three items

                order_status = self._get_order_status(order.orderId)

                if order_status['status'] == 'Filled':
                    
                    if order.orderType == 'STP' or \
                       (order.orderType == 'LMT' and order.action == 'SELL'): # Explicit parentheses for LMT SELL

                        current_order_pnl += order_status['avg_fill_price'] * int(order_status['filled'])

                    elif (order.orderType == 'MKT' and order.action == 'BUY') or \
                         (order.orderType == 'LMT' and order.action == 'BUY'): # Added LMT BUY condition

                        current_order_pnl -= order_status['avg_fill_price'] * int(order_status['filled'])

                    elif order.orderType == 'MKT' and order.action == 'SELL':

                        current_order_pnl += order_status['avg_fill_price'] * int(order_status['filled'])

                    else:

                        raise TypeError(f"Order type {order.orderType} with action {order.action} is not supported.")
                    
                    filled_count += 1
            
            # For single order logic, each filled order contributes to PnL.
            # The filled_count >= 2 logic was for bracket orders (entry + exit).
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
        
        # Set the limit price for the BUY order to the latest mid-price
        limit_price = mid_price
        limit_price = round(limit_price / self.config.mnq_tick_size) * self.config.mnq_tick_size

        logging.debug(f"Calculated LMT price (latest mid-price): {limit_price}")

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
            self._handle_successful_bracket_order(limit_order_list, contract)
        else:
            self._handle_failed_bracket_order(limit_order_list, contract)

    def _handle_successful_bracket_order(self, order_list: List[Order], contract: Contract):
        """Handle a successful limit order submission. This is called when the order was accepted by the API."""
        logging.info("Limit order was accepted by the API.")
        # Append as a list containing the single order tuple (Order, Contract, HandledFlag)
        self.orders.append([(o, contract, False) for o in order_list])
        self.db.add_order(order_list) # Add the single order to DB (DB part needs future update for contract)

        for order in order_list:
            order_id = order.orderId
            status = self._get_order_status(order_id)
            self.db.add_order_status(order_id, status)

        self.update_positions()

    def _handle_failed_bracket_order(self, order_list: List[Order], contract: Contract):
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
            self._handle_successful_bracket_order(order_list, contract)
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
                    self.orders.append([(o, contract, True) for o in order_list]) # Mark as handled (cancelled)
                    self.db.add_order(order_list) # DB part needs future update for contract
                    self.db.add_order_status(limit_order.orderId, final_status)
                elif final_status:
                     logging.error(f"Failed to cancel order {limit_order.orderId}. Final status: {final_status['status']}. Manual intervention likely required.")
                else:
                     logging.error(f"Failed to cancel order {limit_order.orderId} and could not retrieve final status. Manual intervention likely required.")

            except Exception as e:
                logging.error(f"Error during cancellation attempt for order {limit_order.orderId}: {e}")
                logging.error(f"Manual intervention likely required for order {limit_order.orderId}.")

    def has_pending_orders(self):
        for bracket_order_items in self.orders:

            for order, _, _ in bracket_order_items: # Adjusted to unpack three items
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

        for bracket_idx, bracket_order_items in enumerate(self.orders):
            for order_idx, (order, stored_contract, already_handled) in enumerate(bracket_order_items):
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
                        self.orders[bracket_idx][order_idx] = (order, stored_contract, True)
                        
                        contract_for_resubmission = stored_contract
                        if contract_for_resubmission is None:
                            # Fallback if contract wasn't stored (e.g. loaded from old DB format)
                            logging.info(f"Stored contract for order {order.orderId} is None, attempting to get from API.")
                            self.api.request_open_orders() # Ensure open orders are fresh
                            time.sleep(0.5) 
                            order_details = self.api.get_open_order(order.orderId)
                            if order_details and 'contract' in order_details:
                                contract_for_resubmission = order_details['contract']
                            else:
                                logging.error(f"Could not get contract details for cancelled order {order.orderId} from API. Cannot resubmit.")
                        
                        if contract_for_resubmission:
                            orders_to_resubmit.append(contract_for_resubmission)
                        else:
                            logging.error(f"No contract details available for cancelled order {order.orderId}. Cannot resubmit.")
                    else:
                        logging.info(f"Not resubmitting cancelled order type: {order.orderType}, id:{order.orderId}.")
                        # Mark as handled even if not resubmitting
                        self.orders[bracket_idx][order_idx] = (order, stored_contract, True)

        # Resubmit collected orders outside the iteration
        for contract_to_resubmit in orders_to_resubmit:
             logging.info(f"Resubmitting order for contract {contract_to_resubmit.symbol} {contract_to_resubmit.lastTradeDateOrContractMonth}")
             self.place_bracket_order(contract_to_resubmit) # place_bracket_order now places a limit order

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
        
        for bracket_order_items in self.orders:

            total_orders += len(bracket_order_items)

            for order, _, _ in bracket_order_items: # Adjusted to unpack three items

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
        
        for bracket_order_items in self.orders:

            for order, _, _ in bracket_order_items: # Adjusted to unpack three items
            
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
            matching_position_data = self.api.get_matching_position(position)

            ibkr_quantity = 0
            if matching_position_data is not None:
                ibkr_quantity = int(matching_position_data['position'])

            if position.quantity > 0 and ibkr_quantity == 0:
                # Local state says open, IBKR says closed/none. Correct local state.
                logging.warning(f"CloseAll: Local position {position.ticker} (Qty: {position.quantity}) but IBKR reports 0. Updating local state to 0.")
                position.quantity = 0
                position.avg_price = 0.0
                self.db.add_position(position) # Log the correction
                # Rebuild self.positions to remove zero-quantity items
                self.positions = [p for p in self.positions if p.quantity > 0]
                logging.info("CloseAll: Local position state corrected. No MKT order needed as IBKR already flat.")
                return # Position is now correctly reflected as closed locally.

            elif position.quantity > 0 and ibkr_quantity > 0:
                if position.quantity > ibkr_quantity:
                    logging.warning(f"CloseAll: Local position {position.ticker} (Qty: {position.quantity}) but IBKR reports Qty: {ibkr_quantity}. "
                                     f"Will attempt to close IBKR quantity: {ibkr_quantity}.")
                    # Adjust local quantity to what IBKR reports, then close that.
                    # This is a partial correction; full sync is populate_from_db's job.
                    # The MKT order below will use ibkr_quantity.
                
                # Proceed to close the position reported by IBKR (or the lesser of local/IBKR if local was higher)
                quantity_to_close = min(position.quantity, ibkr_quantity) # Ensure we don't try to sell more than IBKR has
                if quantity_to_close <= 0 : # Should not happen if position.quantity > 0 and ibkr_quantity > 0
                    logging.error(f"CloseAll: Calculated quantity_to_close is {quantity_to_close} for {position.ticker}. Aborting close attempt.")
                    return

                logging.info(f"CloseAll: Attempting to close {quantity_to_close} shares of {position.ticker} via MKT order.")
                contract = Contract()
                contract.symbol = position.ticker
                contract.secType = position.security
                contract.currency = position.currency
                contract.exchange = self.config.exchange
                contract.lastTradeDateOrContractMonth = position.expiry

                # Place the order. place_market_order waits for an initial status update.
                placed_order_id, initial_status = self.api.place_market_order(contract, "SELL", quantity_to_close)
            
                # Construct an Order object for tracking, as get_open_order might fail if it fills too quickly.
                closing_order_obj = Order()
                closing_order_obj.orderId = placed_order_id
                closing_order_obj.action = "SELL"
                closing_order_obj.orderType = "MKT"
                closing_order_obj.totalQuantity = quantity_to_close # Use the quantity we decided to close
                # Other attributes like lmtPrice, auxPrice, parentId are not relevant for a simple MKT close.

                # Add this constructed order to self.orders for processing by update_positions
                # The 'contract' object used to place the order is already in scope.
                self.orders.append([(closing_order_obj, contract, False)])
                logging.info(f"Appended closing market order {placed_order_id} to self.orders for tracking.")

                # Log the order and its initial status to the database
                self.db.add_order(closing_order_obj) 
                
                # Get the most up-to-date status (could have changed from initial_status if filled quickly)
                # _get_order_status checks self.api.order_statuses first, which place_market_order populates.
                status_to_log = self._get_order_status(placed_order_id)
                if status_to_log:
                    self.db.add_order_status(placed_order_id, status_to_log)
                elif initial_status: # Fallback to initial status if current is somehow None
                    logging.warning(f"Could not get current status for closing order {placed_order_id} to log to DB. Using initial status: {initial_status}")
                    self.db.add_order_status(placed_order_id, initial_status)
                else:
                    logging.error(f"Could not get any status (current or initial) for closing order {placed_order_id} to log to DB.")

                self.update_positions() # This should process the fill and set local quantity to 0
                return # Explicit return after attempting MKT close path

            elif position.quantity <= 0 : # Local state already shows 0 or negative (error)
                logging.info(f"CloseAll: Local position for {position.ticker} already shows quantity {position.quantity}. No action taken.")
                return

            # Fallback for unhandled scenarios, though above logic should cover typical cases
            logging.error(f"CloseAll: Unhandled scenario for position {position.ticker} Qty: {position.quantity}, IBKR Qty: {ibkr_quantity}. No MKT order placed.")
            return

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

        # Store each loaded order as a single-item list with None for Contract
        # (DB schema and loading logic needs update to store/retrieve full Contract)
        single_orders_with_contract_placeholder = []
        for order, filled_flag in zip(loaded_orders, filled_flags):
            single_orders_with_contract_placeholder.append([(order, None, filled_flag)])

        self.orders = single_orders_with_contract_placeholder

        logging.info("PortfolioManager: Populating positions from database.")
        logging.debug(f"PortfolioManager: Raw positions found: {len(raw_positions)}")

        for position in raw_positions:
            time_created = pd.to_datetime(position['created_timestamp'])

            if time_created > trading_day_start:
                self.positions.append(Position.from_dict(position))

        logging.debug(f"Loaded {len(self.positions)} positions from database initially.")

        # Synchronize with IBKR API positions
        logging.info("PortfolioManager: Synchronizing loaded positions with IBKR API.")
        ibkr_api_positions_raw = self.api.get_positions() # List of dicts {'contract': Contract, 'position': float, 'avg_cost': float}
        
        # Convert API positions to a dictionary keyed by contract_id for easier lookup
        ibkr_api_positions_map = {}
        for pos_data in ibkr_api_positions_raw:
            contract = pos_data['contract']
            # API position can be float (e.g., 0.0), ensure int for quantity.
            quantity = int(pos_data['position']) 
            avg_cost = float(pos_data['avg_cost'])
            ibkr_api_positions_map[contract.conId] = {
                'contract': contract, 
                'quantity': quantity, 
                'avg_cost': avg_cost
            }

        # Use a dictionary for current self.positions (initially loaded from DB) for efficient lookup and update.
        # This map will hold Position objects.
        current_in_memory_positions_map = {p.contract_id: p for p in self.positions}
        
        processed_api_con_ids = set() # To track con_ids reported by API

        # Iterate through positions reported by the API
        for con_id, api_pos_details in ibkr_api_positions_map.items():
            processed_api_con_ids.add(con_id)
            api_contract = api_pos_details['contract']
            api_quantity = api_pos_details['quantity'] # This is now an int
            api_avg_cost = api_pos_details['avg_cost']

            local_pos_obj = current_in_memory_positions_map.get(con_id)

            if local_pos_obj: # Position already exists in our local memory (came from DB)
                # Check if API state differs from local state
                if local_pos_obj.quantity != api_quantity or \
                   (api_quantity > 0 and local_pos_obj.avg_price != api_avg_cost): # Only compare avg_price if there's a position
                    logging.info(f"PortfolioManager: Updating position for {api_contract.symbol} (ID: {con_id}). "
                                 f"Local Qty: {local_pos_obj.quantity}, Local AvgPx: {local_pos_obj.avg_price} -> "
                                 f"API Qty: {api_quantity}, API AvgPx: {api_avg_cost}.")
                    local_pos_obj.quantity = api_quantity
                    local_pos_obj.avg_price = api_avg_cost if api_quantity > 0 else 0.0
                    self.db.add_position(local_pos_obj) # Record the updated state in DB
            else: # Position reported by API is new to our local memory
                if api_quantity > 0: # Only create a new local record if API shows an actual holding
                    logging.info(f"PortfolioManager: New position from API for {api_contract.symbol} (ID: {con_id}): "
                                 f"Qty: {api_quantity}, AvgPx: {api_avg_cost}. Adding to local state and DB.")
                    new_pos_obj = Position(
                        ticker=api_contract.symbol,
                        security=api_contract.secType,
                        currency=api_contract.currency,
                        expiry=api_contract.lastTradeDateOrContractMonth,
                        contract_id=con_id,
                        quantity=api_quantity,
                        avg_price=api_avg_cost,
                        timezone=self.config.timezone
                    )
                    current_in_memory_positions_map[con_id] = new_pos_obj # Add to our map
                    self.db.add_position(new_pos_obj) # Record this new position in DB
        
        # Check for positions in local memory (from DB) that were NOT reported by API (implies closure)
        for con_id, local_pos_obj in current_in_memory_positions_map.items():
            if con_id not in processed_api_con_ids: # This contract was in DB but not in API's list at all
                if local_pos_obj.quantity > 0: # If local DB thought it had a position
                    logging.info(f"PortfolioManager: Position for {local_pos_obj.ticker} (ID: {con_id}) "
                                 f"with Qty: {local_pos_obj.quantity} in local state, but not in API's current report. Marking as closed.")
                    local_pos_obj.quantity = 0
                    local_pos_obj.avg_price = 0.0 # Avg price for zero quantity is zero
                    self.db.add_position(local_pos_obj) # Record the closure in DB

        # Reconstruct self.positions list from the synchronized map.
        # This list should primarily track actively held positions.
        self.positions = [pos for pos in current_in_memory_positions_map.values() if pos.quantity > 0]

        logging.info("PortfolioManager: Positions after API synchronization:")
        if len(self.positions) > 0:
            for p_idx, p_obj in enumerate(self.positions):
                logging.info(f"  Synced Position [{p_idx}]: {str(p_obj)}")
        else:
            logging.info("PortfolioManager: No active positions after API synchronization.")
        
        # The existing consistency check will now run on the API-synchronized self.positions
        if check_state:
            if len(self.positions) > 0:
                # The original logic takes self.positions[-1].
                latest_db_position = self.positions[-1] # This is now an API-synced position
                matching_ibkr_position_data = self.api.get_matching_position(latest_db_position)

                inconsistent_state_detected = False
                error_log_message = ""

                if matching_ibkr_position_data is None:
                    # IBKR reports no such position for this contract.
                    if latest_db_position.quantity > 0:
                        # Local DB claims a position exists (quantity > 0), but IBKR does not. This is an inconsistency.
                        inconsistent_state_detected = True
                        error_log_message = (f"Inconsistent DB state: Position {latest_db_position.ticker} (ID: {latest_db_position.contract_id}) "
                                             f"with quantity {latest_db_position.quantity} in DB, but not found in IBKR.")
                else:
                    # IBKR reports a position for this contract. Compare quantities.
                    ibkr_quantity = int(matching_ibkr_position_data['position'])
                    if latest_db_position.quantity != ibkr_quantity:
                        # Quantities differ. This is an inconsistency.
                        inconsistent_state_detected = True
                        error_log_message = (f"Inconsistent DB state: Position {latest_db_position.ticker} (ID: {latest_db_position.contract_id}) "
                                             f"has quantity {latest_db_position.quantity} in DB, but IBKR reports quantity {ibkr_quantity}.")
                
                if inconsistent_state_detected:
                    logging.error(error_log_message + " Reinitializing database and portfolio state.")
                    self.cancel_all_orders() # Attempt to cancel any lingering orders
                    self.clear_orders_statuses_positions() # Clear local state
                    self.db.reinitialize() # Wipe and reinitialize DB tables
                    self.db.print_all_entries() # Log the fresh DB state
                else:
                    logging.info("DB state consistent with IBKR.")

        return len(loaded_orders), len(self.order_statuses), len(self.positions)



