# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
from sql.aggregate import Sum
from trytond.pool import Pool
from trytond.tools import grouped_slice, reduce_ids
from trytond.transaction import Transaction


__all__ = ['StockMixin']


class StockMixin(object):
    '''Mixin class to setup input and output stock quantity fields.'''

    @classmethod
    def get_input_output_product(cls, products, name):
        '''
        Get all input/output in all products
        @param products: list records
        @param name: field name
        Context:
        - locations: list ids
        - stock_date_end: datetime
        '''
        pool = Pool()
        Move = pool.get('stock.move')
        Period = pool.get('stock.period')
        Location = pool.get('stock.location')
        Date = pool.get('ir.date')
        move = Move.__table__()

        transaction = Transaction()
        context = transaction.context

        stock_date_end = context.get('stock_date_end', Date.today())
        location_ids = context.get('locations', [])

        # TODO Get Period Cache
        # PeriodCache = Period.get_cache(grouping)

        locations = Location.browse(context.get('locations'))
        if name == 'input_stock':
            location_ids = [l.input_location for l in locations]
        else:
            location_ids = [l.storage_location for l in locations]
        location_ids = [l.id for l in Location.search([
                ('parent', 'child_of', location_ids),
                ], order=[])]
        if name == 'input_stock':
            in_locations = move.to_location.in_(location_ids)
            move_state_in = move.state.in_(['draft'])
        else:
            in_locations = move.from_location.in_(location_ids)
            move_state_in = move.state.in_(['assigned'])

        result = {}
        product2lines = {}
        for product in products:
            result[product.id] = 0
            product2lines.setdefault(product.id, []).append(product)

        cursor = transaction.cursor
        for in_products in grouped_slice(product2lines.keys()):
            product_ids = reduce_ids(move.product, in_products)
            query = (move
                .select(
                    move.product.as_('product'),
                    Sum(move.internal_quantity).as_('quantity'),
                    where=(product_ids
                        & in_locations
                        & move_state_in
                        & (
                            (move.effective_date == None)
                            |
                            (move.effective_date == stock_date_end))
                        ),
                    group_by=(move.product)
                    )
                )
            cursor.execute(*query)
            for product_id, quantity in cursor.fetchall():
                for line in product2lines[product_id]:
                    result[line.id] = quantity
        return result

    @classmethod
    def get_input_output_location(cls, products, name):
        '''
        Get all input/output in all locations for products
        @param locations: list records
        @param name: field name
        Context:
        - locations: list ids
        - stock_date_end: datetime
        '''
        pool = Pool()
        Move = pool.get('stock.move')
        Period = pool.get('stock.period')
        Date = pool.get('ir.date')
        move = Move.__table__()

        transaction = Transaction()
        context = transaction.context

        stock_date_end = context.get('stock_date_end', Date.today())
        location_ids = context.get('locations', [])

        # TODO Get Period Cache
        # PeriodCache = Period.get_cache(grouping)

        if name == 'input_stock':
            in_locations = move.to_location.in_(location_ids)
            move_state_in = move.state.in_(['draft'])
        else:
            in_locations = move.from_location.in_(location_ids)
            move_state_in = move.state.in_(['assigned'])

        result = {}
        product2lines = {}
        for product in products:
            result[product.id] = {}
            product2lines.setdefault(product.id, []).append(product)

        cursor = transaction.cursor
        for in_products in grouped_slice(product2lines.keys()):
            product_ids = reduce_ids(move.product, in_products)
            query = (move
                .select(
                    move.product.as_('product'),
                    Sum(move.internal_quantity).as_('quantity'),
                    move.from_location.as_('from_location'),
                    move.to_location.as_('to_location'),
                    where=(product_ids
                        & in_locations
                        & move_state_in
                        & (
                            (move.effective_date == None)
                            |
                            (move.effective_date == stock_date_end))
                        ),
                    group_by=(move.product, move.from_location, move.to_location)
                    )
                )
            cursor.execute(*query)
            for product_id, quantity, from_location, to_location in cursor.fetchall():
                for line in product2lines[product_id]:
                    if name == 'input_stock':
                        result[line.id].update({to_location: quantity})
                    else:
                        result[line.id].update({from_location: quantity})
        return result
