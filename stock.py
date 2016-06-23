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
    def get_input_output_stock(cls, products, name):
        pool = Pool()
        Move = pool.get('stock.move')
        Location = pool.get('stock.location')
        move = Move.__table__()

        transaction = Transaction()
        context = transaction.context
        stock_date_end = context.get('stock_date_end')
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
