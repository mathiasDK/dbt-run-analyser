import pandas as pd
import random

def model(dbt, session):

    n_orders = 1000
    n_products = 100
    product_ids = [f'x{i}' for i in range(n_products)]

    orders = []
    products = []
    quantities = []

    for store in ['po', 'on']:

        for i in range(n_orders):
            order_id = f'{store}_{1000+i}'
            for l in random.randint(1,5):
                product = random.choice(product_ids)
                quantity = random.randint(1,5)
                products.append(product)
                orders.append(order_id)
                quantities.append(quantity)

    data = {
        'order_id': orders,
        'product_id': products,
        'quantity': quantities,
    }

    df = pd.DataFrame(data)

    return df