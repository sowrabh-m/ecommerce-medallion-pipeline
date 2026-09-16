import pandas as pd
from faker import Faker
import random
from datetime import date, timedelta
import os

Faker.seed(42)
random.seed(42)
fake = Faker()

def generate_customers(n: int) -> pd.DataFrame:
    rows = []
    for i in range(1, n+1):
        rows.append({
            "customer_id": f"CUST{i:05d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "signup_date": fake.date_between(start_date='-2y', end_date='today'),
            "city": fake.city(),
            "country": fake.country(),
        })
    return pd.DataFrame(rows)

def generate_products(n: int) -> pd.DataFrame:
    rows = []
    categories = ["Electronics", "Home", "Clothing", "Sports", "Books", "Toys"]
    for i in range(1, n+1):
        rows.append({
            "product_id": f"PROD{i:04d}",
            "product_name": fake.word().capitalize() + " " + fake.word().capitalize(),
            "category": random.choice(categories),
            "unit_price": round(random.uniform(100, 2000), 2)
        })
    return pd.DataFrame(rows)

def generate_orders(order_date: str, customer_ids: list, n: int) -> pd.DataFrame:
    rows = []
    status = ["pending", "shipped", "delivered", "cancelled"]
    for i in range(1, n+1):
        rows.append({
            "order_id": f"ORD-{order_date.replace('-', '')}-{i:04d}",
            "customer_id": random.choice(customer_ids),
            "order_date": order_date,
            "status": random.choice(status)
        })
    return pd.DataFrame(rows)

def generate_order_items(orders_df: pd.DataFrame, product_ids: list, price_lookup: dict) -> pd.DataFrame:
    rows = []
    for _, order in orders_df.iterrows():
        num_items = random.randint(1, 5)
        for item_seq in range(1, num_items + 1):
            product_id = random.choice(product_ids)
            rows.append({
                "order_item_id": f"{order['order_id']}-ITEM{item_seq:02d}",
                "order_id": order["order_id"],
                "product_id": product_id,
                "quantity": random.randint(1, 5),
                "unit_price": price_lookup[product_id],
            })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    customers_df = generate_customers(100)
    products_df = generate_products(30)
    customers_df.to_csv("data/raw/customers.csv", index=False)
    products_df.to_csv("data/raw/products.csv", index=False)

    customers_df = pd.read_csv("data/raw/customers.csv")
    products_df = pd.read_csv("data/raw/products.csv")
    customer_ids = customers_df["customer_id"].tolist()
    product_ids = products_df["product_id"].tolist()
    price_lookup = dict(zip(products_df["product_id"], products_df["unit_price"]))

    start_date = date(2026, 1, 1)
    num_days = 30

    for day_offset in range(num_days):
        current_date = start_date + timedelta(days=day_offset)
        date_str = current_date.isoformat()
        n_orders = random.randint(10, 30)

        orders_df = generate_orders(date_str, customer_ids, n_orders)
        order_items_df = generate_order_items(orders_df, product_ids, price_lookup)

        day_dir = f"data/raw/{date_str}"
        os.makedirs(day_dir, exist_ok=True)
        orders_df.to_csv(f"{day_dir}/orders.csv", index=False)
        order_items_df.to_csv(f"{day_dir}/order_items.csv", index=False)

    print(f"Generated {num_days} days of orders/order_items into data/raw/<date>/")