from sqlalchemy import select
from sqlalchemy.sql import func
from sqlalchemy import distinct
from sqlalchemy import cast, Date, distinct, union
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, DateTime, ForeignKey, Numeric, CheckConstraint
from datetime import datetime


engine = create_engine('sqlite:///Sqlite-Data/sqlite3.db')
session = Session(bind=engine)
metadata = MetaData()

customers = Table('customers', metadata,
                  Column('id', Integer(), primary_key=True),
                  Column('first_name', String(100), nullable=False),
                  Column('last_name', String(100), nullable=False),
                  Column('username', String(50), nullable=False),
                  Column('email', String(200), nullable=False),
                  Column('address', String(200), nullable=False),
                  Column('town', String(50), nullable=False),
                  Column('created_on', DateTime(), default=datetime.now),
                  Column('updated_on', DateTime(), default=datetime.now, onupdate=datetime.now)
                  )

items = Table('items', metadata,
              Column('id', Integer(), primary_key=True),
              Column('name', String(200), nullable=False),
              Column('cost_price', Numeric(10, 2), nullable=False),
              Column('selling_price', Numeric(10, 2), nullable=False),
              Column('quantity', Integer(), nullable=False),
              CheckConstraint('quantity > 0', name='quantity_check')
              )

orders = Table('orders', metadata,
               Column('id', Integer(), primary_key=True),
               Column('customer_id', ForeignKey('customers.id')),
               Column('date_placed', DateTime(), default=datetime.now),
               Column('date_shipped', DateTime())
               )

order_lines = Table('order_lines', metadata,
                    Column('id', Integer(), primary_key=True),
                    Column('order_id', ForeignKey('orders.id')),
                    Column('item_id', ForeignKey('items.id')),
                    Column('quantity', Integer())
                    )

customers_list = [
    {
        "first_name": "John",
        "last_name": "Lara",
        "username": "johnlara",
        "email": "johnlara@mail.com",
        "address": "3073 Derek Drive",
        "town": "Norfolk"
    },
    {
        "first_name": "Sarah",
        "last_name": "Tomlin",
        "username": "sarahtomlin",
        "email": "sarahtomlin@mail.com",
        "address": "3572 Poplar Avenue",
        "town": "Norfolk"
    },
    {
        "first_name": "Pablo",
        "last_name": "Gibson",
        "username": "pablogibson",
        "email": "pablogibson@mail.com",
        "address": "3494 Murry Street",
        "town": "Peterbrugh"
    },
    {
        "first_name": "Pablo",
        "last_name": "Lewis",
        "username": "pablolewis",
        "email": "pablolewis@mail.com",
        "address": "3282 Jerry Toth Drive",
        "town": "Peterbrugh"
    },
]
items_list = [
    {
        "name": "Chair",
        "cost_price": 9.21,
        "selling_price": 10.81,
        "quantity": 5
    },
    {
        "name": "Pen",
        "cost_price": 3.45,
        "selling_price": 4.51,
        "quantity": 3
    },
    {
        "name": "Headphone",
        "cost_price": 15.52,
        "selling_price": 16.81,
        "quantity": 50
    },
    {
        "name": "Travel Bag",
        "cost_price": 20.1,
        "selling_price": 24.21,
        "quantity": 50
    },
    {
        "name": "Keyboard",
        "cost_price": 20.12,
        "selling_price": 22.11,
        "quantity": 50
    },
    {
        "name": "Monitor",
        "cost_price": 200.14,
        "selling_price": 212.89,
        "quantity": 50
    },
    {
        "name": "Watch",
        "cost_price": 100.58,
        "selling_price": 104.41,
        "quantity": 50
    },
    {
        "name": "Water Bottle",
        "cost_price": 20.89,
        "selling_price": 25.00,
        "quantity": 50
    },
]

order_list = [
    {
        "customer_id": 1
    },
    {
        "customer_id": 1
    }
]

order_line_list = [
    {
        "order_id": 1,
        "item_id": 1,
        "quantity": 5
    },
    {
        "order_id": 1,
        "item_id": 2,
        "quantity": 2
    },
    {
        "order_id": 1,
        "item_id": 3,
        "quantity": 1
    },
    {
        "order_id": 2,
        "item_id": 1,
        "quantity": 5
    },
    {
        "order_id": 2,
        "item_id": 2,
        "quantity": 5
    },
]


engine.execute(items.insert(), items_list)
engine.execute(customers.insert(), customers_list)
engine.execute(orders.insert(), order_list)
engine.execute(order_lines.insert(), order_line_list)
metadata.create_all(engine)

# Selecting records
s = select([customers])
str(s)
r = engine.execute(s)
r.fetchall()
rs = engine.execute(s)
for row in rs:
    print(row)
# Filtering records
s = select([items]).where(
    items.c.cost_price > 20)
str(s)
rs = engine.execute(s)
r.fetchall()
for row in rs:
    print(row)
    s = select([items]). \
        where(
        ~(items.c.quantity == 50) &
        (items.c.cost_price < 20)
    )
    engine.execute(s).fetchall()

# Comparison operators
s = select([orders]).where(
    orders.c.date_shipped == 'None')
str(s)
rs = engine.execute(s).fetchall()
for row in rs:
    print(row)
# Ordering results
s = select([items]).where(
    items.c.quantity > 10
).order_by(items.c.cost_price)
str(s)
rs = engine.execute(s).fetchall()
for row in rs:
    print(row)

# Grouping results

c = [
    func.count("*").label('count'),
    customers.c.town
]

s = select(c).group_by(customers.c.town)

print(s)
engine.execute(s).fetchall()
# Joins
s = select([
        orders.c.id.label('order_id'),
        orders.c.date_placed,
        order_lines.c.quantity,
        items.c.name,
]).select_from(
        orders.join(customers).join(order_lines).join(items)
    ).where and (customers.c.first_name == "John", customers.c.last_name == "Green",)

str(s)
rs = engine.execute(s).fetchall()
for row in rs:
    print(row)

