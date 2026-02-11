import json, random, time, string
#from google.cloud import pubsub_v1
from datetime import datetime, timedelta

#publisher = pubsub_v1.PublisherClient()
## Please mention project_id & pub/sub topic name 
#topic_path = publisher.topic_path("PROJECT_ID", "trade-events")

def generate_trade():
    trade_id = random.randint(1000, 9999)
    version = random.randint(1, 5)
    party_id = ''.join(random.choices(string.ascii_uppercase, k=5)) + \
           ''.join(random.choices(string.digits, k=4)) + \
           random.choice(string.ascii_uppercase)
    book_id= random.randint(1, 999)
    product_id = random.choice(["option", "future", "delivery"])
    price = round(random.uniform(100.00, 500.00), 2)
    currency= random.choice(["INR", "USD", "EUR"])
    trade_date = datetime.now() - timedelta(seconds=random.randint(1, 3600))
    #trade_date= (datetime.now() + timedelta(days=random.randint(-5, 10))).strftime("%Y-%m-%d")
    maturity_date = (datetime.now() + timedelta(days=random.randint(-5, 10))).strftime("%Y-%m-%d")
    trade = {
        "trade_id": trade_id,
        "version": version,
        "party_id": party_id,
        "book_id": 100,
        "product_id":product_id,
        "price": price,
        "currency": currency,
        "trade_date": trade_date,
        "source":"broker_X",
        "maturity_date": maturity_date,
        "created_at": datetime.now().isoformat()
    }
    return trade

while True:
    trade = generate_trade()
    #publisher.publish(topic_path, json.dumps(trade).encode("utf-8"))
    print(f"Published trade: {trade}")
    time.sleep(2)