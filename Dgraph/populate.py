import csv
import pydgraph

def connect_dgraph():
    client_stub = pydgraph.DgraphClientStub('localhost:9080')
    client = pydgraph.DgraphClient(client_stub)
    return client

def load_csv(filename):
    with open(filename, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)

def create_users(client, users):
    mutations = []
    for u in users:
        mutations.append({
            "uid": f"_:{u['user_id']}",
            "dgraph.type": "User",
            "name": u['name'],
            "email": u['email'],
            "joined_at": u['joined_at']
        })
    txn = client.txn()
    try:
        txn.mutate(set_obj=mutations)
        txn.commit()
        print(f"{len(users)} users inserted.")
    finally:
        txn.discard()

def create_products(client, products):
    mutations = []
    for p in products:
        mutations.append({
            "uid": f"_:{p['product_id']}",
            "dgraph.type": "Product",
            "name": p['name'],
            "category": p['category'],
            "price": float(p['price'])
        })
    txn = client.txn()
    try:
        txn.mutate(set_obj=mutations)
        txn.commit()
        print(f"{len(products)} products inserted.")
    finally:
        txn.discard()

def create_reviews(client, reviews):
    mutations = []
    for r in reviews:
        mutations.append({
            "uid": f"_:{r['review_id']}",
            "dgraph.type": "Review",
            "rating": float(r['rating']),
            "comment": r['comment'],
            "review_created_at": r['created_at'],
            "reviewed_by": {"uid": f"_:{r['reviewed_by_uid']}"},
            "of_product": {"uid": f"_:{r['of_product_uid']}"}
        })
    txn = client.txn()
    try:
        txn.mutate(set_obj=mutations)
        txn.commit()
        print(f"{len(reviews)} reviews inserted.")
    finally:
        txn.discard()

def create_interactions(client, interactions):
    mutations = []
    for i in interactions:
        mutations.append({
            "uid": f"_:{i['interaction_id']}",
            "dgraph.type": "Interaction",
            "interaction_type": i['interaction_type'],
            "timestamp": i['timestamp'],
            "duration": float(i['duration']),
            "by_user": {"uid": f"_:{i['by_user_uid']}"},
            "with_product": {"uid": f"_:{i['with_product_uid']}"}
        })
    txn = client.txn()
    try:
        txn.mutate(set_obj=mutations)
        txn.commit()
        print(f"{len(interactions)} interactions inserted.")
    finally:
        txn.discard()

def create_carts(client, carts):
    mutations = []
    for c in carts:
        contains_list = [{"uid": f"_:{pid.strip()}"} for pid in c['contains_product_ids'].split(";")]
        mutations.append({
            "uid": f"_:{c['cart_id']}",
            "dgraph.type": "Cart",
            "cart_created_at": c['created_at'],
            "contains": contains_list,
            "owner": {"uid": f"_:{c['user_uid']}"}  # relación con el usuario
        })
    txn = client.txn()
    try:
        txn.mutate(set_obj=mutations)
        txn.commit()
        print(f"{len(carts)} carts inserted.")
    finally:
        txn.discard()

if __name__ == "__main__":
    client = connect_dgraph()

    users = load_csv("data/users.csv")
    products = load_csv("data/products.csv")
    reviews = load_csv("data/reviews.csv")
    interactions = load_csv("data/interactions.csv")
    carts = load_csv("data/carts.csv")

    create_users(client, users)
    create_products(client, products)
    create_reviews(client, reviews)
    create_interactions(client, interactions)
    create_carts(client, carts)

    print("All data populated successfully.")
