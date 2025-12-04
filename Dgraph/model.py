
import datetime
import json
import json

import pydgraph

def set_schema(client):
    schema = """
    # ----------- Tipos de Nodos -----------
    type User {
        name
        email
        joined_at
        reviewed
        purchased
        interacted
        has_cart
    }
    
    type Product {
        name
        category
        price
        reviews
        purchased_with
        interactions
    }
    
    type Review {
        rating
        comment
        created_at
        reviewed_by
        of_product
    }
    
    type Interaction {
        interaction_type
        timestamp
        duration
        by_user
        with_product
    }
    
    type Cart {
        created_at
        contains
    }
    
    # ----------- Predicados con Índice -----------
    # ---------- User ----------
    name: string @index(term) .
    email: string @index(exact) .
    joined_at: datetime .
    
    reviewed: [uid] .        # User → Review
    purchased: [uid] .       # User → Product
    interacted: [uid] .      # User → Interaction
    has_cart: uid .          # User → Cart
    
    
    # ---------- Product ----------
    category: string @index(exact) .
    price: float .
    
    reviews: [uid] .         # Product → Review
    purchased_with: [uid] .  # Product → Product
    interactions: [uid] .    # Product → Interaction
    
    
    # ---------- Review ----------
    rating: float @index(float) .
    comment: string @index(fulltext) .
    created_at: datetime @index(hour) .
    
    reviewed_by: uid .       # Review → User
    of_product: uid .        # Review → Product
    
    
    # ---------- Interaction ----------
    interaction_type: string @index(exact) .
    timestamp: datetime @index(hour) .
    duration: float .
    
    by_user: uid .           # Interaction → User
    with_product: uid .      # Interaction → Product
    
    
    # ---------- Cart ----------
    created_at: datetime @index(day) .
    contains: [uid] .        # Cart → Product
    """
    return client.alter(pydgraph.Operation(schema=schema))
    
    
# QUERIES

# Obtener reseñas de un producto
def get_reviews(client, product_uid):
    query = """
    query getReviews($pid: string) {
      product(func: uid($pid)) {
        name
        reviews {
          rating
          comment
          created_at
          reviewed_by {
            name
            email
          }
        }
      }
    }
    """
    variables = {"pid": product_uid}
    res = client.txn(read_only=True).query(query, variables)
    return res.json

# Registro de interacciones (view, click, purchase)
def get_user_interactions(client, user_uid):
    query = """
    query userInteractions($uid: string) {
      user(func: uid($uid)) {
        name
        interacted(orderdesc: timestamp, first: 20) {
          interaction_type
          timestamp
          duration
          with_product {
            name
          }
        }
      }
    }
    """
    variables = {"uid": user_uid}
    res = client.txn(read_only=True).query(query, variables)
    return res.json

# Recomendación basada en historial de compras
def get_purchase_recommendations(client, user_uid):
    query = """
    query recommendations($uid: string) {
      user(func: uid($uid)) {
        purchased {
          name
          purchased_with {
            name
            price
          }
        }
      }
    }
    """
    variables = {"uid": user_uid}
    res = client.txn(read_only=True).query(query, variables)
    return res.json

# Recomendación basada en productos comprados juntos (co-purchase)
def get_copurchased_products(client, product_uid):
    query = """
    query copurchase($pid: string) {
      product(func: uid($pid)) {
        name
        purchased_with {
          name
          price
        }
      }
    }
    """
    variables = {"pid": product_uid}
    res = client.txn(read_only=True).query(query, variables)
    return res.json

# Productos Populares
# Más comprados
def get_popular_products(client):
    query = """
    {
      popularProducts(func: type(Product), orderdesc: count(~purchased), first: 10) {
        name
        total_buys: count(~purchased)
      }
    }
    """
    res = client.txn(read_only=True).query(query)
    return res.json

# Más vistos
def get_most_viewed_products(client):
    query = """
    {
      mostViewed(func: type(Product), orderdesc: count(interactions), first: 10) {
        name
        view_count: count(interactions)
      }
    }
    """
    res = client.txn(read_only=True).query(query)
    return res.json

# Recomendación por usuarios similares
def get_similar_users(client, user_uid):
    query = """
    query similarUsers($uid: string) {
      var(func: uid($uid)) {
        purchasedProducts as purchased
      }

      similar(func: type(User)) @filter(uid_in(purchased, uid(purchasedProducts))) {
        name
        purchased {
          name
        }
      }
    }
    """
    variables = {"uid": user_uid}
    res = client.txn(read_only=True).query(query, variables)
    return res.json

# Recomendación por productos con reseñas rating > 4
def get_top_rated_products(client):
    query = """
    {
      topRated(func: type(Product)) @cascade {
        name
        reviews @filter(gt(rating, 4)) {
          rating
        }
      }
    }
    """
    res = client.txn(read_only=True).query(query)
    return res.json

# Análisis de comportamiento (vistas, duración, abandono)
def get_product_views(client, product_uid):
    query = """
    query productViews($pid: string) {
      product(func: uid($pid)) {
        name
        interactions @filter(eq(interaction_type, "view")) {
          timestamp
          duration
          by_user {
            name
          }
        }
      }
    }
    """
    variables = {"pid": product_uid}
    res = client.txn(read_only=True).query(query, variables)
    return res.json

# Recomendación por tendencia
def get_trending_products(client):
    query = """
    {
      trending(func: type(Product)) 
        @filter(gt(count(~purchased), 20)) 
        @cascade {
          name
          popularity: count(~purchased)
          good_reviews: count(reviews @filter(gt(rating, 4)))
        }
    }
    """
    res = client.txn(read_only=True).query(query)
    return res.json

# Recomendación por abandono de carrito
def get_abandoned_cart_recommendations(client, user_uid):
    query = """
    query abandoned($uid: string) {
      user(func: uid($uid)) {
        name
        has_cart {
          contains {
            name
            price
            purchased_with {
              name
            }
          }
        }
      }
    }
    """
    variables = {"uid": user_uid}
    res = client.txn(read_only=True).query(query, variables)
    return res.json

    
    
    
