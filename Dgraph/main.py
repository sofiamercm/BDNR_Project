import pydgraph
import json
import os
from model import (
    set_schema,
    get_reviews,
    get_user_interactions,
    get_purchase_recommendations,
    get_copurchased_products,
    get_popular_products,
    get_most_viewed_products,
    get_similar_users,
    get_top_rated_products,
    get_product_views,
    get_trending_products,
    get_abandoned_cart_recommendations,
    debug_uids
)
from populate import (
    load_csv,
    create_users,
    create_products,
    create_reviews,
    create_interactions,
    create_carts
)

# ------------------ Conexión ------------------
def connect_dgraph():
    client_stub = pydgraph.DgraphClientStub('localhost:9080')
    return pydgraph.DgraphClient(client_stub)

# ------------------ Reset de datos ------------------
def drop_data(client):
    op = pydgraph.Operation(drop_all=True)
    client.alter(op)
    print("🧹 Datos y schema borrados.")

# ------------------ Menú ------------------
def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_menu():
    print("══════════════════════════════════════")
    print("               DATABASE               ")
    print("══════════════════════════════════════")
    print("1. Configurar schema")
    print("2. Poblar datos")
    print("3. Obtener reseñas de un producto")
    print("4. Ver interacciones de un usuario")
    print("5. Recomendaciones por historial de compras")
    print("6. Productos comprados juntos (co-purchase)")
    print("7. Productos más populares")
    print("8. Productos más vistos")
    print("9. Usuarios similares")
    print("10. Productos mejor valorados")
    print("11. Análisis de vistas de un producto")
    print("12. Productos en tendencia")
    print("13. Recomendaciones por abandono de carrito")
    print("14. Borrar datos")
    print("15. Verificar datos")
    print("0. Salir")
    print("══════════════════════════════════════")

def main():
    client = connect_dgraph()

    while True:
        clear_screen()
        print_menu()
        choice = input("👉 Selecciona una opción: ")

        if choice == "1":
            set_schema(client)
            print("✅ Schema configurado correctamente.")
        elif choice == "2":
            # Poblar datos desde CSV
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

            print("✅ Datos poblados correctamente!")
        elif choice == "3":
            product_uid = input("Ingrese ID del producto (ej. p2): ")
            res = get_reviews(client, product_uid)
            print(json.dumps(res, indent=2))
        elif choice == "4":
            uid = input("Ingrese UID del usuario: ")
            res = get_user_interactions(client, uid)
            print(json.dumps(res, indent=2))
        elif choice == "5":
            uid = input("Ingrese UID del usuario: ")
            res = get_purchase_recommendations(client, uid)
            print(json.dumps(res, indent=2))
        elif choice == "6":
            pid = input("Ingrese UID del producto: ")
            res = get_copurchased_products(client, pid)
            print(json.dumps(res, indent=2))
        elif choice == "7":
            res = get_popular_products(client)
            print(json.dumps(res, indent=2))
        elif choice == "8":
            res = get_most_viewed_products(client)
            print(json.dumps(res, indent=2))
        elif choice == "9":
            uid = input("Ingrese UID del usuario: ")
            res = get_similar_users(client, uid)
            print(json.dumps(res, indent=2))
        elif choice == "10":
            res = get_top_rated_products(client)
            print(json.dumps(res, indent=2))
        elif choice == "11":
            pid = input("Ingrese UID del producto: ")
            res = get_product_views(client, pid)
            print(json.dumps(res, indent=2))
        elif choice == "12":
            res = get_trending_products(client)
            print(json.dumps(res, indent=2))
        elif choice == "13":
            uid = input("Ingrese UID del usuario: ")
            res = get_abandoned_cart_recommendations(client, uid)
            print(json.dumps(res, indent=2))
        elif choice == "14":
            drop_data(client)
        elif choice == "15":
            debug_uids(client)
        elif choice == "0":
            print("👋 Saliendo del programa...")
            break
        else:
            print("⚠️ Opción inválida.")

        input("\nPresiona ENTER para continuar...")

if __name__ == "__main__":
    main()
