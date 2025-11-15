def main_menu():
    while True:
        print("\n=== MENÚ PRINCIPAL ===")
        print("1. MongoDB")
        print("2. Dgraph")
        print("3. Cassandra")
        print("4. Salir")
        choice = input("Selecciona una opción: ")

        if choice == "1":
            mongodb_menu()
        elif choice == "2":
            dgraph_menu()
        elif choice == "3":
            cassandra_menu()
        elif choice == "4":
            print("Saliendo del sistema...")
            break
        else:
            print("Opción inválida. Intenta de nuevo.")

# MongoDB
def mongodb_menu():
    consultas = [
        "Registro de productos",
        "Registro de usuario",
        "Gestión del carrito de compras",
        "Consulta del carrito",
        "Consulta de productos por rango de precio (general)",
        "Consulta por rango de precio (producto específico)",
        "Consulta los productos 'nuevos lanzamientos'",
        "Consulta de productos por categoría",
        "Consulta de productos disponibles",
        "Consulta de wishlist de un usuario específico"
    ]
    show_submenu("MongoDB", consultas)

# Dgraph
def dgraph_menu():
    consultas = [
        "Registro de reseñas",
        "Registro de interacciones",
        "Recomendación basada en historial de compras",
        "Recomendaciones de productos similares",
        "Consulta de productos populares",
        "Recomendación por usuarios similares",
        "Recomendación por reseñas",
        "Análisis de comportamiento de navegación",
        "Recomendación por tendencia",
        "Recomendación por abandono de carrito"
    ]
    show_submenu("Dgraph", consultas)

# Cassandra
def cassandra_menu():
    consultas = [
        "Registro de vistas de productos",
        "Historial de búsqueda",
        "Registro de compras",
        "Tiempo de permanencia",
        "Clics en recomendaciones",
        "Productos en carritos",
        "Abandono de carrito",
        "Interacciones con filtros",
        "Sesiones de navegación",
        "Productos vistos no comprados",
        "Recomendaciones mostradas",
        "Wishlist",
        "Comparación de productos",
        "Métricas por categoría",
        "Feedback sobre recomendaciones"
    ]
    show_submenu("Cassandra", consultas)

# Submenú genérico
def show_submenu(db_name, consultas):
    while True:
        print(f"\n--- {db_name.upper()} ---")
        for i, consulta in enumerate(consultas, 1):
            print(f"{i}. {consulta}")
        print(f"{len(consultas)+1}. Volver al menú principal")
        choice = input("Selecciona una consulta: ")

        if choice.isdigit() and 1 <= int(choice) <= len(consultas):
            print(f"\nEjecutando: {consultas[int(choice)-1]} (simulado)")
        elif choice == str(len(consultas)+1):
            break
        else:
            print("Opción inválida. Intenta de nuevo.")

# Ejecutar el menú
if __name__ == "__main__":
    main_menu()