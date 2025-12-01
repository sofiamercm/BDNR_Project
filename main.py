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
            "1. Registro de productos",
            "2. Registro de usuario",
            "3. Gestión del carrito de compras",
            "4. Consulta del carrito",
            "5. Consulta de productos por rango de precio (general)",
            "6. Consulta por rango de precio (producto específico)",
            "7. Consulta los productos 'nuevos lanzamientos'",
            "8. Consulta de productos por categoría",     
            "9. Consulta de productos disponibles",
            "10.Consulta de wishlist de un usuario específico"
    ]
    show_submenu("MongoDB", consultas)

# Dgraph
def dgraph_menu():
    consultas = [
        "1. Registro de reseñas",
        "2. Registro de interacciones",
        "3. Recomendación basada en historial de compras",
        "4. Recomendaciones de productos similares",
        "5. Consulta de productos populares",
        "6. Recomendación por usuarios similares",
        "7. Recomendación por reseñas",
        "8. Análisis de comportamiento de navegación",
        "9. Recomendación por tendencia",
        "10.Recomendación por abandono de carrito"
    ]
    show_submenu("Dgraph", consultas)

# Cassandra
def cassandra_menu():
    consultas = [
        "1. Registro de vistas de productos",
        "2. Historial de búsqueda",
        "3. Registro de compras",
        "4. Tiempo de permanencia",
        "5. Clics en recomendaciones",
        "6. Productos en carritos",
        "7. Abandono de carrito",
        "8. Interacciones con filtros",
        "9. Sesiones de navegación",
        "10.Productos vistos no comprados",
        "11.Recomendaciones mostradas",
        "12.Wishlist",
        "13.Comparación de productos",
        "14.Métricas por categoría",
        "15.Feedback sobre recomendaciones"
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