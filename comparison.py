import time
import mysql.connector
from pymongo import MongoClient


# Функція для виконання MySQL запитів
def run_mysql_query(query):
    try:
        # Підключення до бази даних MySQL
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="richard223",
            database="Pinterest"
        )
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        print("Query executed successfully in MySQL.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")


# Функція для виконання MongoDB операцій
def run_mongodb_operation(collection, operation_type, data=None, filter=None):
    try:
        # Підключення до MongoDB
        client = MongoClient("mongodb://localhost:27017/")  # Замініть на ваш MongoDB URI
        db = client["Pinterest"]
        coll = db[collection]

        start_time = time.time()

        if operation_type == "insert":
            coll.insert_one(data)
        elif operation_type == "delete":
            coll.delete_one(filter)
        elif operation_type == "update":
            coll.update_one(filter, {"$set": data})
        elif operation_type == "find":
            coll.find(filter)

        end_time = time.time()
        operation_time = end_time - start_time
        print(f"Time taken for MongoDB {operation_type}: {operation_time:.4f} seconds")

    except Exception as e:
        print(f"Error: {e}")


# Перевірка швидкодії вставки
def test_insertion():
    # Запит на вставку для MySQL
    insert_query_mysql = """
    INSERT INTO pins (user_id, title, description, image_url, created_at)
    VALUES (1, 'New Pin Title', 'Description of new pin', 'https://example.com/newimage.jpg', NOW());
    """
    start_time = time.time()
    run_mysql_query(insert_query_mysql)
    end_time = time.time()
    insertion_time_mysql = end_time - start_time
    print(f"Time taken for insertion in MySQL: {insertion_time_mysql:.4f} seconds")

    # Запит на вставку для MongoDB
    data_mongo = {
        "user_id": 1,
        "title": "New Pin Title",
        "description": "Description of new pin",
        "image_url": "https://example.com/newimage.jpg",
        "created_at": time.strftime('%Y-%m-%d %H:%M:%S')
    }
    run_mongodb_operation("pins", "insert", data=data_mongo)


# Перевірка швидкодії видалення
def test_deletion():
    # Запит на видалення для MySQL
    delete_query_mysql = """
    DELETE FROM pins WHERE title = 'New Pin Title';
    """
    start_time = time.time()
    run_mysql_query(delete_query_mysql)
    end_time = time.time()
    deletion_time_mysql = end_time - start_time
    print(f"Time taken for deletion in MySQL: {deletion_time_mysql:.4f} seconds")

    # Запит на видалення для MongoDB
    filter_mongo = {"title": "New Pin Title"}
    run_mongodb_operation("pins", "delete", filter=filter_mongo)


# Перевірка швидкодії оновлення
def test_update():
    # Запит на оновлення для MySQL
    update_query_mysql = """
    UPDATE pins SET description = 'Updated Description' WHERE title = 'New Pin Title';
    """
    start_time = time.time()
    run_mysql_query(update_query_mysql)
    end_time = time.time()
    update_time_mysql = end_time - start_time
    print(f"Time taken for update in MySQL: {update_time_mysql:.4f} seconds")

    # Запит на оновлення для MongoDB
    filter_mongo = {"title": "New Pin Title"}
    update_data_mongo = {"description": "Updated Description"}
    run_mongodb_operation("pins", "update", data=update_data_mongo, filter=filter_mongo)


# Перевірка швидкодії вибірки
def test_selection():
    # Запит на вибірку для MySQL
    select_query_mysql = """
    SELECT title, description, image_url FROM pins WHERE user_id = 1;
    """
    start_time = time.time()
    run_mysql_query(select_query_mysql)
    end_time = time.time()
    selection_time_mysql = end_time - start_time
    print(f"Time taken for selection in MySQL: {selection_time_mysql:.4f} seconds")

    # Запит на вибірку для MongoDB
    filter_mongo = {"user_id": 1}
    run_mongodb_operation("pins", "find", filter=filter_mongo)


# Комбінація всіх операцій для перевірки
def run_performance_tests():
    print("Testing insertion:")
    test_insertion()

    print("\nTesting deletion:")
    test_deletion()

    print("\nTesting update:")
    test_update()

    print("\nTesting selection:")
    test_selection()


# Основний блок для запуску
if __name__ == "__main__":
    print("Running MySQL and MongoDB performance tests...\n")
    run_performance_tests()
