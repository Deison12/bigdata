import mysql.connector

try:
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password=""
    )
    print("Conexión exitosa a MySQL")
    conn.close()
except Exception as e:
    print("Error de conexión:", e)