# `probar_mysql.py`

## Propósito
Actuar como un *Proof of Concept* (Prueba de concepto) o "health check" estático. Se encarga únicamente de inicializar un intento de túnel con el motor del servidor MySQL para confirmar que la base de la conexión responde satisfactoriamente y validar que se cuenta con un paquete `mysql-connector-python` debidamente instalado y accesible al ambiente.

## Comportamiento Subyacente
- Utiliza las credenciales estándar por defecto definidas para XAMPP/WAMP o un MySQL local (localhost, host = `127.0.0.1`, user = `root` y un password `""` vacío).
- Captura la excepción nativa resultante utilizando un bloque sintáctico tradicional (`try... except Exception`).
- Imprime directamente la causa del error por la salida de texto (System Output Console).ewq
- Llama y cierra prolija y seguidamente el canal con `conn.close()` en caso de éxito.
