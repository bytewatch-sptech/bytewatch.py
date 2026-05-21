import mysql.connector

class Database:
    def __init__(self):
        self.config = {
            "host": "localhost",
            "user": "root",
            "password": "09042023",
            "database": "monitoramento"
        }

    def macAddressExiste(self, mac_address):
        db = mysql.connector.connect(**self.config)
        cursor = db.cursor()
        
        try:
            sql = f"SELECT mac_address FROM servidor WHERE mac_address = '{mac_address}'"
            cursor.execute(sql)
            
            resultado = cursor.fetchone()
            
            servidorExiste = resultado is not None
            return servidorExiste
        
        finally:
            cursor.close()
            db.close()

database = Database()
