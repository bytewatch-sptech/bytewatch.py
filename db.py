import mysql.connector

class Database:
    def __init__(self):
        self.db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="0262",
        database="monitoramento"
        )

        self.conn = self.db.cursor()

    def macAddressExiste(self, macAddress):
        self.conn.execute(f"SELECT * FROM servidor WHERE mac_address = '{macAddress}'")
        resultado = self.conn.fetchall()
        servidorExiste = len(resultado) > 0
        self.fechar_conexao()
        return servidorExiste

    def fechar_conexao(self):
        self.conn.close()
        self.db.close()

database = Database()
