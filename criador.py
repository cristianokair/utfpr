import psycopg2 as pg

nomeBanco = "grupo_extensao"
host = "localhost"
porta = 5432
usuario = "csk"
senha = "csk123"

def connectDatabase( nomeBanco = nomeBanco, host = host, porta = porta, usuario = usuario, senha = senha ):
    try:
        db = pg.connect( 
            dbname = nomeBanco,
            host = host,
            port = porta,
            user = usuario,
            password = senha
         )

    except Exception as err :
        print("Falha ao conectar ao banco de dados!")
        print(err)

    else:
        return db

def createDatabase( cursor, db ):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cidade ( codigo_ibge integer PRIMARY KEY, nome_cidade varchar, estado varchar, numero_habitantes integer );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS contagio ( codigo_ibge integer, data_registro date, total_casos_confirmados integer, novos_casos_confirmados integer, total_mortes integer, novas_mortes integer );
        """
    )

    db.commit()

db = connectDatabase()

cursor = db.cursor()
createDatabase( cursor, db )

cursor.close()
db.close()
