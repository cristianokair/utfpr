import psycopg2

%matplotlib inline
plt.rcParams["figure.figsize"] = [15, 15]

host = "localhost"
nomeBanco = "grupo_extensão"
usuario = "csk"
senha = "csk123"
porta = 5432

def conectarBD(hostName, dbName, userName, pw, port):
    try:
        conn = psycopg2.connect(
            host = hostName,
            dbname = dbName,
            user = userName,
            password = pw,
            port = port
        )
    except Exception as err:
        print("Houve um erro!")
        print(err)
    
    return conn

conn = conectarBD( host, nomeBanco, usuario, senha, porta )

cur = conn.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS cidade (id serial PRIMARY KEY, codigo_ibge integer, nome_cidade varchar, estado varchar, numero_habitantes integer);")
cur.execute("CREATE TABLE IF NOT EXISTS casos (id serial PRIMARY KEY, codigo_ibge integer, data_registro date, total_mortes integer, novas_mortes integer, total_casos_confirmados integer, novos_casos_confirmados integer);")
conn.commit()
cur.close()
conn.close()