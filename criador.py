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

cur.execute("CREATE TABLE IF NOT EXISTS cidade (id serial PRIMARY KEY, city_ibge_code integer, city varchar, state varchar);")
cur.execute("CREATE TABLE IF NOT EXISTS casos (id serial PRIMARY KEY, city_ibge_code integer, date last_available_confirmed integer, last_available_deaths integer, new_confirmed integer, new_deaths integer);")
conn.commit()
cur.close()
conn.close()