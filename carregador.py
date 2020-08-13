import pandas as pd
import numpy as np
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

df_completo = pd.read_csv("covid19_casos_brasil.csv")


colsCidade = ['city_ibge_code', 'city', 'state']
filtro_cidades = (df_completo['city'] == 'Joinville') | (df_completo['city'] == 'Francisco Beltrão') | (df_completo['city'] == 'Dois Vizinhos') | (df_completo['city'] == 'Pato Branco') | (df_completo['city'] == 'Florianópolis')
df_completo_cidade = df_completo[filtro_cidades][colsCidade]
df_cidade = df_completo_cidade.tail()

for index, row in df_cidade.iterrows():
    cur.execute("""
        INSERT INTO cidade (city_ibge_code, city, state)
        VALUES (%(a)s, %(b)s, %(c)s);
        """,{'a': row['city_ibge_code'],
             'b': row['city'],
             'c': row['state']
            })
conn.commit()

colsCasos = ['city_ibge_code', 'last_available_confirmed', 'last_available_deaths', 'new_confirmed', 'new_deaths']
df_completo_casos = df_completo[filtro_cidades][colsCasos]
df_cidade = df_completo_casos

for index, row in df_cidade.iterrows():
    cur.execute("""
        INSERT INTO casos (city_ibge_code, last_available_confirmed, last_available_deaths, new_confirmed, new_deaths)
        VALUES (%(a)s, %(b)s, %(c)s, %(d)s, %(e)s);
        """,{'a': row['city_ibge_code'],
             'b': row['last_available_confirmed'],
             'c': row['last_available_deaths'],
             'd': row['new_confirmed'],
             'e': row['new_deaths']
            })
conn.commit()
cur.close()
conn.close()