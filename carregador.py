import numpy 
import pandas as pd
import psycopg2 as pg

import criador as criador

usuario = "csk"
senha = "csk123"

filePath = 'dados_covid.csv'

db = criador.connectDatabase()
cursor = db.cursor()

dadosCovid = pd.read_csv( filePath )
filtro_cidades = (dadosCovid['city'] == 'Joinville') | (dadosCovid['city'] == 'Francisco Beltrão') | (dadosCovid['city'] == 'Dois Vizinhos') | (dadosCovid['city'] == 'Pato Branco') | (dadosCovid['city'] == 'Florianópolis')
dadosCovid = dadosCovid[filtro_cidades]

cidades = dadosCovid.drop_duplicates( 'city' )
for index, cidade in cidades.iterrows():
    try:
        cursor.execute( 
            """
            INSERT INTO cidade ( codigo_ibge, estado, nome_cidade, numero_habitantes )
            VALUES ( %(CODIGO_IBGE)s, %(ESTADO)s, %(NOME_CIDADE)s, %(NUMERO_HABITANTES)s );
            """, {
                'CODIGO_IBGE': cidade['city_ibge_code'],
                'ESTADO': cidade['state'],
                'NOME_CIDADE': cidade[ 'city' ],
                'NUMERO_HABITANTES': cidade[ 'estimated_population_2019' ]
            }
        )
    except Exception as err:
        print("Falha ao executar insert!")
        print(err)

dadosCovid = dadosCovid.sort_values( 'city' )
for index, casos in dadosCovid.iterrows():
    try:
        cursor.execute( 
            """
            INSERT INTO contagio ( data_registro, codigo_ibge, total_casos_confirmados, novos_casos_confirmados, total_mortes, novas_mortes )
            VALUES ( %(DATA_REGISTRO)s, %(CODIGO_IBGE)s, %(TOTAL_CASOS_CONFIRMADOS)s, %(NOVOS_CASOS_CONFIRMADOS)s, %(TOTAL_MORTES)s, %(NOVAS_MORTES)s );
            """, {
                'DATA_REGISTRO': casos['date'],
                'CODIGO_IBGE': casos['city_ibge_code'],
                'TOTAL_CASOS_CONFIRMADOS': casos['last_available_confirmed'],
                'NOVOS_CASOS_CONFIRMADOS': casos[ 'new_confirmed' ],
                'TOTAL_MORTES': casos[ 'last_available_deaths' ],
                'NOVAS_MORTES': casos[ 'new_deaths' ],
            }
        )
    except Exception as err:
        print("Falha ao executar insert!")
        print(err)

db.commit()

cursor.close()
db.close()
