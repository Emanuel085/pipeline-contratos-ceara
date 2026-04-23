from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import psycopg2
import google.generativeai as genai
import warnings
from datetime import datetime as dt

warnings.filterwarnings("ignore", category=FutureWarning)

#CONFIGURAÇÕES

DB_PARAMS = {
    "host": "host.docker.internal",
    "database": "projeto_pipeline",
    "user": "postgres",
    "password": "Manel123",
    "port": "5432"
}

GEMINI_KEY = "AIzaSyBuGKQaqkFTKz0_36HPa9MOrlpPmEQBxsI"

#EXTRAÇÃO

def extrair_dados():
    url = "https://api-dados-abertos.cearatransparente.ce.gov.br/transparencia/contratos/contratos"

    params = {
        "page": 1,
        "data_assinatura_inicio": "01/01/2025",
        "data_assinatura_fim": "14/04/2026"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    dados = response.json()

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS contratos_api")

    cur.execute("""
    CREATE TABLE contratos_api (
        id TEXT,
        valor_total FLOAT,
        descricao_objeto TEXT
    )
    """)

    for item in dados["data"]:
        valor = item.get("valor_contrato", 0)

        try:
            valor = float(valor)
        except:
            valor = 0

        cur.execute("""
            INSERT INTO contratos_api VALUES (%s, %s, %s)
        """, (
            str(item.get("id")),
            valor,
            item.get("descricao_objeto", "").encode("utf-8", "ignore").decode("utf-8")
        ))

    conn.commit()
    cur.close()
    conn.close()

    print("Extração finalizada")

#CLASSIFICAÇÃO INTELIGENTE

def classificar_texto(descricao, resposta_ia):
    desc = descricao.lower()
    texto = (resposta_ia or "").lower()

    #REGRAS

    if any(p in desc for p in ["hospital", "saude", "médico", "medico", "enfermagem"]):
        return "Saúde"

    if any(p in desc for p in ["escola", "educacao", "ensino", "aluno", "creche"]):
        return "Educação"

    if any(p in desc for p in ["obra", "construção", "construcao", "reforma", "pavimentação", "rodovia"]):
        return "Infraestrutura"

    if any(p in desc for p in ["software", "sistema", "ti", "computador", "notebook", "tecnologia"]):
        return "Tecnologia"

    #FALLBACK IA

    if "saude" in texto or "saúde" in texto:
        return "Saúde"

    if "educacao" in texto or "educação" in texto:
        return "Educação"

    if "infraestrutura" in texto:
        return "Infraestrutura"

    if "tecnologia" in texto:
        return "Tecnologia"

    return "Outros"


def classificar_contratos():

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, valor_total, descricao_objeto
        FROM contratos_api
        ORDER BY valor_total DESC
        LIMIT 30
    """)

    dados = cur.fetchall()

    genai.configure(api_key=GEMINI_KEY)
    model = genai.GenerativeModel("gemini-pro")

    cur.execute("DROP TABLE IF EXISTS contratos_classificados")

    cur.execute("""
    CREATE TABLE contratos_classificados (
        id TEXT,
        valor_total FLOAT,
        descricao_objeto TEXT,
        classificacao_ia TEXT
    )
    """)

    for row in dados:
        id_, valor, descricao = row

        #Limpeza do texto
        descricao = (
            descricao
            .replace("¿", "-")
            .replace("\n", " ")
            .replace("  ", " ")
        )

        try:
            prompt = f"""
Classifique o contrato abaixo em UMA categoria:

Saúde
Educação
Infraestrutura
Tecnologia
Outros

Responda apenas com uma palavra.

Contrato:
{descricao}
"""
            resposta = model.generate_content(prompt)
            texto = resposta.text.strip() if resposta.text else ""

        except:
            texto = ""

        categoria = classificar_texto(descricao, texto)

        print(f"{id_} → {categoria}")

        cur.execute("""
            INSERT INTO contratos_classificados VALUES (%s, %s, %s, %s)
        """, (id_, valor, descricao, categoria))

    conn.commit()
    cur.close()
    conn.close()

    print("Classificação finalizada")

#RELATÓRIO

def gerar_relatorio():

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    cur.execute("SELECT * FROM contratos_classificados")
    dados = cur.fetchall()

    html_rows = ""
    for row in dados:
        html_rows += f"""
        <tr>
            <td>{row[0]}</td>
            <td>R$ {row[1]:,.2f}</td>
            <td><b>{row[3]}</b></td>
            <td>{row[2]}</td>
        </tr>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{
                font-family: Arial;
                background: linear-gradient(135deg,#1e3c72,#2a5298);
                padding:20px;
            }}

            .box {{
                background:white;
                padding:30px;
                border-radius:10px;
            }}

            h1 {{
                text-align:center;
                color:#1e3c72;
            }}

            table {{
                width:100%;
                border-collapse: collapse;
            }}

            th {{
                background:#1e3c72;
                color:white;
                padding:10px;
            }}

            td {{
                padding:8px;
                border-bottom:1px solid #ddd;
            }}

            tr:nth-child(even) {{
                background:#f2f2f2;
            }}
        </style>
    </head>

    <body>
        <div class="box">
            <h1>Relatório de Contratos</h1>
            <p>Gerado em: {dt.now().strftime('%d/%m/%Y %H:%M')}</p>

            <table>
                <tr>
                    <th>ID</th>
                    <th>Valor</th>
                    <th>Classificação</th>
                    <th>Descrição</th>
                </tr>

                {html_rows}

            </table>
        </div>
    </body>
    </html>
    """

    with open("/home/airflow/airflow/dags/relatorio_final.html", "w", encoding="utf-8") as f:
        f.write(html)

    cur.close()
    conn.close()

    print("Relatório gerado")

#DAG

default_args = {
    "owner": "carlos",
    "start_date": datetime(2026, 4, 14),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="pipeline_contratos_ceara",
    default_args=default_args,
    schedule="@daily",
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="extrair",
        python_callable=extrair_dados
    )

    t2 = PythonOperator(
        task_id="classificar",
        python_callable=classificar_contratos
    )

    t3 = PythonOperator(
        task_id="relatorio",
        python_callable=gerar_relatorio
    )

    t1 >> t2 >> t3
 