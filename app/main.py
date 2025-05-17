from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)

app = FastAPI()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432")
    )

@app.get("/{marketplace}/{short_code}")
def redirect(marketplace: str, short_code: str):
    try:
        logging.info(f"Redirecionando marketplace={marketplace}, short_code={short_code}")
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT url_affiliates_product 
                    FROM tb_products_affiliates 
                    WHERE marketplace = %s AND short_code = %s
                """, (marketplace, short_code))
                
                result = cur.fetchone()
                logging.info(f"Resultado da consulta: {result}")

                if result:
                    return RedirectResponse(url=result['url_affiliates_product'])
                else:
                    raise HTTPException(status_code=404, detail="Short code not found")
    except Exception as e:
        logging.exception("Erro ao redirecionar")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")