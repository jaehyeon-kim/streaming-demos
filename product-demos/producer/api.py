import os
import logging
import asyncio

import sqlalchemy
from sqlalchemy import Engine, Connection
import pandas as pd
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

logging.getLogger().setLevel(logging.INFO)

try:
    LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "5"))
    REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "5"))
except ValueError:
    LOOKBACK_MINUTES = 5
    REFRESH_SECONDS = 5


def create_engine(
    user: str = os.getenv("DB_USER", "develop"),
    password: str = os.getenv("DB_PASS", "password"),
    host: str = os.getenv("DB_HOST", "localhost"),
    db_name: str = os.getenv("DB_NAME", "develop"),
    echo: bool = True,
) -> Engine:
    return sqlalchemy.create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}/{db_name}", echo=echo
    )


def read_from_db(conn: Connection, minutes: int = 0):
    sql = """
    SELECT
        u.id AS user_id
        , u.age
        , u.gender
        , u.country
        , u.traffic_source
        , o.order_id
        , o.id AS item_id
        , p.category
        , p.cost
        , o.status AS item_status
        , o.sale_price
        , o.created_at
    FROM users AS u
    JOIN order_items AS o ON u.id = o.user_id
    JOIN products AS p ON p.id = o.product_id
    """
    if minutes > 0:
        sql = f"{sql} WHERE o.created_at >= current_timestamp - interval '{minutes} minute'"
    else:
        sql = f"{sql} LIMIT 1"
    return pd.read_sql(sql=sql, con=conn)


app = FastAPI()


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_records(self, df: pd.DataFrame, websocket: WebSocket):
        records = df.to_json(orient="records")
        await websocket.send_json(records)


manager = ConnectionManager()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    engine = create_engine()
    conn = engine.connect()
    try:
        while True:
            df = read_from_db(conn=conn, minutes=LOOKBACK_MINUTES)
            logging.info(f"{df.shape[0]} records are fetched...")
            await manager.send_records(df, websocket)
            await asyncio.sleep(REFRESH_SECONDS)
    except WebSocketDisconnect:
        conn.close()
        engine.dispose()
        manager.disconnect(websocket)
