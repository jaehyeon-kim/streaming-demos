import os
import logging
import asyncio

from sqlalchemy import create_engine, Engine, Connection
import pandas as pd
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

logging.basicConfig(level=logging.INFO)

try:
    LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "5"))
    REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "5"))
except ValueError:
    LOOKBACK_MINUTES = 5
    REFRESH_SECONDS = 5


def get_db_engine() -> Engine:
    """Creates and returns a SQLAlchemy engine."""
    user = os.getenv("DB_USER", "develop")
    password = os.getenv("DB_PASS", "password")
    host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME", "develop")

    try:
        return create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}/{db_name}", echo=True
        )
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        raise


def fetch_data(conn: Connection, minutes: int = 0):
    """Fetches data from the database with an optional lookback filter."""
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
    try:
        return pd.read_sql(sql=sql, con=conn)
    except Exception as e:
        logging.error(f"Error reading from database: {e}")
        return pd.DataFrame()


app = FastAPI()


class ConnectionManager:
    """Manages WebSocket connections."""

    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logging.info(f"New WebSocket connection: {websocket.client}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logging.info(f"WebSocket disconnected: {websocket.client}")

    async def send_data(self, df: pd.DataFrame, websocket: WebSocket):
        """Converts DataFrame to JSON and sends it via WebSocket."""
        if not df.empty:
            await websocket.send_json(df.to_json(orient="records"))


manager = ConnectionManager()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Handles WebSocket connections and continuously streams data."""
    await manager.connect(websocket)

    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            while True:
                df = fetch_data(conn, LOOKBACK_MINUTES)
                logging.info(f"Fetched {df.shape[0]} records from database")
                await manager.send_data(df, websocket)
                await asyncio.sleep(REFRESH_SECONDS)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logging.error(f"WebSocket error: {e}")
    finally:
        engine.dispose()
