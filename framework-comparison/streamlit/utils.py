import json

import aiohttp
import pandas as pd
import streamlit as st
from streamlit.delta_generator import DeltaGenerator


def load_records(records: list):
    df = pd.DataFrame(records)
    assert set(
        [
            "order_id",
            "item_id",
            "country",
            "traffic_source",
            "age",
            "cost",
            "sale_price",
        ]
    ).issubset(df.columns)
    df["age"] = df["age"].astype(int)
    df["cost"] = df["cost"].astype(float)
    df["sale_price"] = df["sale_price"].astype(float)
    metric_values = {
        "num_orders": df["order_id"].nunique(),
        "num_order_items": df["item_id"].nunique(),
        "total_sales": round(df["sale_price"].sum()),
    }
    return metric_values, df


def create_metric_items(metric_values, prev_values):
    return [
        {
            "label": "Number of Orders",
            "value": metric_values["num_orders"],
            "delta": (metric_values["num_orders"] - prev_values["num_orders"]),
        },
        {
            "label": "Number of Order Items",
            "value": metric_values["num_order_items"],
            "delta": (
                metric_values["num_order_items"] - prev_values["num_order_items"]
            ),
        },
        {
            "label": "Total Sales",
            "value": f"$ {metric_values['total_sales']}",
            "delta": (metric_values["total_sales"] - prev_values["total_sales"]),
        },
    ]


def generate_metrics(placeholder: DeltaGenerator, metric_items: list = None):
    if metric_items is None:
        metric_items = [
            {"label": "Number of Orders", "value": 0, "delta": 0},
            {"label": "Number of Order Items", "value": 0, "delta": 0},
            {"label": "Total Sales", "value": 0, "delta": 0},
        ]
    with placeholder.container():
        for i, col in enumerate(st.columns(len(metric_items))):
            metric = metric_items[i]
            col.metric(
                label=metric["label"], value=metric["value"], delta=metric["delta"]
            )


async def consume(metric_placeholder: DeltaGenerator):
    prev_values = {"num_orders": 0, "num_order_items": 0, "total_sales": 0}
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect("ws://localhost:8000/ws") as ws:
            async for msg in ws:
                metric_values, df = load_records(json.loads(msg.json()))
                generate_metrics(
                    metric_placeholder, create_metric_items(metric_values, prev_values)
                )
                prev_values = metric_values
