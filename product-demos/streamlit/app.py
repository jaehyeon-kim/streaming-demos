import json
import asyncio

import aiohttp
import streamlit as st
from streamlit.delta_generator import DeltaGenerator

from utils import (
    load_records,
    create_metric_items,
    generate_metrics,
    create_options_items,
    generate_charts,
)


async def generate(
    metric_placeholder: DeltaGenerator, chart_placeholder: DeltaGenerator
):
    prev_values = {"num_orders": 0, "num_order_items": 0, "total_sales": 0}
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect("ws://localhost:8000/ws") as ws:
            async for msg in ws:
                metric_values, df = load_records(json.loads(msg.json()))
                generate_metrics(
                    metric_placeholder, create_metric_items(metric_values, prev_values)
                )
                generate_charts(chart_placeholder, create_options_items(df))
                prev_values = metric_values


st.set_page_config(
    page_title="theLook eCommerce",
    page_icon="✅",
    layout="wide",
)

st.title("theLook eCommerce Dashboard")

connect = st.checkbox("Connect to WS Server")
metric_placeholder = st.empty()
chart_placeholder = st.empty()

if connect:
    asyncio.run(
        generate(
            metric_placeholder=metric_placeholder, chart_placeholder=chart_placeholder
        )
    )
else:
    generate_metrics(metric_placeholder, None)
