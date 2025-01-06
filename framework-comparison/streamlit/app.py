import asyncio

import streamlit as st

from utils import generate_metrics, consume


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
    asyncio.run(consume(metric_placeholder=metric_placeholder))
else:
    generate_metrics(metric_placeholder, None)
