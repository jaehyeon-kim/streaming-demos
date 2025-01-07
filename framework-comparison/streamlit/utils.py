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
    df["sale_price"] = df["sale_price"].astype(float).round(1)
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


def to_title_case(s: str):
    return s.replace("_", " ").title()


def create_chart_configs(df: pd.DataFrame):
    chart_cols = [
        {"x": "country", "y": "sale_price"},
        {"x": "traffic_source", "y": "sale_price"},
    ]
    configs = []
    for col in chart_cols:
        data = df[[col["x"], col["y"]]].groupby(col["x"]).sum().reset_index()
        spec = {
            "title": f"Revenue by {to_title_case(col['x'])}",
            "mark": "bar",
            "encoding": {
                "x": {
                    "field": col["x"],
                    "type": "nominal",
                    "title": to_title_case(col["x"]),
                    "sort": "-y",
                },
                "y": {"field": col["y"], "type": "quantitative", "title": "Revenue"},
                "tooltip": [
                    {
                        "field": col["x"],
                        "type": "nominal",
                        "title": to_title_case(col["x"]),
                    },
                    {"field": col["y"], "type": "quantitative", "title": "Revenue"},
                ],
                "color": {
                    "field": col["x"],
                    "type": "nominal",
                    "title": to_title_case(col["x"]),
                },
            },
        }
        configs.append((data, spec))
    return configs


def generate_charts(placeholder: DeltaGenerator, chart_configs: list):
    with placeholder.container():
        for i, col in enumerate(st.columns(len(chart_configs))):
            data, spec = chart_configs[i]
            col.vega_lite_chart(data=data, spec=spec, use_container_width=True)
