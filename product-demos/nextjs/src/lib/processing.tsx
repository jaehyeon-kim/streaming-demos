import { MetricProps } from "@/components/metric";

export interface Metrics {
  num_orders: number;
  num_order_items: number;
  total_sales: number;
}

export interface Record {
  user_id: string;
  age: number;
  gender: string;
  country: string;
  traffic_source: string;
  order_id: string;
  item_id: string;
  category: string;
  item_status: string;
  sale_price: number;
  created_at: number;
}

export const defaultMetrics: Metrics = {
  num_orders: 0,
  num_order_items: 0,
  total_sales: 0,
};

export const defaultMetricItems: MetricProps[] = [
  { label: "Number of Orders", value: 0, delta: 0, is_currency: false },
  { label: "Number of Order Items", value: 0, delta: 0, is_currency: false },
  { label: "Total Sales", value: 0, delta: 0, is_currency: true },
];

export function getMetrics(records: Record[]) {
  const num_orders = [...new Set(records.map((r) => r.order_id))].length;
  const num_order_items = [...new Set(records.map((r) => r.item_id))].length;
  const total_sales = Math.round(
    records.map((r) => Number(r.sale_price)).reduce((a, b) => a + b, 0)
  );
  return {
    num_orders: num_orders,
    num_order_items: num_order_items,
    total_sales: total_sales,
  };
}

export function createMetricItems(currMetrics: Metrics, prevMetrics: Metrics) {
  const labels = [
    { label: "Number of Orders", metric: "num_orders", is_currency: false },
    {
      label: "Number of Order Items",
      metric: "num_order_items",
      is_currency: false,
    },
    { label: "Total Sales", metric: "total_sales", is_currency: true },
  ];
  return labels.map((obj) => {
    const label = obj.label;
    const value = currMetrics[obj.metric as keyof Metrics];
    const delta =
      currMetrics[obj.metric as keyof Metrics] -
      prevMetrics[obj.metric as keyof Metrics];
    const is_currency = obj.is_currency;
    return {
      label,
      value,
      delta,
      is_currency,
    };
  });
}

export function createOptionsItems(records: Record[]) {
  const chartCols = [
    { x: "country", y: "sale_price" },
    { x: "traffic_source", y: "sale_price" },
  ];
  return chartCols.map((col) => {
    // key is string but it throws the following error. Change the type to 'string | number'.
    // Argument of type 'string | number' is not assignable to parameter of type 'string'.
    // Type 'number' is not assignable to type 'string'.ts(2345)
    const recordsMap = new Map<string | number, number>();
    for (const r of records) {
      recordsMap.set(
        r[col.x as keyof Record],
        (recordsMap.get(r[col.x as keyof Record]) || 0) +
          Number(r[col.y as keyof Record])
      );
    }
    const recordsItems = Array.from(recordsMap, ([x, y]) => ({ x, y })).sort(
      (a, b) => (a.y > b.y ? -1 : 1)
    );
    const suffix = col.x
      .split("_")
      .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
      .join(" ");
    return {
      title: { text: "Revenue by ".concat(suffix) },
      yAxis: { type: "value" },
      xAxis: {
        type: "category",
        data: recordsItems.map((r) => r.x),
        axisLabel: { show: true, rotate: 75 },
      },
      series: [
        {
          data: recordsItems.map((r) => Math.round(r.y)),
          type: "bar",
          colorBy: "data",
        },
      ],
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    };
  });
}
