"use client";

import { useEffect, useState } from "react";
import { Checkbox } from "@nextui-org/react";
import ReactECharts, { EChartsOption } from "echarts-for-react";
import useWebSocket from "react-use-websocket";

import Metric, { MetricProps } from "@/components/metric";
import {
  getMetrics,
  createMetricItems,
  defaultMetrics,
  defaultMetricItems,
  createOptionsItems,
} from "@/lib/processing";

export default function Home() {
  const [toConnect, toggleToConnect] = useState(false);
  const [currMetrics, setCurrMetrics] = useState(defaultMetrics);
  const [prevMetrics, setPrevMetrics] = useState(defaultMetrics);
  const [metricItems, setMetricItems] = useState(defaultMetricItems);
  const [chartOptions, setChartOptions] = useState([] as EChartsOption[]);

  const { lastJsonMessage } = useWebSocket(
    "ws://localhost:8000/ws",
    {
      share: false,
      shouldReconnect: () => true,
    },
    toConnect
  );

  useEffect(() => {
    const records = JSON.parse(lastJsonMessage as string);
    if (!!records) {
      setPrevMetrics(currMetrics);
      setCurrMetrics(getMetrics(records));
      setMetricItems(createMetricItems(currMetrics, prevMetrics));
      setChartOptions(createOptionsItems(records));
    }
  }, [lastJsonMessage]);

  const createMetrics = (metricItems: MetricProps[]) => {
    return metricItems.map((item, i) => {
      return (
        <Metric
          key={i}
          label={item.label}
          value={item.value}
          delta={item.delta}
          is_currency={item.is_currency}
        ></Metric>
      );
    });
  };

  const createCharts = (chartOptions: EChartsOption[]) => {
    return chartOptions.map((option, i) => {
      return (
        <ReactECharts
          key={i}
          className="col-span-12 md:col-span-6"
          option={option}
          style={{ height: "500px" }}
        />
      );
    });
  };

  return (
    <div>
      <div className="mt-20">
        <div className="flex m-2 justify-between items-center">
          <h1 className="text-4xl font-bold">theLook eCommerce Dashboard</h1>
        </div>
        <div className="flex m-2 mt-5 justify-between items-center mt-5">
          <Checkbox
            color="primary"
            onChange={() => toggleToConnect(!toConnect)}
          >
            Connect to WS Server
          </Checkbox>
          ;
        </div>
      </div>
      <div className="grid grid-cols-12 gap-4 mt-5">
        {createMetrics(metricItems)}
      </div>
      <div className="grid grid-cols-12 gap-4 mt-5">
        {createCharts(chartOptions)}
      </div>
    </div>
  );
}
