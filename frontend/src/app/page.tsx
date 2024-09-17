"use client";

import { useRef } from "react";
import { Options } from "highcharts";
import HighChart from "./components/HighChart";

// Define a type for the chart reference
interface ChartRef {
  chart: {
    reflow: () => void;
  };
}

// Define different chart configurations
const lineChartOptions: Options = {
  chart: {
    type: 'line'
  },
  title: {
    text: 'AAPL Stock Price'
  },
  series: [{
    type: 'line',
    name: 'AAPL',
    data: [
      [1220832000000, 22.56],
      [1220918400000, 21.67],
      [1221004800000, 21.66],
      // ... more data
    ],
    tooltip: {
      valueDecimals: 2
    }
  }]
};

const columnChartOptions: Options = {
  chart: {
    type: 'column'
  },
  title: {
    text: 'MSFT Stock Price'
  },
  series: [{
    type: 'column',
    name: 'MSFT',
    data: [
      [1220832000000, 25.76],
      [1220918400000, 24.87],
      [1221004800000, 24.88],
      // ... more data
    ],
    tooltip: {
      valueDecimals: 2
    }
  }]
};

const areaChartOptions: Options = {
  chart: {
    type: 'area'
  },
  title: {
    text: 'GOOGL Stock Price'
  },
  series: [{
    type: 'area',
    name: 'GOOGL',
    data: [
      [1220832000000, 30.66],
      [1220918400000, 29.57],
      [1221004800000, 29.58],
      // ... more data
    ],
    tooltip: {
      valueDecimals: 2
    }
  }]
};

const barChartOptions: Options = {
  chart: {
    type: 'bar'
  },
  title: {
    text: 'AMZN Stock Price'
  },
  series: [{
    type: 'bar',
    name: 'AMZN',
    data: [
      [1220832000000, 85.56],
      [1220918400000, 84.67],
      [1221004800000, 84.66],
      // ... more data
    ],
    tooltip: {
      valueDecimals: 2
    }
  }]
};

const pieChartOptions: Options = {
  chart: {
    type: 'pie'
  },
  title: {
    text: 'TSLA Stock Price'
  },
  series: [{
    type: 'pie',
    name: 'TSLA',
    data: [
      { name: 'Jan', y: 45.56 },
      { name: 'Feb', y: 44.67 },
      { name: 'Mar', y: 44.66 },
      // ... more data
    ],
    tooltip: {
      valueDecimals: 2
    }
  }]
};

const stackedChartOptions: Options = {
  chart: {
    type: 'column'
  },
  title: {
    text: 'FB Stock Price'
  },
  plotOptions: {
    column: {
      stacking: 'normal'
    }
  },
  series: [{
    type: 'column',
    name: 'FB',
    data: [
      [1220832000000, 15.56],
      [1220918400000, 14.67],
      [1221004800000, 14.66],
      // ... more data
    ],
    tooltip: {
      valueDecimals: 2
    }
  }]
};

export default function Home() {
  // Use MutableRefObject with the ChartRef type
  const chartRefs = useRef<ChartRef[]>([]);

  const setChartRef = (ref: ChartRef | null) => {
    if (ref) {
      chartRefs.current.push(ref);
    }
  };

  return (
    <main className="flex min-h-screen flex-col items-center justify-between p-24">
      <h1 className="text-6xl font-bold text-center mb-16">
        Financial Forecasting
      </h1>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
        <div style={{ width: "600px", height: "400px" }}>
          <HighChart options={lineChartOptions} />
        </div>
        <div style={{ width: "600px", height: "400px" }}>
          <HighChart options={columnChartOptions} />
        </div>
        <div style={{ width: "600px", height: "400px" }}>
          <HighChart options={areaChartOptions} />
        </div>
        <div style={{ width: "600px", height: "400px" }}>
          <HighChart options={barChartOptions} />
        </div>
        <div style={{ width: "600px", height: "400px" }}>
          <HighChart options={pieChartOptions} />
        </div>
        <div style={{ width: "600px", height: "400px" }}>
          <HighChart options={stackedChartOptions} />
        </div>
      </div>
    </main>
  );
}
