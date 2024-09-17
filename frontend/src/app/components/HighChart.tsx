// components/HighChart.tsx
import React, { forwardRef } from "react";
import Highcharts from "highcharts";
import HighchartsReact from "highcharts-react-official";

interface HighChartProps {
  options: Highcharts.Options;
}

const HighChart = forwardRef<HighchartsReact.RefObject, HighChartProps>(
  (props, ref) => (
    <HighchartsReact
      highcharts={Highcharts}
      ref={ref}
      containerProps={{ style: { width: "100%", height: "100%" } }}
      options={props.options}
    />
  )
);

// Adding displayName for better debugging and linting
HighChart.displayName = "HighChart";

export default HighChart;
