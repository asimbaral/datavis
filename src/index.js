import React from "react";
import ReactDOM from "react-dom";

import "./index.css";

import MapChart from "./MapChart";

function App() {
  return (
    <div>
      <MapChart />
    </div>
  );
}

const rootElement = document.getElementById("root");
ReactDOM.render(<App />, rootElement);
