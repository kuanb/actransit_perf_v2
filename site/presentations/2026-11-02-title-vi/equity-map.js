const mapElement = document.querySelector("#equity-map");
const mapStatus = document.querySelector("#map-status");

function escapeHtml(value) {
  return String(value).replace(/[&<>'"]/g, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "'": "&#39;",
    '"': "&quot;",
  })[character]);
}

function routeTooltip(properties) {
  const classification = properties.minority_route ? "Provisional minority route" : "Other route";
  const className = properties.minority_route ? "" : " other";
  return `<div class="equity-tooltip">
    <strong>AC Transit Line ${escapeHtml(properties.route_id)}</strong>
    <span class="tooltip-class${className}">${classification}</span>
    ${properties.minority_mileage_share_pct.toFixed(1)}% of East Bay route mileage is in provisional minority block groups.
  </div>`;
}

function blockGroupTooltip(properties) {
  const classification = properties.minority ? "Provisional minority block group" : "Other block group";
  const className = properties.minority ? "" : " other";
  return `<div class="equity-tooltip">
    <strong>Census block group ${escapeHtml(properties.geoid)}</strong>
    <span class="tooltip-class${className}">${classification}</span>
    People of color: ${properties.poc_share_pct.toFixed(1)}%<br>
    ACS population: ${Number(properties.population).toLocaleString()}<br>
    Threshold: 74.0% people of color
  </div>`;
}

function routeStyle(feature) {
  return {
    color: feature.properties.minority_route ? "#b3364a" : "#167383",
    weight: 3,
    opacity: 0.9,
  };
}

function blockGroupStyle(feature) {
  return {
    color: feature.properties.minority ? "#d29aa4" : "#96c5cc",
    weight: 0.7,
    opacity: 0.7,
    fillColor: feature.properties.minority ? "#f1cbd2" : "#cae5e9",
    fillOpacity: 0.34,
  };
}

if (!mapElement || typeof L === "undefined") {
  if (mapStatus) {
    mapStatus.textContent = "The interactive map could not be loaded.";
    mapStatus.classList.add("error");
  }
} else {
  const map = L.map(mapElement, {
    scrollWheelZoom: false,
  });

  map.createPane("blockGroupsPane").style.zIndex = 350;
  map.createPane("routesPane").style.zIndex = 450;

  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    maxZoom: 19,
  }).addTo(map);

  fetch("data/map-data.geojson")
    .then((response) => {
      if (!response.ok) throw new Error(`map data request failed: ${response.status}`);
      return response.json();
    })
    .then((data) => {
      const blockGroups = L.geoJSON(data, {
        pane: "blockGroupsPane",
        filter: (feature) => feature.properties.kind === "block_group",
        style: blockGroupStyle,
        onEachFeature: (feature, layer) => {
          layer.bindTooltip(blockGroupTooltip(feature.properties), { sticky: true, opacity: 0.98 });
          layer.on({
            mouseover: () => layer.setStyle({ weight: 1.8, fillOpacity: 0.52 }),
            mouseout: () => blockGroups.resetStyle(layer),
          });
        },
      }).addTo(map);

      const routes = L.geoJSON(data, {
        pane: "routesPane",
        filter: (feature) => feature.properties.kind === "route",
        style: routeStyle,
        onEachFeature: (feature, layer) => {
          layer.bindTooltip(routeTooltip(feature.properties), { sticky: true, opacity: 0.98 });
          layer.on({
            mouseover: () => {
              layer.setStyle({ weight: 7, opacity: 1 });
              layer.bringToFront();
            },
            mouseout: () => routes.resetStyle(layer),
          });
        },
      }).addTo(map);

      L.control.layers(null, {
        "AC Transit routes": routes,
        "Census block groups": blockGroups,
      }, { collapsed: true }).addTo(map);

      const legend = L.control({ position: "bottomright" });
      legend.onAdd = () => {
        const element = L.DomUtil.create("div", "map-legend");
        element.innerHTML = `<strong>Map layers</strong>
          <span><i class="map-swatch route-minority"></i>Provisional minority route</span>
          <span><i class="map-swatch route-other"></i>Other route</span>
          <span><i class="map-swatch bg-minority"></i>Minority block group (≥74% POC)</span>
          <span><i class="map-swatch bg-other"></i>Other block group (&lt;74% POC)</span>`;
        return element;
      };
      legend.addTo(map);

      map.fitBounds(routes.getBounds(), { padding: [18, 18] });
      mapStatus.classList.add("hidden");
    })
    .catch(() => {
      mapStatus.textContent = "The interactive map data could not be loaded.";
      mapStatus.classList.add("error");
    });
}
