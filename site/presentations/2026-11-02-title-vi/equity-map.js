const mapElement = document.querySelector("#equity-map");
const mapStatus = document.querySelector("#map-status");

const SOURCE_ID = "title-vi-equity";
const BLOCK_GROUP_FILL_LAYER = "title-vi-block-groups-fill";
const BLOCK_GROUP_LINE_LAYER = "title-vi-block-groups-line";
const ROUTE_LAYER = "title-vi-routes";

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
  const minorityRoute = properties.minority_route === true || properties.minority_route === "true";
  const classification = minorityRoute ? "Provisional minority route" : "Other route";
  const className = minorityRoute ? "" : " other";
  return `<div class="equity-tooltip">
    <strong>AC Transit Line ${escapeHtml(properties.route_id)}</strong>
    <span class="tooltip-class${className}">${classification}</span>
    ${Number(properties.minority_mileage_share_pct).toFixed(1)}% of East Bay route mileage is in provisional minority block groups.
  </div>`;
}

function blockGroupTooltip(properties) {
  const minority = properties.minority === true || properties.minority === "true";
  const classification = minority ? "Provisional minority block group" : "Other block group";
  const className = minority ? "" : " other";
  return `<div class="equity-tooltip">
    <strong>Census block group ${escapeHtml(properties.geoid)}</strong>
    <span class="tooltip-class${className}">${classification}</span>
    People of color: ${Number(properties.poc_share_pct).toFixed(1)}%<br>
    ACS population: ${Number(properties.population).toLocaleString()}<br>
    Threshold: 74.0% people of color
  </div>`;
}

function extendBounds(bounds, coordinates) {
  if (!Array.isArray(coordinates)) return;
  if (typeof coordinates[0] === "number" && typeof coordinates[1] === "number") {
    bounds.extend(coordinates);
    return;
  }
  coordinates.forEach((coordinate) => extendBounds(bounds, coordinate));
}

function addLegend(map) {
  map.addControl({
    onAdd() {
      const element = document.createElement("div");
      element.className = "mapboxgl-ctrl map-legend";
      element.innerHTML = `<strong>Map layers</strong>
        <span><i class="map-swatch route-minority"></i>Provisional minority route</span>
        <span><i class="map-swatch route-other"></i>Other route</span>
        <span><i class="map-swatch bg-minority"></i>Minority block group (≥74% POC)</span>
        <span><i class="map-swatch bg-other"></i>Other block group (&lt;74% POC)</span>`;
      return element;
    },
    onRemove() {},
  }, "bottom-left");
}

function addLayerControl(map) {
  map.addControl({
    onAdd() {
      const element = document.createElement("div");
      element.className = "mapboxgl-ctrl map-layer-control";
      element.innerHTML = `<strong>Show layers</strong>
        <label><input type="checkbox" data-layer="routes" checked> AC Transit routes</label>
        <label><input type="checkbox" data-layer="block-groups" checked> Census block groups</label>`;
      element.querySelectorAll("input").forEach((input) => {
        input.addEventListener("change", () => {
          const visibility = input.checked ? "visible" : "none";
          const layers = input.dataset.layer === "routes"
            ? [ROUTE_LAYER]
            : [BLOCK_GROUP_FILL_LAYER, BLOCK_GROUP_LINE_LAYER];
          layers.forEach((layer) => map.setLayoutProperty(layer, "visibility", visibility));
        });
      });
      return element;
    },
    onRemove() {},
  }, "top-right");
}

function initializeMap(data) {
  mapboxgl.accessToken = MAPBOX_TOKEN;
  const map = new mapboxgl.Map({
    container: "equity-map",
    style: "mapbox://styles/mapbox/light-v11",
    center: [-122.2, 37.78],
    zoom: 11,
    attributionControl: true,
  });
  map.addControl(new mapboxgl.NavigationControl(), "bottom-right");

  map.on("load", () => {
    map.addSource(SOURCE_ID, { type: "geojson", data, generateId: true });
    map.addLayer({
      id: BLOCK_GROUP_FILL_LAYER,
      type: "fill",
      source: SOURCE_ID,
      filter: ["==", ["get", "kind"], "block_group"],
      paint: {
        "fill-color": ["case", ["==", ["get", "minority"], true], "#f1cbd2", "#cae5e9"],
        "fill-opacity": ["case", ["boolean", ["feature-state", "hover"], false], 0.52, 0.34],
      },
    });
    map.addLayer({
      id: BLOCK_GROUP_LINE_LAYER,
      type: "line",
      source: SOURCE_ID,
      filter: ["==", ["get", "kind"], "block_group"],
      paint: {
        "line-color": ["case", ["==", ["get", "minority"], true], "#d29aa4", "#96c5cc"],
        "line-width": ["case", ["boolean", ["feature-state", "hover"], false], 1.8, 0.7],
        "line-opacity": 0.7,
      },
    });
    map.addLayer({
      id: ROUTE_LAYER,
      type: "line",
      source: SOURCE_ID,
      filter: ["==", ["get", "kind"], "route"],
      layout: { "line-join": "round", "line-cap": "round" },
      paint: {
        "line-color": ["case", ["==", ["get", "minority_route"], true], "#b3364a", "#167383"],
        "line-width": ["case", ["boolean", ["feature-state", "hover"], false], 7, 3],
        "line-opacity": ["case", ["boolean", ["feature-state", "hover"], false], 1, 0.9],
      },
    });

    addLegend(map);
    addLayerControl(map);

    const bounds = new mapboxgl.LngLatBounds();
    data.features
      .filter((feature) => feature.properties.kind === "route")
      .forEach((feature) => extendBounds(bounds, feature.geometry.coordinates));
    if (!bounds.isEmpty()) map.fitBounds(bounds, { padding: 24, maxZoom: 12.5, duration: 0 });

    const popup = new mapboxgl.Popup({ closeButton: false, closeOnClick: false, maxWidth: "260px", offset: 8 });
    let hoveredFeatureID = null;

    function clearHover() {
      if (hoveredFeatureID !== null) {
        map.setFeatureState({ source: SOURCE_ID, id: hoveredFeatureID }, { hover: false });
        hoveredFeatureID = null;
      }
      map.getCanvas().style.cursor = "";
      popup.remove();
    }

    function showFeature(event) {
      const features = map.queryRenderedFeatures(event.point, {
        layers: [ROUTE_LAYER, BLOCK_GROUP_FILL_LAYER],
      });
      if (!features.length) {
        clearHover();
        return;
      }

      const feature = features[0];
      if (hoveredFeatureID !== feature.id) {
        if (hoveredFeatureID !== null) {
          map.setFeatureState({ source: SOURCE_ID, id: hoveredFeatureID }, { hover: false });
        }
        hoveredFeatureID = feature.id;
        map.setFeatureState({ source: SOURCE_ID, id: hoveredFeatureID }, { hover: true });
      }

      const html = feature.properties.kind === "route"
        ? routeTooltip(feature.properties)
        : blockGroupTooltip(feature.properties);
      map.getCanvas().style.cursor = "pointer";
      popup.setLngLat(event.lngLat).setHTML(html).addTo(map);
    }

    map.on("mousemove", showFeature);
    map.on("click", showFeature);
    map.getCanvas().addEventListener("mouseleave", clearHover);
    mapStatus.classList.add("hidden");
  });
}

if (!mapElement || typeof mapboxgl === "undefined" || typeof MAPBOX_TOKEN === "undefined") {
  if (mapStatus) {
    mapStatus.textContent = "The interactive map could not be loaded.";
    mapStatus.classList.add("error");
  }
} else {
  fetch("data/map-data.geojson")
    .then((response) => {
      if (!response.ok) throw new Error(`map data request failed: ${response.status}`);
      return response.json();
    })
    .then(initializeMap)
    .catch(() => {
      mapStatus.textContent = "The interactive map data could not be loaded.";
      mapStatus.classList.add("error");
    });
}
