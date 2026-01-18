import * as Cesium from "cesium";
import Sandcastle from "Sandcastle";


Cesium.Ion.defaultAccessToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiIxN2U4ODQ4MC02YmE3LTQzZDMtYmZlMS0xNGE1NDVjYjVjY2QiLCJpZCI6MzU5MDI4LCJpYXQiOjE3NjI4MDE3MDB9.5HZG2c0iyHn_REC3zQ6cPJHqcfShc1_GX4lSeAE7s2U';

// ---- Cesium Viewer ----
const viewer = new Cesium.Viewer('cesiumContainer', {
    terrainProvider: new Cesium.CesiumTerrainProvider({
        url: 'http://localhost:8888/', // Tirol DEM terrain
        requestVertexNormals: true
    }),
    baseLayerPicker: false,
    timeline: false,
    animation: false,
    geocoder: false
});

// ---- Global state ----
let orthoLayer = null;

// ---- Vertical exaggeration ----
const exaggerationSlider = document.getElementById('exaggeration');
const exValue = document.getElementById('exValue');

viewer.scene.verticalExaggeration = 1.5;

exaggerationSlider.addEventListener('input', e => {
    const v = parseFloat(e.target.value);
    viewer.scene.verticalExaggeration = v;
    exValue.textContent = `${v}x`;
});

// ---- Load ortho imagery ----
function loadOrtho() {
    const gs = document.getElementById('geoserverUrl').value;
    const ws = document.getElementById('workspace').value;
    const layer = document.getElementById('orthoLayer').value;

    if (orthoLayer) {
        viewer.imageryLayers.remove(orthoLayer);
    }

    orthoLayer = viewer.imageryLayers.addImageryProvider(
        new Cesium.WebMapServiceImageryProvider({
            url: `${gs}/wms`,
            layers: `${ws}:${layer}`,
            parameters: {
                service: 'WMS',
                version: '1.1.1',
                request: 'GetMap',
                styles: '',
                format: 'image/png',
                transparent: true,
                srs: 'EPSG:3857'
            },
            credit: 'TIRIS Tirol'
        })
    );
}

// ---- Fly to Tirol ----
function flyToTirol() {
    viewer.camera.flyTo({
        destination: Cesium.Cartesian3.fromDegrees(
            11.39,
            47.26,
            12000
        ),
        orientation: {
            pitch: Cesium.Math.toRadians(-45)
        },
        duration: 3
    });
}

// ---- Initial view ----
flyToTirol();
