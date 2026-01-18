<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>3D Tirol Map</title>
    <script src="https://cesium.com/downloads/cesiumjs/releases/1.95/Build/Cesium/Cesium.js"></script>
    <link href="https://cesium.com/downloads/cesiumjs/releases/1.95/Build/Cesium/Widgets/widgets.css" rel="stylesheet">
    <style>
        html, body, #cesiumContainer {
            width: 100%;
            height: 100%;
            margin: 0;
            padding: 0;
            overflow: hidden;
        }
        #controls {
            position: absolute;
            top: 10px;
            left: 10px;
            background: rgba(42, 42, 42, 0.9);
            padding: 15px;
            border-radius: 5px;
            color: white;
            font-family: Arial, sans-serif;
            z-index: 1000;
            max-width: 300px;
        }
        #controls h3 {
            margin: 0 0 10px 0;
            font-size: 16px;
        }
        #controls label {
            display: block;
            margin: 8px 0 4px 0;
            font-size: 12px;
        }
        #controls input, #controls select, #controls button {
            width: 100%;
            padding: 6px;
            margin-bottom: 8px;
            border: 1px solid #666;
            border-radius: 3px;
            background: #333;
            color: white;
            font-size: 12px;
        }
        #controls button {
            background: #0078d4;
            cursor: pointer;
            border: none;
            padding: 8px;
        }
        #controls button:hover {
            background: #106ebe;
        }
        .info {
            font-size: 11px;
            color: #aaa;
            margin-top: 10px;
        }
    </style>
</head>
<body>
    <div id="cesiumContainer"></div>
    <div id="controls">
        <h3>🗺️ 3D Map Controls</h3>
        
        <label>GeoServer URL:</label>
        <input type="text" id="geoserverUrl" value="http://localhost:8888/geoserver" placeholder="http://localhost:8080/geoserver">
        
        <label>Workspace:</label>
        <input type="text" id="workspace" value="tirol" placeholder="tirol">
        
        <label>Ortho Layer:</label>
        <input type="text" id="orthoLayer" value="ortho_image" placeholder="ortho_image">
        
        <label>Terrain Type:</label>
        <select id="terrainType">
            <option value="world">Cesium World Terrain (Default)</option>
            <option value="ellipsoid">Flat (Ellipsoid)</option>
            <option value="custom">Custom Terrain (Advanced)</option>
        </select>
        
        <label>Exaggeration:</label>
        <input type="range" id="exaggeration" min="1" max="3" step="0.1" value="1.5">
        <span id="exaggerationValue">1.5x</span>
        
        <button onclick="loadMap()">📍 Load Map</button>
        <button onclick="flyToAOI()">🎯 Fly to AOI</button>
        <button onclick="toggleOrtho()">🖼️ Toggle Ortho</button>
        
        <div class="info">
            <strong>Coordinates (EPSG:31255):</strong><br>
            Austria GK Central<br>
            <strong>Navigation:</strong><br>
            Left drag: Pan<br>
            Right drag: Zoom<br>
            Middle drag: Rotate
        </div>
    </div>

    <script>
        // Initialize Cesium with ion access token
        Cesium.Ion.defaultAccessToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJlYWE1OWUxNy1mMWZiLTQzYjYtYTQ0OS1kMWFjYmFkNjc5YzciLCJpZCI6NTc3MzMsImlhdCI6MTYyNzg0NTE4Mn0.XcKpgANiY19MC4bdFUXMVEBToBmqS8kuYpUlxJHYZxk';

        // Create viewer
        const viewer = new Cesium.Viewer('cesiumContainer', {
            terrainProvider: Cesium.createWorldTerrain({
                requestWaterMask: true,
                requestVertexNormals: true
            }),
            baseLayerPicker: false,
            geocoder: false,
            homeButton: false,
            sceneModePicker: true,
            navigationHelpButton: false,
            animation: false,
            timeline: false,
            fullscreenButton: true
        });

        // Global variables
        let orthoImageryLayer = null;
        let currentExaggeration = 1.5;

        // Set initial exaggeration
        viewer.scene.verticalExaggeration = currentExaggeration;

        // Tirol AOI approximate center (lon, lat in WGS84)
        const aoiCenter = {
            longitude: 11.39,
            latitude: 47.26,
            height: 5000
        };

        // Update exaggeration display
        document.getElementById('exaggeration').addEventListener('input', function(e) {
            currentExaggeration = parseFloat(e.target.value);
            document.getElementById('exaggerationValue').textContent = currentExaggeration + 'x';
            viewer.scene.verticalExaggeration = currentExaggeration;
        });

        // Load map with GeoServer layers
        function loadMap() {
            const geoserverUrl = document.getElementById('geoserverUrl').value;
            const workspace = document.getElementById('workspace').value;
            const orthoLayer = document.getElementById('orthoLayer').value;
            const terrainType = document.getElementById('terrainType').value;

            // Update terrain
            if (terrainType === 'ellipsoid') {
                viewer.terrainProvider = new Cesium.EllipsoidTerrainProvider();
                console.log('Terrain set to flat ellipsoid');
            } else if (terrainType === 'world') {
                viewer.terrainProvider = Cesium.createWorldTerrain({
                    requestWaterMask: true,
                    requestVertexNormals: true
                });
                console.log('Terrain set to Cesium World Terrain');
            }

            // Remove existing ortho layer if present
            if (orthoImageryLayer) {
                viewer.imageryLayers.remove(orthoImageryLayer, false);
            }

            // Add ortho imagery from GeoServer WMS
            try {
                const wmsUrl = `${geoserverUrl}/wms`;
                orthoImageryLayer = viewer.imageryLayers.addImageryProvider(
                    new Cesium.WebMapServiceImageryProvider({
                        url: wmsUrl,
                        layers: `${workspace}:${orthoLayer}`,
                        parameters: {
                            format: 'image/png',
                            transparent: true,
                            srs: 'EPSG:31255'
                        },
                        credit: new Cesium.Credit('TIRIS - Tirol')
                    })
                );
                console.log('Ortho layer loaded successfully');
                alert('Map loaded! Use mouse to navigate.');
            } catch (error) {
                console.error('Error loading ortho layer:', error);
                alert('Error loading ortho layer. Check console for details.');
            }
        }

        // Fly to AOI
        function flyToAOI() {
            viewer.camera.flyTo({
                destination: Cesium.Cartesian3.fromDegrees(
                    aoiCenter.longitude,
                    aoiCenter.latitude,
                    aoiCenter.height
                ),
                orientation: {
                    heading: Cesium.Math.toRadians(0),
                    pitch: Cesium.Math.toRadians(-45),
                    roll: 0.0
                },
                duration: 3
            });
        }

        // Toggle ortho layer visibility
        function toggleOrtho() {
            if (orthoImageryLayer) {
                orthoImageryLayer.show = !orthoImageryLayer.show;
                console.log('Ortho visibility:', orthoImageryLayer.show);
            } else {
                alert('Please load the map first!');
            }
        }

        // Initial fly to AOI
        flyToAOI();

        // Camera position display (optional)
        viewer.camera.changed.addEventListener(function() {
            const position = viewer.camera.positionCartographic;
            const lon = Cesium.Math.toDegrees(position.longitude);
            const lat = Cesium.Math.toDegrees(position.latitude);
            const height = position.height;
            
            // You can display this info if needed
            // console.log(`Lon: ${lon.toFixed(4)}, Lat: ${lat.toFixed(4)}, Height: ${height.toFixed(0)}m`);
        });

        console.log('3D Map initialized. Configure GeoServer settings and click "Load Map".');
    </script>
</body>
</html>