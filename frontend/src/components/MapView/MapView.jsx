import React, { useEffect, useRef } from 'react';

const MapView = ({ riskLevel }) => {
  const cesiumContainerRef = useRef(null);
  const viewerRef = useRef(null);

  useEffect(() => {
    if (!cesiumContainerRef.current || viewerRef.current) return;

    const initCesium = async () => {
      window.Cesium.Ion.defaultAccessToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiIxN2U4ODQ4MC02YmE3LTQzZDMtYmZlMS0xNGE1NDVjYjVjY2QiLCJpZCI6MzU5MDI4LCJpYXQiOjE3NjI4MDE3MDB9.5HZG2c0iyHn_REC3zQ6cPJHqcfShc1_GX4lSeAE7s2U';

      const viewer = new window.Cesium.Viewer(cesiumContainerRef.current, {
        terrain: await window.Cesium.Terrain.fromWorldTerrain(),
        baseLayerPicker: false,
        animation: false,
        timeline: false
      });

      viewerRef.current = viewer;

      const ortho = viewer.imageryLayers.addImageryProvider(
        new window.Cesium.WebMapServiceImageryProvider({
          url: 'http://localhost:8888/geoserver/wms',
          layers: 'tirol:ortho_image',
          parameters: {
            service: 'WMS',
            version: '1.1.1',
            request: 'GetMap',
            styles: '',
            format: 'image/png',
            transparent: true,
            srs: 'EPSG:3857'
          },
          credit: 'TIRIS Ortho'
        })
      );

      const demHillshade = viewer.imageryLayers.addImageryProvider(
        new window.Cesium.WebMapServiceImageryProvider({
          url: 'http://localhost:8888/geoserver/wms',
          layers: 'tirol:dem',
          parameters: {
            service: 'WMS',
            version: '1.1.1',
            request: 'GetMap',
            styles: 'hillshade',
            format: 'image/png',
            transparent: true,
            srs: 'EPSG:3857'
          },
          credit: 'TIRIS DEM'
        })
      );

      viewer.imageryLayers.raiseToTop(demHillshade);
      demHillshade.alpha = 0.4;

      const aoiRectangle = window.Cesium.Rectangle.fromDegrees(
        11.31, 47.21, 11.45, 47.32
      );

      viewer.camera.flyTo({
        destination: aoiRectangle,
        duration: 2
      });
    };

    initCesium();

    return () => {
      if (viewerRef.current) {
        viewerRef.current.destroy();
        viewerRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    if (viewerRef.current && riskLevel) {
      const color = getRiskColor(riskLevel);
      // Future: overlay risk zones on map
    }
  }, [riskLevel]);

  const getRiskColor = (level) => {
    const colors = {
      'LOW': '#4CAF50',
      'MODERATE': '#FFC107',
      'HIGH': '#FF9800',
      'CRITICAL': '#F44336'
    };
    return colors[level] || '#CCCCCC';
  };

  return <div ref={cesiumContainerRef} style={{ width: '100%', height: '100%' }} />;
};

export default MapView;