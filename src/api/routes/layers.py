from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pathlib import Path
import geopandas as gpd
import rasterio
from rasterio.features import shapes
from rasterio.warp import transform_bounds
from shapely.geometry import shape, mapping, box
import json
import requests
import sys
sys.path.append(str(Path(__file__).parent.parent))

router = APIRouter(prefix="/layers", tags=["layers"])

DATA_DIR = Path("data")
RELEASE_AREAS_SDAT = DATA_DIR / "processed" / "release_areas" / "release_areas.sdat"
SIMULATION_SDAT = DATA_DIR / "processed" / "simulations" / "sim_003" / "process_area.sdat"

AOI_GEOJSON = {
    "type": "FeatureCollection",
    "name": "aoi",
    "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::31255"}},
    "features": [{
        "type": "Feature",
        "properties": {"id": 1},
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [[[
                [-48425.116036475767032, 215307.525832502753474],
                [-48414.887467524895328, 215310.843206216581166],
                [-48403.069323669493315, 215313.331236501922831],
                [-48389.661604909553716, 215314.989923358807573],
                [-48374.664311245091085, 215315.819266787264496],
                [-48425.116036475767032, 215307.525832502753474]
            ]]]
        }
    }]
}


@router.get("/aoi")
async def get_aoi():
    """Get AOI boundary in WGS84"""
    try:
        gdf = gpd.GeoDataFrame.from_features(AOI_GEOJSON, crs="EPSG:31255")
        gdf_wgs84 = gdf.to_crs(epsg=4326)
        return json.loads(gdf_wgs84.to_json())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/release-areas")
async def get_release_areas():
    """
    Serve release areas from SAGA .sdat file as GeoJSON
    CRS: EPSG:31255 → WGS84
    """
    try:
        if not RELEASE_AREAS_SDAT.exists():
            return {"type": "FeatureCollection", "features": []}
        
        with rasterio.open(RELEASE_AREAS_SDAT) as src:
            data = src.read(1)
            transform = src.transform
            
            mask = data == 1
            
            geoms = []
            for geom, val in shapes(data, mask=mask, transform=transform):
                geoms.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {"type": "release_area"}
                })
            
            geojson = {"type": "FeatureCollection", "features": geoms}
            
            gdf = gpd.GeoDataFrame.from_features(geojson, crs="EPSG:31255")
            gdf_wgs84 = gdf.to_crs(epsg=4326)
            
            return json.loads(gdf_wgs84.to_json())
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Release areas error: {str(e)}")


@router.get("/simulation")
async def get_simulation_results():
    """
    Serve simulation process area from SAGA .sdat as GeoJSON
    CRS: EPSG:31255 → WGS84
    """
    try:
        if not SIMULATION_SDAT.exists():
            return {"type": "FeatureCollection", "features": []}
        
        with rasterio.open(SIMULATION_SDAT) as src:
            data = src.read(1)
            transform = src.transform
            
            mask = data > 0
            
            geoms = []
            for geom, val in shapes(data, mask=mask, transform=transform):
                geoms.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {"process_area_value": float(val)}
                })
            
            geojson = {"type": "FeatureCollection", "features": geoms}
            
            gdf = gpd.GeoDataFrame.from_features(geojson, crs="EPSG:31255")
            gdf_wgs84 = gdf.to_crs(epsg=4326)
            
            return json.loads(gdf_wgs84.to_json())
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Simulation error: {str(e)}")


@router.get("/osm")
async def get_osm_infrastructure():
    """
    Fetch OSM infrastructure (buildings, roads, bike paths) via Overpass API
    """
    try:
        gdf_aoi = gpd.GeoDataFrame.from_features(AOI_GEOJSON, crs="EPSG:31255")
        gdf_aoi_wgs84 = gdf_aoi.to_crs(epsg=4326)
        bounds = gdf_aoi_wgs84.total_bounds
        
        south, west, north, east = bounds[1], bounds[0], bounds[3], bounds[2]
        
        overpass_url = "http://overpass-api.de/api/interpreter"
        
        query = f"""
        [out:json][timeout:25];
        (
          way["building"]({south},{west},{north},{east});
          way["highway"]({south},{west},{north},{east});
          way["highway"="cycleway"]({south},{west},{north},{east});
          way["highway"="path"]({south},{west},{north},{east});
          way["highway"="footway"]({south},{west},{north},{east});
        );
        out geom;
        """
        
        response = requests.post(overpass_url, data={'data': query}, timeout=30)
        osm_data = response.json()
        
        features = []
        for element in osm_data.get('elements', []):
            if element['type'] == 'way' and 'geometry' in element:
                coords = [[node['lon'], node['lat']] for node in element['geometry']]
                
                tags = element.get('tags', {})
                feature_type = tags.get('highway') or tags.get('building', 'unknown')
                
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "LineString" if 'highway' in tags else "Polygon",
                        "coordinates": coords
                    },
                    "properties": {
                        "osm_id": element.get('id'),
                        "type": feature_type,
                        "name": tags.get('name', '')
                    }
                }
                features.append(feature)
        
        return {"type": "FeatureCollection", "features": features}
        
    except Exception as e:
        return {"type": "FeatureCollection", "features": []}


@router.get("/bounds")
async def get_aoi_bounds():
    """
    Get AOI bounds in WGS84 for camera positioning
    """
    try:
        gdf = gpd.GeoDataFrame.from_features(AOI_GEOJSON, crs="EPSG:31255")
        gdf_wgs84 = gdf.to_crs(epsg=4326)
        bounds = gdf_wgs84.total_bounds
        
        return {
            "west": float(bounds[0]),
            "south": float(bounds[1]),
            "east": float(bounds[2]),
            "north": float(bounds[3])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))