# ignfetch

Download geospatial datasets from IGN in one command.

## Installation

```bash
uv sync
```

## Usage

Available commands:

```
ignfetch bdhaie OUTPUT
  Download BD Haie dataset (hedge vector data).

ignfetch bdortho OUTPUT [--year YEAR] [--department DEPT] [--irc] [--no-decompress]
  Download BD-Ortho orthophoto data.

ignfetch lidar-hd GEOJSON-FILE OUTPUT [--compress]
  Download LiDAR HD tiles intersecting AOI polygon.

ignfetch departement OUTPUT
  Download département boundaries (shapefile or GeoJSON).

ignfetch foretv1 OUTPUT
  Download Foret V1 forest cover dataset.
```

All outputs are saved as `.parquet` files unless otherwise specified.
