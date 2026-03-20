from importlib.resources import files

import geopandas as gpd


def dalles_bdortho() -> gpd.GeoDataFrame:
    path = files("ignfetch.assets").joinpath("dalles_bdortho.parquet")
    return gpd.read_parquet(path)


def years_bdortho_rvb() -> gpd.GeoDataFrame:
    path = files("ignfetch.assets").joinpath("year_of_capture_bdortho_rvb.parquet")
    return gpd.read_parquet(path)


def years_bdortho_irc() -> gpd.GeoDataFrame:
    path = files("ignfetch.assets").joinpath("year_of_capture_bdortho_irc.parquet")
    return gpd.read_parquet(path)
