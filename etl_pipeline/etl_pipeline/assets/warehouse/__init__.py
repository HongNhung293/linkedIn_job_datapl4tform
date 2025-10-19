from dagster import load_assets_from_modules

from . import warehouse_assets


warehouse_assets = load_assets_from_modules ([
    warehouse_assets
])