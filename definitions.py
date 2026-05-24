from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
import proyecto
# from proyecto import practica_job, sensor_carpeta_codigo


defs = Definitions(
    assets=load_assets_from_modules([proyecto]),
    # ¡AQUÍ ESTÁ LA CLAVE! Debes añadir el check aquí:
     asset_checks=load_asset_checks_from_modules([proyecto]),
    # jobs=[practica_job],
    # sensors=[sensor_carpeta_codigo],
)