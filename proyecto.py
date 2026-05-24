import re, requests, pandas as pd
import os
# import subprocess
from dagster import asset, asset_check, Output, AssetCheckResult, MetadataValue
from plotnine import *
from plotnine import geom_point, scale_x_continuous, scale_fill_cmap, theme_void, element_rect
from plotnine import ggplot, aes, geom_map, theme, element_text, labs, coord_equal, scale_y_continuous
from plotnine import facet_wrap, geom_boxplot, scale_fill_brewer, theme_minimal, element_blank    
import geopandas as gpd


# CARGA Y PREPROCESADO
# =========================================================================
# RAMA 1: DISTRIBUCIÓN DE RENTA POR FUENTE DE INGRESOS
# =========================================================================
@asset
def distribucion_renta_raw():
    df = pd.read_csv('data/distribucion-renta-ingresos.csv')
    return df

@asset
def distribucion_renta_clean(distribucion_renta_raw):
    """
    Transforma el dato crudo, limpiando formatos, filtrar por ámbito provincial (solo 38) 
    y remueve/imputa nulos, manteniendo el formato largo original.
    """
    df = distribucion_renta_raw.copy()

    # Renombrar la columna MEDIDAS#es a MEDIDAS para limpiar caracteres especiales
    df = df.rename(columns={'MEDIDAS#es': 'MEDIDAS'})
    
    # Filtrar para quedarse ÚNICAMENTE con la provincia 38 (S/C de Tenerife)
    # Excluir la provincia 35 extrayendo los caracteres correspondientes del geocode
    df = df[df['TERRITORIO_CODE'].str.split('_').str[1].str[:2] == '38']
    
    # Corregir el desfase del prefijo temporal en TERRITORIO_CODE
    # Cambiamos, por ejemplo, "20220101_" por "20210101_" si el año del registro es 2021.
    def sincronizar_prefijo(row):
        ano_real = str(row['año'])
        # Reemplaza los primeros 4 caracteres del código por el año real de la fila
        return ano_real + row['TERRITORIO_CODE'][4:]
        
    df['TERRITORIO_CODE'] = df.apply(sincronizar_prefijo, axis=1)

    df = df.rename(columns={'TERRITORIO_CODE': 'geocode'})

    # Tratamiento de strings con comas y conversión segura a flotante
    df['OBS_VALUE'] = df['OBS_VALUE'].astype(str).str.replace(',', '.')
    
    # 'coerce' convierte textos corruptos en NaN y se transforman en 0
    df['OBS_VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce').fillna(0)

    # Eliminación de columnas redundantes
    df = df.drop(columns=['distrito', 'seccion'], errors='ignore')

    df['año'] = df['año'].astype(str)
    
    df.to_csv('data/distribucion-renta-ingresos-clean.csv', index=False)

    return df


@asset_check(asset=distribucion_renta_clean)
def check_cero_nulos_destino(distribucion_renta_clean):
    """Garantiza la ausencia absoluta de nulos tras la transformación"""
    df = distribucion_renta_clean
    nulos_totales = int(df.isna().sum().sum())
    
    passed = (nulos_totales == 0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "nulos_totales_post_limpieza": MetadataValue.int(nulos_totales),
            "principio_gestalt": "Continuidad Visual",
            "impacto": "Obligatorio. Evita que falten datos o registros al renderizar con Plotnine."
        }
    )

@asset_check(asset=distribucion_renta_clean)
def check_rangos_y_coherencia_ingresos(distribucion_renta_clean):
    """
    Verifica que las métricas de ingresos estén en rangos (0-100%) y valida
    que la columna renombrada 'MEDIDAS' no contenga nulos o textos inesperados.
    """
    df = distribucion_renta_clean
    
    # Aplicación del filtro solicitado para aislar las fuentes de ingresos
    fuentes_ingreso = ['SUELDOS_SALARIOS', 'PRESTACIONES_DESEMPLEO', 'PENSIONES', 'OTRAS_PRESTACIONES']
    df_ingresos = df[df['MEDIDAS_CODE'].isin(fuentes_ingreso)]
    
    # Validar rangos numéricos sobre el subconjunto filtrado
    anomalos_numericos = len(df_ingresos[(df_ingresos['OBS_VALUE'] < 0) | (df_ingresos['OBS_VALUE'] > 100)])
    
    # Validar la columna 'MEDIDAS' recién renombrada sobre este mismo subconjunto
    # Comprobamos si se ha quedado algún campo de texto vacío tras el renombrado y filtrado
    anomalos_texto = int(df_ingresos['MEDIDAS'].isna().sum())
            
    passed = (anomalos_numericos == 0) and (anomalos_texto == 0)
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "casos_num_fuera_rango": MetadataValue.int(anomalos_numericos),
            "nulos_en_columna_medidas": MetadataValue.int(anomalos_texto),
            "principio_gestalt": "Veracidad Visual / Similitud",
            "impacto": "Crítico. Asegura que las etiquetas textuales existan para construir leyendas limpias en Plotnine."
        }
    )

@asset_check(asset=distribucion_renta_clean)
def check_cardinalidad_provincia(distribucion_renta_clean):
    """Valida que los registros procesados correspondan a la provincia de estudio (S/C de Tenerife -> 38)"""
    df = distribucion_renta_clean
    
    # Extraemos el código de provincia desde el identificador geográfico (ej: '20220101_38001...')
    provincias_detectadas = df['geocode'].apply(lambda x: x.split('_')[1][:2]).unique()
    
    passed = list(provincias_detectadas) == ['38']
    return AssetCheckResult(
        passed=passed,
        metadata={
            "provincias_en_dataset": MetadataValue.text(str(list(provincias_detectadas))),
            "principio_gestalt": "Carga Cognitiva",
            "impacto": "Evita sobrecargar las visualizaciones incluyendo secciones censales de otras provincias."
        }
    )



# =========================================================================
# RAMA 2: RENTA MEDIA Y MEDIANA
# =========================================================================

@asset
def rentamedia_raw():
    df = pd.read_csv('data/rentamedia-sc-3.csv')
    return df

@asset
def rentamedia_clean(rentamedia_raw):
    """
    Transforma el dataset de renta, limpiando formatos, filtra la provincia 38, 
    sincroniza claves temporales y guarda en CSV.
    """
    df = rentamedia_raw.copy()
    
    # Renombrar la columna MEDIDAS#es a MEDIDAS para normalizar el esquema
    df = df.rename(columns={'MEDIDAS#es': 'MEDIDAS'})
    
    # Filtrar para quedarse ÚNICAMENTE con la provincia 38 (S/C de Tenerife)
    df = df[df['TERRITORIO_CODE'].str.split('_').str[1].str[:2] == '38']
    
    # Corregir el desfase del prefijo temporal en TERRITORIO_CODE
    def sincronizar_prefijo(row):
        ano_real = str(row['año'])
        return ano_real + row['TERRITORIO_CODE'][4:]
        
    df['TERRITORIO_CODE'] = df.apply(sincronizar_prefijo, axis=1)

    df = df.rename(columns={'TERRITORIO_CODE': 'geocode'})
    
    # CONTROLLER LOGS - Ver cuántos nulos reales tenemos en OBS_VALUE antes de imputar
    nulos_antes = df['OBS_VALUE'].isnull().sum()
    print(f"Dagster LOG - Valores nulos detectados en OBS_VALUE antes de imputar: {nulos_antes}")

    # ESTRATEGIA GESTALT: Imputación por vecindad territorial (Municipio + Año)
    # Rellenamos el NaN de la calle con la mediana de las demás calles de su propio municipio ese año
    df['OBS_VALUE'] = df.groupby(['año', 'municipio'])['OBS_VALUE'].transform(
        lambda x: x.fillna(x.median())
    )

    # Eliminar registros que sigan siendo nulos
    # Esto ocurre si un municipio entero no tiene ningún dato en todo el año (evita inventar datos)
    df = df.dropna(subset=['OBS_VALUE'])
    
    nulos_despues = df['OBS_VALUE'].isnull().sum()
    print(f"Dagster LOG - Valores nulos residuales en OBS_VALUE tras la limpieza: {nulos_despues}")

    # Remover desgloses redundantes solicitados
    df = df.drop(columns=['distrito', 'seccion'], errors='ignore')

    df['año'] = df['año'].astype(str)

    # Exportar el CSV limpio para poder analizarlo de inmediato en tu Jupyter Notebook
    df.to_csv('data/rentamedia-sc-3-clean.csv', index=False)
    
    return df


@asset_check(asset=rentamedia_clean)
def check_cero_nulos_rentamedia(rentamedia_clean):
    """Garantiza la ausencia absoluta de nulos tras la transformación de renta"""
    df = rentamedia_clean
    nulos_totales = int(df.isna().sum().sum())
    
    passed = (nulos_totales == 0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "nulos_totales_post_limpieza": MetadataValue.int(nulos_totales),
            "principio_gestalt": "Continuidad Visual",
            "impacto": "Evita fallos y pérdidas de registros al cruzar la información en el Data Mart."
        }
    )

@asset_check(asset=rentamedia_clean)
def check_valores_renta_positivos(rentamedia_clean):
    """Verifica que no existan valores aberrantes o negativos en los ingresos económicos"""
    df = rentamedia_clean
    
    # Contamos si existen registros de renta menores o iguales a cero
    anomalos = len(df[df['OBS_VALUE'] <= 0])
            
    passed = (anomalos == 0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "casos_con_renta_invalida": MetadataValue.int(anomalos),
            "principio_gestalt": "Veracidad Visual",
            "impacto": "Crítico. Los valores negativos en renta desvirtúan los cálculos de asimetría económica."
        }
    )

@asset_check(asset=rentamedia_clean)
def check_cardinalidad_provincia_rentamedia(rentamedia_clean):
    """Valida que los registros de renta correspondan en su totalidad a la provincia 38"""
    df = rentamedia_clean
    provincias_detectadas = df['geocode'].apply(lambda x: x.split('_')[1][:2]).unique()
    
    passed = list(provincias_detectadas) == ['38']
    return AssetCheckResult(
        passed=passed,
        metadata={
            "provincias_en_dataset": MetadataValue.text(str(list(provincias_detectadas))),
            "principio_gestalt": "Carga Cognitiva",
            "impacto": "Garantiza la consistencia interna del territorio antes de mapear la información."
        }
    )


# =========================================================================
# RAMA 3: SECTOR DE OCUPACIÓN
# =========================================================================
@asset
def ocupacion_raw():
    df = pd.read_csv('data/ocupacion-sc-3.csv')
    return df

@asset
def ocupacion_clean(ocupacion_raw):
    """
    Transforma el dataset de ocupación adaptándose a sus columnas reales.
    Estandariza el esquema para hacerlo 100% simétrico con los datasets de renta.
    """
    df = ocupacion_raw.copy()

    # Conversión numérica segura (Estandarización por seguridad del pipeline)
    # Nota: 'geocode' ya viene con el prefijo temporal correcto en este CSV, no requiere reajuste.
    df['num_casos'] = pd.to_numeric(df['num_casos'], errors='coerce').fillna(0)
    
    # Colapsar Dimensión de Sexo (Sumar Hombres y Mujeres) ---
    # Agrupamos por el resto de dimensiones identificadoras
    columnas_clave = ['año', 'municipio', 'geocode', 'ocupacion']

    # min_count=1 garantiza que: NaN + NaN = NaN (evita inventar ceros en datos protegidos)
    df = df.groupby(columnas_clave, as_index=False)['num_casos'].sum(min_count=1)

    # Remover desgloses redundantes solicitados
    df = df.drop(columns=['code_distrito', 'code_seccion', 'seccion', 'code_municipio'], errors='ignore')

    # Convertimos num_casos a tipo 'Int64' (con I mayúscula) para permitir enteros con nulos
    df['num_casos'] = df['num_casos'].astype('Int64')

    df['año'] = df['año'].astype(str)

    # Guardar copia limpia simétrica para usar directamente en tus cuadernos de Plotnine
    df.to_csv('data/ocupacion-sc-3-clean.csv', index=False)
    
    return df


@asset_check(asset="ocupacion_clean")
def check_cero_nulos_ocupacion(ocupacion_clean):
    """Garantiza la integridad absoluta del dataset de ocupación antes del Data Mart"""
    df = ocupacion_clean
    nulos_totales = int(df.isna().sum().sum())
    
    passed = (nulos_totales == 0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "nulos_totales_post_limpieza": MetadataValue.int(nulos_totales),
            "principio_gestalt": "Continuidad Visual",
            "impacto": "Obligatorio. Asegura que Plotnine disponga de registros continuos para mapear la ocupación."
        }
    )

@asset_check(asset="ocupacion_clean")
def check_valores_ocupacion_no_negativos(ocupacion_clean):
    """Verifica que los valores de ocupación (conteos o tasas) sean lógicos (>= 0)"""
    df = ocupacion_clean
    
    # Buscamos si existe algún valor negativo absurdo
    anomalos = len(df[df['num_casos'] < 0])
            
    passed = (anomalos == 0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "casos_con_ocupacion_negativa": MetadataValue.int(anomalos),
            "principio_gestalt": "Veracidad Visual",
            "impacto": "Crítico. Evita distorsiones en las escalas de color (escalas de ocupación por sectores)."
        }
    )

@asset_check(asset="ocupacion_clean")
def check_confirmar_provincia_38_ocupacion(ocupacion_clean):
    """Filtro de control para verificar si el dataset venía únicamente con la provincia 38 de origen"""
    df = ocupacion_clean
    provincias_detectadas = df['geocode'].apply(lambda x: x.split('_')[1][:2]).unique()
    
    # Pasará en verde si tenías razón y solo venía la 38 (Tenerife)
    passed = list(provincias_detectadas) == ['38']
    return AssetCheckResult(
        passed=passed,
        metadata={
            "provincias_en_dataset": MetadataValue.text(str(list(provincias_detectadas))),
            "principio_gestalt": "Carga Cognitiva",
            "impacto": "Informativo/Validación. Confirma si el proveedor filtró correctamente el ámbito geográfico."
        }
    )


# =========================================================================
# RAMA 4: RELACIÓN CON LA ACTIVIDAD ECONÓMICA
# =========================================================================
@asset
def actividad_raw():
    df = pd.read_csv('data/actividad-sc-3.csv')
    return df

@asset
def actividad_clean(actividad_raw):
    """
    Transforma el dataset de actividad adaptándose a sus columnas reales.
    Estandariza el esquema para hacerlo 100% simétrico con el resto del proyecto.
    """
    df = actividad_raw.copy()
    
    # Renombrar columnas para unificar el esquema con los otros datasets
    # 'Periodo' pasa a ser 'año'
    df = df.rename(columns={
        'Periodo': 'año',
    })

    # Conversión numérica segura e imputación de ceros legítimos
    # Reemplazamos las celdas vacías (nulos) por 0, ya que significan "0 personas en esa actividad"
    df['num_casos'] = pd.to_numeric(df['num_casos'], errors='coerce').fillna(0)
    df['num_casos'] = df['num_casos'].astype('int64')
    
    # Colapsar Dimensión de Sexo (Sumar Hombres y Mujeres) ---
    # Agrupamos por el resto de dimensiones identificadoras
    columnas_clave = ['año', 'municipio', 'geocode', 'Actividad económica']

    # min_count=1 garantiza que: NaN + NaN = NaN (evita inventar ceros en datos protegidos)
    df = df.groupby(columnas_clave, as_index=False)['num_casos'].sum(min_count=1)

    # Remover desgloses redundantes solicitados
    df = df.drop(columns=['cod_distrito', 'cod_seccion', 'seccion', 'provincia', 'cod_municipio', 'cod_provincia'], errors='ignore')

    df['año'] = df['año'].astype(str)

    # Guardar copia limpia simétrica para su uso inmediato en las gráficas de Plotnine
    df.to_csv('data/actividad-sc-3-clean.csv', index=False)
    
    return df


@asset_check(asset="actividad_clean")
def check_cero_nulos_actividad(actividad_clean):
    """Garantiza que no queden nulos tras la imputación de ceros en num_casos"""
    df = actividad_clean
    nulos_totales = int(df.isna().sum().sum())
    
    passed = (nulos_totales == 0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "nulos_totales_post_limpieza": MetadataValue.int(nulos_totales),
            "principio_gestalt": "Continuidad Visual",
            "impacto": "Evita huecos de información inesperados al renderizar las facetas por sector económico."
        }
    )

@asset_check(asset="actividad_clean")
def check_valores_actividad_no_negativos(actividad_clean):
    """Verifica que el recuento de trabajadores por actividad sea mayor o igual a cero"""
    df = actividad_clean
    anomalos = len(df[df['num_casos'] < 0])
            
    passed = (anomalos == 0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "casos_con_actividad_negativa": MetadataValue.int(anomalos),
            "principio_gestalt": "Veracidad Visual",
            "impacto": "Crítico. Asegura la coherencia matemática de los volúmenes de población activa."
        }
    )

@asset_check(asset="actividad_clean")
def check_confirmar_provincia_38_actividad(actividad_clean):
    """Filtro de control para verificar la delimitación de la provincia (38)"""
    df = actividad_clean
    provincias_detectadas = df['geocode'].apply(lambda x: x.split('_')[1][:2]).unique()
    
    passed = list(provincias_detectadas) == ['38']
    return AssetCheckResult(
        passed=passed,
        metadata={
            "provincias_en_dataset": MetadataValue.text(str(list(provincias_detectadas))),
            "principio_gestalt": "Carga Cognitiva",
            "impacto": "Informativo. Confirma la integridad del alcance geográfico del proyecto."
        }
    )




# =========================================================================
# GENERACIÓN DE GRÁFICOS CON IA
# =========================================================================
def pedir_codigo_a_ia(template_ia):
    """Función de soporte que permite llamar a la IA desde cualquier gráfico con 
    una sola línea sin necesidad de repetir el mismo código de requests y 
    re.search
    """
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {"Authorization": "Bearer sk-1234"}
    try:
        response = requests.post(url, json=template_ia, headers=headers, timeout=60)
        response.raise_for_status()
        codigo_raw = response.json()['choices'][0]['message']['content']
        match = re.search(r"```python\s+(.*?)\s+```", codigo_raw, re.DOTALL)
        return match.group(1) if match else codigo_raw.strip()
    except Exception as e:
        return f"# Error en la petición: {e}"



@asset(
    description="Prepara el DataFrame unificado de Sueldos vs Desempleo por isla para el Gráfico 1."
)
def df_evolucion_islas_2x2(distribucion_renta_clean):
    df = distribucion_renta_clean.copy()
    df['año_num'] = df['año'].astype(int)
    
    # 1. Mapeo geográfico interinsular
    mapeo_islas = {
        'Adeje': 'Tenerife', 'Arona': 'Tenerife', 'Granadilla de Abona': 'Tenerife',
        'Guía de Isora': 'Tenerife', 'San Cristóbal de La Laguna': 'Tenerife',
        'Santa Cruz de Tenerife': 'Tenerife', 'Puerto de la Cruz': 'Tenerife',
        'La Orotava': 'Tenerife', 'Los Realejos': 'Tenerife', 'Candelaria': 'Tenerife',
        'Tacoronte': 'Tenerife', 'Icod de los Vinos': 'Tenerife', 'Vilaflor de Chasna': 'Tenerife',
        'La Victoria de Acentejo': 'Tenerife', 'Santa Úrsula': 'Tenerife',
        'Santa Cruz de La Palma': 'La Palma', 'Los Llanos de Aridane': 'La Palma',
        'Villa de Mazo': 'La Palma', 'El Paso': 'La Palma', 'Breña Alta': 'La Palma',
        'San Sebastián de La Gomera': 'La Gomera', 'Valle Gran Rey': 'La Gomera',
        'Vallehermoso': 'La Gomera', 'El Pinar de El Hierro': 'El Hierro',
        'Valverde': 'El Hierro', 'Frontera': 'El Hierro'
    }
    df['isla'] = df['municipio'].str.strip().map(mapeo_islas)
    df = df.dropna(subset=['isla'])

    # 2. Filtrar y pivotar para tener las métricas en columnas limpias por isla y año
    df_filtrado = df[df['MEDIDAS_CODE'].isin(['SUELDOS_SALARIOS', 'PRESTACIONES_DESEMPLEO'])]
    
    df_islas = df_filtrado.groupby(['año_num', 'isla', 'MEDIDAS_CODE'])['OBS_VALUE'].mean().unstack().reset_index()
    df_islas = df_islas.rename(columns={
        'año_num': 'año',
        'SUELDOS_SALARIOS': 'sueldos',
        'PRESTACIONES_DESEMPLEO': 'desempleo'
    })
    
    # 3. Exportar a la carpeta de datos
    os.makedirs('data', exist_ok=True)
    df_islas.to_csv('data/df_evolucion_islas_2x2.csv', index=False)
    return df_islas


@asset(
    description="Genera el prompt para el Gráfico 1 corrigiendo el parámetro del subtítulo para la API de Plotnine."
)
def prompt_islas_2x2_ia():
    template_tecnico = """
    def generar_plot(df):
        plot = (
            ggplot(df, aes(x='año'))
            + geom_line(aes(y='sueldos'))
            + geom_point(aes(y='sueldos'))
            # ... añadir capas siguiendo la jerarquía visual
        )
        return plot
    """

    system_content = (
        "Eres un experto en Plotnine y Gramática de Gráficos. "
        "Tu objetivo es generar visualizaciones con un diseño profesional y limpio."
    )
    
    descripcion_grafico = """
    REGLAS SINTÁCTICAS CRÍTICAS:
    - Prohibido usar el prefijo 'p9.'.
    - Uso obligatorio de paréntesis global 'plot = ( ... )' con operador '+' al inicio de línea.
    - REGLA DE API: En theme(), NO EXISTE 'subtitle'. Debes usar obligatoriamente 'plot_subtitle' para dar estilo al subtítulo.

    REGLAS DE DISEÑO (TONOS AZULES Y ESPACIO MAXIMIZADO):
    - Base: ggplot(df, aes(x='año'))
    - Capa Sueldos (Azul Oscuro): geom_line(aes(y='sueldos'), color='#1f4e79', size=2.0) + geom_point(aes(y='sueldos'), color='#1f4e79', size=4.0)
    - Capa Desempleo (Azul Claro): geom_line(aes(y='desempleo'), color='#5b9bd5', size=2.0, linetype='dashed') + geom_point(aes(y='desempleo'), color='#5b9bd5', size=4.0)
    - Facetas: facet_wrap('~isla', ncol=2)
    - Escala Eje X: scale_x_continuous(breaks=[2021, 2022, 2023])

    CONTROL DE ETIQUETAS (AZULES):
    Usa labs() con estos textos literales:
    - title='Subida Salarial vs Bajada del Desempleo (2021-2023)'
    - subtitle='Línea Azul Oscuro: Peso de Salarios (%) | Línea Azul Claro Discontinua: Peso del Desempleo (%)'
    - x='Año'
    - y='Porcentaje sobre el Total de Ingresos (%)'

    TEMA Y OPTIMIZACIÓN DE ESPACIO (AGRANDAR TEXTO Y CORREGIR SUBTÍTULO):
    - theme_minimal()
    - theme(
        figure_size=(12, 7),
        panel_spacing=0.1,
        plot_title=element_text(face='bold', size=18, margin={'b': 12}),
        plot_subtitle=element_text(size=13, margin={'b': 10}),          
        strip_text=element_text(face='bold', size=15),
        axis_title=element_text(size=14, face='bold'),
        axis_text=element_text(size=12),
        panel_grid_minor=element_blank()
    )
    """

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": f"Basándote en {template_tecnico}, genera el código con estas reglas estrictas de API:\n{descripcion_grafico}"}
        ],
        "temperature": 0,
        "stream": False
    }


@asset
def vis_islas_2x2_ia(prompt_islas_2x2_ia, df_evolucion_islas_2x2):
    """
    Asset simplificado que invoca a la IA y compila dinámicamente el gráfico de 
    vasos comunicantes usando la inyección nativa del DataFrame.
    """
    codigo = pedir_codigo_a_ia(prompt_islas_2x2_ia)
    
    # Preparación del entorno aislado con las funciones requeridas por el prompt
    entorno = globals().copy()
    entorno.update({
        'df': df_evolucion_islas_2x2,
        'geom_point': geom_point,
        'scale_x_continuous': scale_x_continuous
    })
    
    # Compilación dinámica ultraligera
    exec(codigo, entorno)
    grafico = entorno['generar_plot'](df_evolucion_islas_2x2)
    
    # Almacenamiento final
    os.makedirs('data/outputs', exist_ok=True)
    grafico.save("data/outputs/01_evolucion_islas_2x2.png", dpi=300)
    
    return "OK"



# =========================================================================
# RAMA 6: GRÁFICO 2 CON IA (MAPA DE COROPLETAS CONTINUO)
# =========================================================================

@asset(
    description="Prepara el GeoDataFrame uniendo la cartografía de Tenerife 2024 con los datos de renta de 2023 reemplazando los primeros 4 dígitos del geocode."
)
def gdf_renta_tenerife_2023(rentamedia_clean):
    """
    Asset de preparación geográfica: Filtra por año 2023 (int) y métrica (str),
    reemplaza los primeros 4 dígitos de los geocodes (2023 -> 2024) y realiza el cruce.
    """
    # 1. Copias de seguridad y lectura fresca de los polígonos de Tenerife
    df_renta = rentamedia_clean.copy()
    gdf_tenerife_2023 = gpd.read_file('data/secciones_20240101_tenerife.json')
    
    # 2. Filtrado seguro (Buscamos el año 2023 y saneamos el texto de la métrica)
    df_renta['MEDIDAS'] = df_renta['MEDIDAS'].astype(str).str.strip()
    
    # Nos aseguramos de que el año sea procesado correctamente
    df_renta['año'] = df_renta['año'].astype(int)
    
    df_map_2023 = df_renta[
        (df_renta['año'] == 2023) & 
        (df_renta['MEDIDAS'] == 'Renta neta media por persona')
    ].copy()

    # 3. NORMALIZACIÓN Y MUTACIÓN DE LLAVES (Justo después de filtrar, garantizando memoria aislada)
    gdf_tenerife_2023['geocode'] = gdf_tenerife_2023['geocode'].astype(str).str.strip()
    
    # Convertimos los códigos a string y mutamos quirúrgicamente: 20230101_... -> 20240101_...
    df_map_2023['geocode'] = df_map_2023['geocode'].astype(str).str.strip()
    df_map_2023['geocode'] = '2024' + df_map_2023['geocode'].str[4:]

    # Logs forenses para verificar en tiempo real en la UI de Dagster
    print("=== CONTROL DE LLAVES (SUTITUCIÓN DE 4 DÍGITOS) ===")
    print(f"Muestra geocode en GeoJSON (2024): {gdf_tenerife_2023['geocode'].head(1).tolist()}")
    print(f"Muestra geocode modificado en Renta (2023 -> 2024): {df_map_2023['geocode'].head(1).tolist()}")
    print(f"Dagster LOG - Secciones censales con renta encontradas: {len(df_map_2023)}")

    # 4. Cruce espacial (How='left' para retener los 681 polígonos de la isla)
    gdf_final_map = pd.merge(gdf_tenerife_2023, df_map_2023, on='geocode', how='left')

    # 5. Guardar archivo de respaldo en disco
    os.makedirs('data', exist_ok=True)
    gdf_final_map.to_file('data/gdf_mapa_coropletas_2023.json', driver='GeoJSON')

    return Output(
        value=gdf_final_map,
        metadata={
            "filas_totales_geo": MetadataValue.int(len(gdf_final_map)),
            "filas_renta_encontradas": MetadataValue.int(len(df_map_2023)),
            "impacto": "Modificación de los primeros 4 dígitos exitosa. Alineación temporal completada."
        }
    )


@asset_check(asset=gdf_renta_tenerife_2023)
def check_integridad_cruce_cartografico(gdf_renta_tenerife_2023):
    """
    Verifica que el cruce de renta y cartografía no haya generado un exceso de nulos.
    Pasa si al menos el 90% de las secciones cartográficas tienen dato de renta.
    """
    # Transformamos a DataFrame plano para aislar la lógica analítica de GeoPandas
    df = pd.DataFrame(gdf_renta_tenerife_2023.copy())
    
    # 1. Cálculo de nulos seguro
    nulos_renta_raw = df['OBS_VALUE'].isna().sum()
    total_secciones_raw = len(df)
    
    # 2. Casteo estricto a enteros nativos de Python para Dagster
    nulos_renta = int(nulos_renta_raw)
    total_secciones = int(total_secciones_raw)
    
    # 3. Cálculo de la tasa de cobertura
    if total_secciones == 0:
        tasa_cobertura = 0.0
    else:
        tasa_cobertura = (total_secciones - nulos_renta) / total_secciones
        
    passed = tasa_cobertura >= 0.90
    
    return AssetCheckResult(
        passed=bool(passed),
        metadata={
            "cobertura_renta_geo": MetadataValue.float(float(tasa_cobertura * 100)),
            "secciones_sin_dato": MetadataValue.int(int(nulos_renta)),  
            "principio_gestalt": "Similitud / Carga Cognitiva",
            "impacto": "Crítico. Evita que la IA reciba DataFrames vacíos o corruptos que rompan el exec()."
        }
    )


@asset(
    description="Genera el prompt estructurado para el Mapa Provincial (2023) alineando perfectamente los títulos a la izquierda sin aire residual."
)
def prompt_mapa_coropletas_ia():
    template_tecnico = """
def generar_plot(df):
    plot = (
        ggplot(df)
        + geom_map(aes(fill='OBS_VALUE'), color='none')
        + scale_fill_cmap(cmap_name='viridis', labels=lambda l: [f"{int(float(x)/1000)}k€" for x in l])
        + scale_x_continuous(limits=(-18.2, -16.0)) # Encuadre perfecto de la provincia
        + scale_y_continuous(limits=(27.5, 29.0))   # Protege La Palma en el norte
        + coord_equal()
    )
    return plot
"""

    system_content = (
        "Eres un experto en Plotnine y Gramática de Gráficos. "
        "Tu objetivo es generar visualizaciones con un diseño profesional y limpio."
    )
    
    descripcion_grafico = """
    REGLAS SINTÁCTICAS CRÍTICAS:
    - Prohibido usar el prefijo 'p9.'.
    - Uso obligatorio de paréntesis global 'plot = ( ... )' con operador '+' al inicio de línea.
    - REGLA DE API MAPAS: Para un GeoDataFrame, usa ggplot(df) + geom_map(aes(fill='OBS_VALUE'), color='none').
    - REGLA DE CORTE: Incluir scale_x_continuous(limits=(-18.2, -16.0)) y scale_y_continuous(limits=(27.5, 29.0)) antes de coord_equal().

    CONTROL DE ETIQUETAS (LITERALES LIMPIOS):
    Usa labs() con estos textos exactos (sin espacios artificiales al inicio):
    - title='Fragmentación Espacial de la Riqueza (2023) en la Provincia SC de Tenerife'
    - subtitle='Distribución de la Renta Neta Media por Persona a Nivel de Sección Censal'
    - fill='Renta Neta'

    TEMA Y ESTÉTICA (ALINEACIÓN IZQUIERDA PERFECTA Y LIENZO AJUSTADO):
    - Usa theme_void() para ocultar rejillas y ejes.
    - Dentro de theme(), configura estas propiedades exactas para calcar el estilo de Tenerife solo:
        figure_size=(9.0, 6.0),                    # <--- FORMATO COMPACTO: Elimina el aire blanco lateral sobrante
        legend_position='right', 
        legend_box_spacing=0.02, 
        legend_key_height=75, 
        legend_key_width=12, 
        plot_title=element_text(face='bold', size=14, margin={'b': 6, 't': 12}, ha='left', x=0.02),      # <--- Anclaje limpio al 2% del borde izquierdo
        plot_subtitle=element_text(size=10, margin={'b': 15}, ha='left', x=0.02),                        # <--- Perfectamente alineado con el título
        legend_text=element_text(size=9),
        legend_title=element_text(size=10, face='bold', margin={'b': 8}),
        plot_background=element_rect(fill='#f9f9f9', color='#f9f9f9')
    )
    """

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": f"Basándote en {template_tecnico}, genera el código con estas reglas estrictas de API para mapas:\n{descripcion_grafico}"}
        ],
        "temperature": 0,
        "stream": False
    }


@asset
def vis_mapa_coropletas_ia(prompt_mapa_coropletas_ia, gdf_renta_tenerife_2023):
    """
    Asset que invoca a la IA para codificar el mapa de coropletas provincial,
    inyectando el entorno con las funciones de escala continuas requeridas.
    """
    
    codigo = pedir_codigo_a_ia(prompt_mapa_coropletas_ia)
    
    lineas_limpias = []
    for linea in codigo.splitlines():
        if '.read_file(' in linea:
            linea = f"# Línea eliminada por sanitizador DataOps de Dagster: {linea.strip()}"
        lineas_limpias.append(linea)
    codigo_sanitizado = "\n".join(lineas_limpias)
    
    def funcion_puente_viridis(*args, **kwargs):
        if 'option' in kwargs:
            kwargs['cmap_name'] = kwargs.pop('option')
        return scale_fill_cmap(*args, **kwargs)

    entorno = globals().copy()
    entorno.update({
        'df': gdf_renta_tenerife_2023, 
        'ggplot': ggplot,
        'aes': aes,
        'geom_map': geom_map,
        'scale_fill_cmap': scale_fill_cmap,
        'scale_fill_viridis': funcion_puente_viridis,
        'scale_x_continuous': scale_x_continuous,
        'scale_y_continuous': scale_y_continuous,
        'theme_void': theme_void,
        'theme': theme,
        'element_text': element_text,
        'element_rect': element_rect,
        'labs': labs,
        'coord_equal': coord_equal
    })
    
    exec(codigo_sanitizado, entorno)
    grafico = entorno['generar_plot'](gdf_renta_tenerife_2023)
    
    os.makedirs('data/outputs', exist_ok=True)
    grafico.save("data/outputs/02_mapa_coropletas_renta_2023.png", dpi=300)
    
    return "OK"



# =========================================================================
# RAMA 2 (AMPLIACIÓN): GRÁFICO 2B - ZOOM DE RENTA EXCLUSIVO EN TENERIFE (2023)
# =========================================================================

@asset(
    description="Aísla el GeoJSON y los datos de renta neta exclusivamente para la isla de Tenerife (2023) usando la columna nativa de isla y códigos INE."
)
def gdf_renta_tenerife_solo_2023(rentamedia_clean):
    """
    Filtra la cartografía usando el identificador oficial de isla de Tenerife (ES709)
    y el DataFrame alfanumérico usando los códigos de municipio de la provincia 38 vinculados a Tenerife.
    """
    df_renta = rentamedia_clean.copy()
    
    # 1. FILTRADO FILIGRANA DEL GEOJSON (CARTOGRAFÍA BASE)
    gdf_provincial = gpd.read_file('data/secciones_20240101_tenerife.json')
    gdf_provincial['geocode'] = gdf_provincial['geocode'].astype(str).str.strip()
    
    # Filtrar de forma nativa por la columna del GeoJSON: ES709 es el código único de la isla de Tenerife
    gdf_tenerife_solo = gdf_provincial[gdf_provincial['gcd_isla'] == 'ES709'].copy()
    
    # Obtener la lista única de códigos de municipio (5 dígitos, ej: '38001') que realmente existen en Tenerife
    gdf_tenerife_solo['cod_municipio'] = gdf_tenerife_solo['geocode'].str.extract(r'_(\d{5})')[0]
    if gdf_tenerife_solo['cod_municipio'].isna().any():
        gdf_tenerife_solo['cod_municipio'] = gdf_tenerife_solo['geocode'].str[4:9]
        
    codigos_municipios_tenerife = gdf_tenerife_solo['cod_municipio'].dropna().unique().tolist()

    # 2. FILTRADO DEL DATASET ALFANUMÉRICO DE RENTA (DF CLEAN)
    df_renta['año'] = df_renta['año'].astype(int)
    df_renta['geocode'] = df_renta['geocode'].astype(str).str.strip()
    
    # Extraer el código de municipio de la columna geocode del CSV limpio
    df_renta['cod_municipio'] = df_renta['geocode'].str.extract(r'_(\d{5})')[0]
    if df_renta['cod_municipio'].isna().any():
        df_renta['cod_municipio'] = df_renta['geocode'].str[4:9]
    
    # Filtrar el DataFrame limpio por Año, Métrica e incluyendo SÓLO los códigos que pertenecen a Tenerife
    df_2023 = df_renta[
        (df_renta['año'] == 2023) & 
        (df_renta['MEDIDAS'] == 'Renta neta media por persona') &
        (df_renta['cod_municipio'].isin(codigos_municipios_tenerife))
    ].copy()
    
    # Aplicar mutación obligatoria del geocode (Regla: Datos 2023 mapean con Estructura Censal 2024)
    df_2023['geocode'] = '2024' + df_2023['geocode'].str[4:]
    
    # 3. CRUCE INNER
    # Al estar ambos lados perfectamente limpios y acotados a Tenerife, el cruce inner
    # garantiza que no se arrastre ningún polígono residual ni registros huérfanos.
    gdf_final = pd.merge(gdf_tenerife_solo, df_2023, on='geocode', how='inner', suffixes=('', '_renta'))
    
    # Limpieza de columnas técnicas antes de exportar el GeoJSON
    columnas_a_borrar = ['cod_municipio', 'cod_municipio_renta', 'municipio', 'año', 'MEDIDAS_CODE', 'MEDIDAS']
    for col in columnas_a_borrar:
        if col in gdf_final.columns:
            gdf_final = gdf_final.drop(columns=[col])
            
    os.makedirs('data', exist_ok=True)
    gdf_final.to_file('data/gdf_renta_tenerife_solo_2023.json', driver='GeoJSON')
    
    return gdf_final


@asset_check(asset=gdf_renta_tenerife_solo_2023)
def check_limites_geograficos_tenerife(gdf_renta_tenerife_solo_2023):
    """
    Asset Check de Calidad: Certifica que el dataset contenga de manera estricta
    el rango de secciones censales que componen en exclusividad la isla de Tenerife.
    """
    df = pd.DataFrame(gdf_renta_tenerife_solo_2023.copy())
    total_secciones = len(df)
    
    # Con el filtro por 'gcd_isla' == 'ES709', el conteo de secciones dará exactamente 549.
    # El check pasará de forma robusta y lógica si está en este umbral.
    passed = 500 < total_secciones < 600
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "secciones_aisladas_tenerife": MetadataValue.int(total_secciones),
            "principio_dataops": "Aislamiento de Entorno de Datos (Data Sandboxing)"
        }
    )



@asset(
    description="Genera el prompt estructurado para el Mapa de Tenerife (2023) compacto, sin espacios en blanco y colorbar estilizado."
)
def prompt_mapa_renta_tenerife_ia():
    template_tecnico = """
def generar_plot(df):
    plot = (
        ggplot(df)
        + geom_map(aes(fill='OBS_VALUE'), color='#ffffff', size=0.05)
        + scale_fill_cmap(cmap_name='viridis', labels=lambda l: [f"{int(float(x)/1000)}k€" for x in l])
        + coord_equal()
    )
    return plot
"""

    system_content = (
        "Eres un experto en Plotnine y Gramática de Gráficos. "
        "Tu objetivo es generar visualizaciones con un diseño profesional y limpio."
    )
    
    descripcion_grafico = """
    REGLAS SINTÁCTICAS CRÍTICAS:
    - Prohibido usar el prefijo 'p9.'.
    - Uso obligatorio de paréntesis global 'plot = ( ... )' con operador '+' al inicio de línea.
    - REGLA DE MAPA PERFECTO: Mantén obligatoriamente + coord_equal() para que la isla conserve su fisonomía real.

    CONTROL DE ETIQUETAS (LITERALES):
    Usa labs() con estos textos:
    - title='Fragmentación Espacial de la Riqueza (2023) en la Isla de Tenerife'
    - subtitle='Distribución de la Renta Neta Media por Persona a Nivel de Sección Censal'
    - fill='Renta Neta'

    TEMA Y ESTÉTICA (ELIMINAR ESPACIO BLANCO Y RE-ESTILIZAR COLORBAR):
    - Usa theme_void() para eliminar rejillas y ejes.
    - Dentro de theme(), configura exactamente estas propiedades. Reducimos el lienzo a (11, 6) para ceñirnos a la silueta panorámica de la isla y estiramos la barra limpiamente:
        figure_size=(11, 6),                     # <--- ¡NUEVO RATIO PANORÁMICO! Ajustado a la silueta de Tenerife para eliminar el aire blanco
        legend_position='right',                 # Leyenda a la derecha
        legend_box_spacing=0.2,                  # Espacio controlado entre la isla y la escala
        legend_key_height=75,                    # <--- ¡MÁXIMA ALTURA VERTICAL! Ahora que coord_equal protege la isla, crecerá de forma espectacular
        legend_key_width=13,                     # <--- ¡ESTILIZADO! Anchura fina y elegante para que no sea un bloque gordo
        plot_title=element_text(face='bold', size=18, margin={'b': 8}),
        plot_subtitle=element_text(size=13, margin={'b': 15}),
        legend_text=element_text(size=10),       # Textos de la escala legibles
        legend_title=element_text(size=11, face='bold', margin={'b': 10}),
        plot_background=element_rect(fill='#f9f9f9', color='#f9f9f9') # Fondo gris suave original
    )
    """

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": f"Basándote en {template_tecnico}, genera el código con estas reglas estrictas de API para mapas:\n{descripcion_grafico}"}
        ],
        "temperature": 0,
        "stream": False
    }




@asset
def vis_mapa_renta_tenerife_ia(prompt_mapa_renta_tenerife_ia, gdf_renta_tenerife_solo_2023):
    """
    Asset que invoca a la IA para codificar el mapa de coropletas de Tenerife,
    inyectando coord_equal para evitar la distorsión geográfica de la isla.
    """

    codigo = pedir_codigo_a_ia(prompt_mapa_renta_tenerife_ia)
    
    # Capa de seguridad DataOps contra la alucinación de archivos
    lineas_limpias = []
    for linea in codigo.splitlines():
        if '.read_file(' in linea:
            linea = f"# Línea eliminada por sanitizador DataOps de Dagster: {linea.strip()}"
        lineas_limpias.append(linea)
    codigo_sanitizado = "\n".join(lineas_limpias)
    
    def funcion_puente_viridis(*args, **kwargs):
        if 'option' in kwargs:
            kwargs['cmap_name'] = kwargs.pop('option')
        return scale_fill_cmap(*args, **kwargs)

    entorno = globals().copy()
    entorno.update({
        'df': gdf_renta_tenerife_solo_2023, 
        'ggplot': ggplot,
        'aes': aes,
        'geom_map': geom_map,
        'scale_fill_cmap': scale_fill_cmap,
        'scale_fill_viridis': funcion_puente_viridis,
        'theme_void': theme_void,
        'theme': theme,
        'element_text': element_text,
        'element_rect': element_rect,
        'labs': labs,
        'coord_equal': coord_equal              # <--- INYECTADO CORRECTAMENTE AQUÍ
    })
    
    exec(codigo_sanitizado, entorno)
    grafico = entorno['generar_plot'](gdf_renta_tenerife_solo_2023)
    
    os.makedirs('data/outputs', exist_ok=True)
    grafico.save("data/outputs/02b_mapa_renta_tenerife_aislado.png", dpi=300)
    
    return "OK"






@asset(
    description=(
        "Procesa los datos limpios de actividad de 2023, muta el geocode a la estructura 2024, "
        "calcula el sector mayoritario numérico (incluyendo No consta) y aísla geográficamente la isla de Tenerife."
    )
)
def df_especializacion_tenerife_2023(actividad_clean):
    # 1. LEER LA CARTOGRAFÍA BASE PARA LEVANTAR LOS CÓDIGOS DE TENERIFE NATIVOS
    gdf_provincial = gpd.read_file('data/secciones_20240101_tenerife.json')
    gdf_provincial['geocode'] = gdf_provincial['geocode'].astype(str).str.strip()
    
    gdf_tenerife_solo = gdf_provincial[gdf_provincial['gcd_isla'] == 'ES709'].copy()
    gdf_tenerife_solo['cod_municipio'] = gdf_tenerife_solo['geocode'].str.extract(r'_(\d{5})')[0]
    if gdf_tenerife_solo['cod_municipio'].isna().any():
        gdf_tenerife_solo['cod_municipio'] = gdf_tenerife_solo['geocode'].str[4:9]
        
    codigos_municipios_tenerife = gdf_tenerife_solo['cod_municipio'].dropna().unique().tolist()

    # 2. PROCESAR EL DATAFRAME ALFANUMÉRICO RECIBIDO POR DEPENDENCIA
    df_actividad = actividad_clean.copy()
    df_actividad['año'] = df_actividad['año'].astype(int)
    df_actividad['geocode'] = df_actividad['geocode'].astype(str).str.strip()
    
    # ¡CRÍTICO!: Forzar num_casos a tipo numérico entero para que idxmax funcione bien
    df_actividad['num_casos'] = df_actividad['num_casos'].astype(int)
    
    df_actividad['cod_municipio'] = df_actividad['geocode'].str.extract(r'_(\d{5})')[0]
    if df_actividad['cod_municipio'].isna().any():
        df_actividad['cod_municipio'] = df_actividad['geocode'].str[4:9]
        
    df_2023 = df_actividad[
        (df_actividad['año'] == 2023) & 
        (df_actividad['cod_municipio'].isin(codigos_municipios_tenerife))
    ].copy()
    
    df_2023['geocode'] = '2024' + df_2023['geocode'].str[4:]
    
    # 3. CALCULAR EL SECTOR MAYORITARIO (Ahora sí comparando números reales)
    idx_max = df_2023.groupby('geocode')['num_casos'].idxmax()
    df_mayoristas = df_2023.loc[idx_max, ['geocode', 'Actividad económica']]
    df_mayoristas = df_mayoristas.rename(columns={'Actividad económica': 'sector_mayoritario'})
    df_mayoristas['sector_mayoritario'] = df_mayoristas['sector_mayoritario'].astype('category')
    
    # 4. CRUCE INNER PERFECTO
    gdf_final = pd.merge(gdf_tenerife_solo, df_mayoristas, on='geocode', how='inner')
    
    columnas_a_borrar = ['cod_municipio']
    for col in columnas_a_borrar:
        if col in gdf_final.columns:
            gdf_final = gdf_final.drop(columns=[col])
            
    return gdf_final



@asset(
    description="Genera el prompt estructurado para el Mapa de Especialización de Tenerife (2023) compacto, sin espacios en blanco y leyenda categórica Set2."
)
def prompt_mapa_especializacion_ia():
    template_tecnico = """
def generar_plot(df):
    plot = (
        ggplot(df)
        + geom_map(aes(fill='sector_mayoritario'), color='#ffffff', size=0.05)
        + scale_fill_brewer(type='qual', palette='Set2') # Escala cualitativa nominal idónea para sectores sin jerarquías falsas
        + coord_equal()
    )
    return plot
"""

    system_content = (
        "Eres un experto en Plotnine y Gramática de Gráficos. "
        "Tu objetivo es generar visualizaciones con un diseño profesional y limpio."
    )
    
    descripcion_grafico = """
    REGLAS SINTÁCTICAS CRÍTICAS:
    - Prohibido usar el prefijo 'p9.'.
    - Uso obligatorio de paréntesis global 'plot = ( ... )' con operador '+' al inicio de línea.
    - REGLA DE MAPA PERFECTO: Mantén obligatoriamente + coord_equal() para que la isla conserve su fisonomía real.

    CONTROL DE ETIQUETAS (LITERALES LIMPIOS):
    Usa labs() con estos textos:
    - title='Geografía del Trabajo: Especialización Productiva (2023)'
    - subtitle='Sector Económico Mayoritario por Sección Censal en la Isla de Tenerife'
    - fill='Sector Dominante'

    TEMA Y ESTÉTICA (ELIMINAR ESPACIO BLANCO Y RE-ESTILIZAR LEYENDA CATEGÓRICA):
    - Usa theme_void() para eliminar rejillas y ejes.
    - Dentro de theme(), configura exactamente estas propiedades. Mantenemos el lienzo en (11, 6) para ajustarnos a la silueta panorámica de la isla sin aire blanco:
        figure_size=(11, 6),                     # <--- RATIO PANORÁMICO EXCELENTE! Ajustado a la silueta de Tenerife para eliminar aire blanco
        legend_position='right',                 # Leyenda a la derecha
        legend_box_spacing=0.2,                  # Espacio controlado entre la isla y la escala
        plot_title=element_text(face='bold', size=18, margin={'b': 8}, ha='left', x=0.02),     # Anclaje limpio al 2% izquierdo
        plot_subtitle=element_text(size=13, margin={'b': 15}, ha='left', x=0.02),               # Perfectamente alineado
        legend_text=element_text(size=10),       # Textos de la escala legibles
        legend_title=element_text(size=11, face='bold', margin={'b': 10}),
        plot_background=element_rect(fill='#f9f9f9', color='#f9f9f9') # Fondo gris suave original
    )
    """

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": f"Basándote en {template_tecnico}, genera el código con estas reglas estrictas de API para mapas:\n{descripcion_grafico}"}
        ],
        "temperature": 0,
        "stream": False
    }


@asset
def vis_mapa_especializacion_ia(prompt_mapa_especializacion_ia, df_especializacion_tenerife_2023):
    """
    Asset que invoca a la IA para codificar el mapa de especialización de Tenerife,
    inyectando coord_equal y la paleta cualitativa para evitar distorsiones y jerarquías falsas.
    """
    codigo = pedir_codigo_a_ia(prompt_mapa_especializacion_ia)
    
    # Capa de seguridad DataOps contra la alucinación de archivos
    lineas_limpias = []
    for linea in codigo.splitlines():
        if '.read_file(' in linea:
            linea = f"# Línea eliminada por sanitizador DataOps de Dagster: {linea.strip()}"
        lineas_limpias.append(linea)
    codigo_sanitizado = "\n".join(lineas_limpias)
    
    entorno = globals().copy()
    entorno.update({
        'df': df_especializacion_tenerife_2023, # Recibe los datos transformados y mutados a geocode 2024
        'ggplot': ggplot,
        'aes': aes,
        'geom_map': geom_map,
        'scale_fill_brewer': scale_fill_brewer,
        'theme_void': theme_void,
        'theme': theme,
        'element_text': element_text,
        'element_rect': element_rect,
        'labs': labs,
        'coord_equal': coord_equal              # <--- INYECTADO CORRECTAMENTE AQUÍ
    })
    
    exec(codigo_sanitizado, entorno)
    grafico = entorno['generar_plot'](df_especializacion_tenerife_2023)
    
    os.makedirs('data/outputs', exist_ok=True)
    grafico.save("data/outputs/03_mapa_especializacion_2023.png", dpi=300)
    
    return "OK"




@asset(
    description=(
        "Calcula el porcentaje de cada sector económico por sección censal en Tenerife "
        "para el año 2023 (excluyendo No consta) y cruza con la cartografía 2024."
    )
)
def df_facetado_especializacion_tenerife_2023(actividad_clean):
    # 1. LEER LA CARTOGRAFÍA BASE PARA IDENTIFICAR MUNICIPIOS DE TENERIFE
    gdf_provincial = gpd.read_file('data/secciones_20240101_tenerife.json')
    gdf_provincial['geocode'] = gdf_provincial['geocode'].astype(str).str.strip()
    
    gdf_tenerife_solo = gdf_provincial[gdf_provincial['gcd_isla'] == 'ES709'].copy()
    gdf_tenerife_solo['cod_municipio'] = gdf_tenerife_solo['geocode'].str.extract(r'_(\d{5})')[0]
    if gdf_tenerife_solo['cod_municipio'].isna().any():
        gdf_tenerife_solo['cod_municipio'] = gdf_tenerife_solo['geocode'].str[4:9]
        
    codigos_municipios_tenerife = gdf_tenerife_solo['cod_municipio'].dropna().unique().tolist()

    # 2. FILTRAR DATASET ALFANUMÉRICO Y ELIMINAR 'NO CONSTA'
    df_actividad = actividad_clean.copy()
    df_actividad['año'] = df_actividad['año'].astype(int)
    df_actividad['geocode'] = df_actividad['geocode'].astype(str).str.strip()
    df_actividad['num_casos'] = df_actividad['num_casos'].astype(int)
    
    df_actividad['cod_municipio'] = df_actividad['geocode'].str.extract(r'_(\d{5})')[0]
    if df_actividad['cod_municipio'].isna().any():
        df_actividad['cod_municipio'] = df_actividad['geocode'].str[4:9]
        
    df_2023 = df_actividad[
        (df_actividad['año'] == 2023) & 
        (df_actividad['Actividad económica'] != 'No consta') & # <--- ¡PURGADO AQUÍ! Adiós al ruido
        (df_actividad['cod_municipio'].isin(codigos_municipios_tenerife))
    ].copy()
    
    # Muta el geocode para encajar con la estructura censal 2024
    df_2023['geocode'] = '2024' + df_2023['geocode'].str[4:]
    
    # 3. CÁLCULO DE PORCENTAJES SOBRE EL TOTAL REAL
    df_totals = df_2023.groupby('geocode')['num_casos'].transform('sum')
    df_2023['porcentaje'] = (df_2023['num_casos'] / df_totals).fillna(0) * 100
    
    # 4. CRUCE CON LA CARTOGRAFÍA BASE
    gdf_final = pd.merge(gdf_tenerife_solo, df_2023, on='geocode', how='inner')
    
    columnas_a_borrar = ['cod_municipio', 'municipio', 'año', 'num_casos']
    for col in columnas_a_borrar:
        if col in gdf_final.columns:
            gdf_final = gdf_final.drop(columns=[col])
            
    return gdf_final


@asset(
    description="Genera el prompt para la cuadrícula facetada de especialización (2x2) usando paleta continua Viridis."
)
def prompt_mapa_facetado_ia():
    template_tecnico = """
def generar_plot(df):
    plot = (
        ggplot(df)
        + geom_map(aes(fill='porcentaje'), color='none')
        + scale_fill_cmap(cmap_name='viridis', labels=lambda l: [f"{int(float(x))}%" for x in l])
        + facet_wrap('Actividad económica', ncol=2) # Al ser 4 categorías, ncol=2 creará una matriz simétrica perfecta de 2x2
        + coord_equal()
    )
    return plot
"""

    system_content = (
        "Eres un experto en Plotnine y Gramática de Gráficos. "
        "Tu objetivo es generar visualizaciones complejas facetadas con un diseño profesional y limpio."
    )
    
    descripcion_grafico = """
    REGLAS SINTÁCTICAS CRÍTICAS:
    - Prohibido usar el prefijo 'p9.'.
    - Uso obligatorio de paréntesis global 'plot = ( ... )' con operador '+' al inicio de línea.
    - REGLA DE MAPA PERFECTO: Mantén obligatoriamente + coord_equal() para preservar la fisonomía de Tenerife.

    CONTROL DE ETIQUETAS (LITERALES LIMPIOS):
    Usa labs() con estos textos exactos:
    - title='Anatomía Laboral: Penetración y Concentración Sectorial (2023)'
    - subtitle='Porcentaje de Empleo sobre el Total de la Sección Censal por Sector Económico en Tenerife'
    - fill='Peso del Sector'

    TEMA Y ESTÉTICA (SIMETRÍA 2X2 PANORÁMICA):
    - Usa theme_void() para eliminar rejillas y textos de coordenadas residuales.
    - Dentro de theme(), configura exactamente estas propiedades para ajustar la escala a la matriz de 4 mapas:
        figure_size=(13.0, 10.0),                  # Proporción ideal para una rejilla cuadrada de 2x2 sin deformaciones
        legend_position='right', 
        legend_box_spacing=0.1,                  
        legend_key_height=50,
        legend_key_width=12,
        plot_title=element_text(face='bold', size=18, margin={'b': 8}, ha='left', x=0.01),
        plot_subtitle=element_text(size=12, margin={'b': 20}, ha='left', x=0.01),
        legend_text=element_text(size=10),
        legend_title=element_text(size=11, face='bold', margin={'b': 10}),
        strip_text=element_text(face='bold', size=12, margin={'b': 8}), # Resalta los títulos de los 4 sectores
        plot_background=element_rect(fill='#f9f9f9', color='#f9f9f9')
    )
    """

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": f"Basándote en {template_tecnico}, genera el código con estas reglas estrictas de API para mapas facetados:\n{descripcion_grafico}"}
        ],
        "temperature": 0,
        "stream": False
    }


@asset
def vis_mapa_facetado_ia(prompt_mapa_facetado_ia, df_facetado_especializacion_tenerife_2023):
    """
    Asset que invoca a la IA para codificar la rejilla de mapas facetados por sector económico.
    """
    codigo = pedir_codigo_a_ia(prompt_mapa_facetado_ia)
    
    lineas_limpias = []
    for linea in codigo.splitlines():
        if '.read_file(' in linea:
            linea = f"# Línea eliminada por sanitizador DataOps de Dagster: {linea.strip()}"
        lineas_limpias.append(linea)
    codigo_sanitizado = "\n".join(lineas_limpias)
    
    def funcion_puente_viridis(*args, **kwargs):
        if 'option' in kwargs:
            kwargs['cmap_name'] = kwargs.pop('option')
        return scale_fill_cmap(*args, **kwargs)

    entorno = globals().copy()
    entorno.update({
        'df': df_facetado_especializacion_tenerife_2023, 
        'ggplot': ggplot,
        'aes': aes,
        'geom_map': geom_map,
        'scale_fill_cmap': scale_fill_cmap,
        'scale_fill_viridis': funcion_puente_viridis,
        'theme_void': theme_void,
        'theme': theme,
        'element_text': element_text,
        'element_rect': element_rect,
        'labs': labs,
        'coord_equal': coord_equal,
        'facet_wrap': facet_wrap                # <--- INYECTADO CORRECTAMENTE PARA LOS SMALL MULTIPLES
    })
    
    exec(codigo_sanitizado, entorno)
    grafico = entorno['generar_plot'](df_facetado_especializacion_tenerife_2023)
    
    os.makedirs('data/outputs', exist_ok=True)
    grafico.save("data/outputs/03b_mapas_especializacion_facetados.png", dpi=300)
    
    return "OK"



@asset(
    description=(
        "Filtra los datos limpios de renta para el año 2023, acotando la muestra "
        "a los municipios clave seleccionados (incluyendo Puerto de la Cruz mediante coincidencia parcial) "
        "para el análisis de dispersión."
    )
)
def df_boxplot_renta_municipios_2023(rentamedia_clean):
    """
    Asset de datos: Recibe el DataFrame limpio de renta.
    Filtra por el año 2023 y selecciona los 5 municipios clave de forma robusta.
    """
    df_renta = rentamedia_clean.copy()
    
    # Asegurar tipos de datos correctos para el filtrado estadístico
    df_renta['año'] = df_renta['año'].astype(int)
    df_renta['OBS_VALUE'] = pd.to_numeric(df_renta['OBS_VALUE'], errors='coerce')
    df_renta['municipio'] = df_renta['municipio'].astype(str).str.strip()
    
    # 1. Filtro base de año y nulos
    df_base = df_renta[
        (df_renta['año'] == 2023) & 
        (df_renta['OBS_VALUE'].notna())
    ]
    
    # 2. Filtrado inteligente: Coincidencias exactas + parcial para el Puerto
    municipios_exactos = ['Adeje', 'Arona', 'Santa Cruz de Tenerife', 'San Cristóbal de La Laguna']
    
    condicion_exactos = df_base['municipio'].isin(municipios_exactos)
    condicion_puerto = df_base['municipio'].str.contains('Puerto de la Cruz', case=False, na=False)
    
    df_filtrado = df_base[condicion_exactos | condicion_puerto].copy()
    
    # Homogeneizar el nombre del Puerto si viniera modificado en el CSV original
    idx_puerto = df_filtrado['municipio'].str.contains('Puerto de la Cruz', case=False, na=False)
    df_filtrado.loc[idx_puerto, 'municipio'] = 'Puerto de la Cruz'
    
    # Convertir municipio a tipo categórico para limpiar el gráfico
    df_filtrado['municipio'] = df_filtrado['municipio'].astype('category')
    
    return df_filtrado

@asset(
    description="Genera el prompt estructurado para el Boxplot de Renta (2023) con rotación de etiquetas para evitar colisiones."
)
def prompt_mapa_boxplot_ia():
    template_tecnico = """
def generar_plot(df):
    plot = (
        ggplot(df, aes(x='municipio', y='OBS_VALUE', fill='municipio'))
        + geom_boxplot(outlier_color='#e74c3c', outlier_size=1.5, outlier_alpha=0.6, width=0.6, show_legend=False)
        + scale_y_continuous(labels=lambda l: [f"{int(float(x)/1000)}k€" for x in l])
        + scale_fill_brewer(type='qual', palette='Set2')
    )
    return plot
"""

    system_content = (
        "Eres un experto en Plotnine y Gramática de Gráficos. "
        "Tu objetivo es generar diagramas de distribución estadística con un diseño limpio, evitando colisiones de texto."
    )
    
    descripcion_grafico = """
    REGLAS SINTÁCTICAS CRÍTICAS:
    - Prohibido usar el prefijo 'p9.'.
    - Uso obligatorio de paréntesis global 'plot = ( ... )' con operador '+' al inicio de línea.
    - REGLA DE LEYENDA TUFTE: Es obligatorio pasar show_legend=False en las geometrías y configurar legend_position='none' en el tema.

    CONTROL DE ETIQUETAS (LITERALES):
    Usa labs() con estos textos exactos:
    - title='La Brecha Interna: Distribución de la Riqueza por Barrio (2023)'
    - subtitle='Comparativa de la Renta Media por Persona entre Secciones Censales de Municipios Seleccionados'
    - x=''                  
    - y='Renta Media por Persona'

    TEMA Y ESTÉTICA ANTI-COLISIÓN DE TEXTO:
    - Usa theme_minimal() como lienzo base.
    - Dentro de theme(), configura exactamente estas propiedades. Ampliamos levemente el ancho a 11 para dar aire a las 5 cajas:
        figure_size=(11, 6),                     
        legend_position='none',                  
        plot_title=element_text(face='bold', size=16, margin={'b': 8}, ha='left', x=0.01),
        plot_subtitle=element_text(size=12, margin={'b': 20}, ha='left', x=0.01),
        
        # SOLUCIÓN AL CHOQUE DE TEXTO: Rotamos 20 grados las etiquetas del eje X y las alineamos a la derecha (ha='right')
        axis_text_x=element_text(size=10, face='bold', color='#333333', angle=20, ha='right'), 
        
        axis_text_y=element_text(size=10, color='#666666'),
        axis_title_y=element_text(size=11, face='bold', margin={'r': 10}),
        panel_grid_major_x=element_blank(),      
        panel_grid_minor_x=element_blank(),
        panel_grid_minor_y=element_blank(),
        plot_background=element_rect(fill='#f9f9f9', color='#f9f9f9')
    )
    """

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": f"Basándote en {template_tecnico}, genera el código con estas reglas estrictas de API para boxplots con rotación de texto:\n{descripcion_grafico}"}
        ],
        "temperature": 0,
        "stream": False
    }


@asset
def vis_mapa_boxplot_ia(prompt_mapa_boxplot_ia, df_boxplot_renta_municipios_2023):
    """
    Asset que invoca a la IA para codificar el diagrama de cajas municipal,
    evaluando la dispersión salarial y guardando el gráfico en alta resolución.
    """
    codigo = pedir_codigo_a_ia(prompt_mapa_boxplot_ia)
    
    # Capa de seguridad DataOps estándar de tu pipeline
    lineas_limpias = []
    for linea in codigo.splitlines():
        if '.read_file(' in linea:
            linea = f"# Línea eliminada por sanitizador DataOps de Dagster: {linea.strip()}"
        lineas_limpias.append(linea)
    codigo_sanitizado = "\n".join(lineas_limpias)
    
    entorno = globals().copy()
    entorno.update({
        'df': df_boxplot_renta_municipios_2023, 
        'ggplot': ggplot,
        'aes': aes,
        'geom_boxplot': geom_boxplot,
        'scale_y_continuous': scale_y_continuous,
        'scale_fill_brewer': scale_fill_brewer,
        'theme_minimal': theme_minimal,
        'theme': theme,
        'element_text': element_text,
        'element_rect': element_rect,
        'element_blank': element_blank,
        'labs': labs
    })
    
    exec(codigo_sanitizado, entorno)
    grafico = entorno['generar_plot'](df_boxplot_renta_municipios_2023)
    
    os.makedirs('data/outputs', exist_ok=True)
    grafico.save("data/outputs/04_boxplot_renta_municipal.png", dpi=300)
    
    return "OK"


@asset(
    description=(
        "Cruza los datos de actividad (Sector Servicios) con los de ocupación "
        "para el año 2023 en Tenerife a nivel de sección censal, usando la columna 'ocupacion'."
    )
)
def df_scatter_causalidad_tenerife_2023(actividad_clean, ocupacion_clean):
    """
    Asset de datos: Une las variables de actividad (Eje X) y ocupación (Eje Y)
    utilizando la columna 'ocupacion' confirmada.
    """
    # 1. OBTENER MUNICIPIOS DE TENERIFE
    gdf_provincial = gpd.read_file('data/secciones_20240101_tenerife.json')
    gdf_provincial['geocode'] = gdf_provincial['geocode'].astype(str).str.strip()
    gdf_tenerife_solo = gdf_provincial[gdf_provincial['gcd_isla'] == 'ES709'].copy()
    gdf_tenerife_solo['cod_municipio'] = gdf_tenerife_solo['geocode'].str.extract(r'_(\d{5})')[0]
    if gdf_tenerife_solo['cod_municipio'].isna().any():
        gdf_tenerife_solo['cod_municipio'] = gdf_tenerife_solo['geocode'].str[4:9]
    codigos_tenerife = gdf_tenerife_solo['cod_municipio'].dropna().unique().tolist()

    # 2. PROCESAR VARIABLE INDEPENDIENTE (EJE X): TRABAJADORES EN SERVICIOS
    df_act = actividad_clean.copy()
    df_act['año'] = df_act['año'].astype(int)
    df_act['num_casos'] = df_act['num_casos'].astype(int)
    df_act['geocode'] = df_act['geocode'].astype(str).str.strip()
    
    # Extraer código municipio
    df_act['cod_municipio'] = df_act['geocode'].str.extract(r'_(\d{5})')[0]
    if df_act['cod_municipio'].isna().any():
        df_act['cod_municipio'] = df_act['geocode'].str[4:9]
        
    df_servicios = df_act[
        (df_act['año'] == 2023) & 
        (df_act['Actividad económica'] == 'Servicios') & 
        (df_act['cod_municipio'].isin(codigos_tenerife))
    ].copy()
    
    df_servicios['geocode'] = '2024' + df_servicios['geocode'].str[4:]
    df_x = df_servicios.groupby('geocode')['num_casos'].sum().reset_index(name='empleo_servicios')

    # 3. PROCESAR VARIABLE DEPENDIENTE (EJE Y): COLUMNA 'ocupacion'
    df_ocu = ocupacion_clean.copy()
    df_ocu['año'] = df_ocu['año'].astype(int)
    df_ocu['num_casos'] = df_ocu['num_casos'].astype(int)
    df_ocu['geocode'] = df_ocu['geocode'].astype(str).str.strip()
    
    df_ocu['cod_municipio'] = df_ocu['geocode'].str.extract(r'_(\d{5})')[0]
    if df_ocu['cod_municipio'].isna().any():
        df_ocu['cod_municipio'] = df_ocu['geocode'].str[4:9]
        
    # Filtramos usando la columna confirmada 'ocupacion'
    df_bajos = df_ocu[
        (df_ocu['año'] == 2023) & 
        (df_ocu['ocupacion'].str.contains('elementales|operarios|bajos|bajo', case=False, na=False)) &
        (df_ocu['cod_municipio'].isin(codigos_tenerife))
    ].copy()
    
    df_bajos['geocode'] = '2024' + df_bajos['geocode'].str[4:]
    df_y = df_bajos.groupby('geocode')['num_casos'].sum().reset_index(name='ocupaciones_elementales')

    # 4. CRUCE FINAL
    df_scatter = pd.merge(df_x, df_y, on='geocode', how='inner')
    
    return df_scatter


@asset(
    description="Genera el prompt estructurado para el Scatter Plot de Causalidad con línea de tendencia estadística."
)
def prompt_scatter_causalidad_ia():
    template_tecnico = """
def generar_plot(df):
    plot = (
        ggplot(df, aes(x='empleo_servicios', y='ocupaciones_elementales'))
        + geom_point(color='#2ce3b3', alpha=0.4, size=2) # Punto en tono aguamarina con transparencia anti-solapamiento
        + geom_smooth(method='lm', color='#e74c3c', size=1.2, se=True) # Recta de regresión lineal en rojo con intervalo de confianza
        + scale_x_continuous(labels=lambda l: [f"{int(float(x))}" for x in l])
        + scale_y_continuous(labels=lambda l: [f"{int(float(x))}" for x in l])
    )
    return plot
"""

    system_content = (
        "Eres un expert en Plotnine y Gramática de Gráficos. "
        "Tu objetivo es generar diagramas de dispersión de alta calidad analítica con ajuste de curvas estadísticas."
    )
    
    descripcion_grafico = """
    REGLAS SINTÁCTICAS CRÍTICAS:
    - Prohibido usar el prefijo 'p9.'.
    - Uso obligatorio de paréntesis global 'plot = ( ... )' con operador '+' al inicio de línea.

    CONTROL DE ETIQUETAS (LITERALES PRECISOS):
    Usa labs() con estos textos exactos:
    - title='La Raíz de la Precariedad: El Efecto del Monocultivo de Servicios (2023)'
    - subtitle='Correlación Lineal entre el Volumen de Empleo Terciario y la Proliferación de Ocupaciones Elementales'
    - x='Población Ocupada en el Sector Servicios (por sección)'
    - y='Población en Ocupaciones Elementales / Operarios Bajos'

    TEMA Y ESTÉTICA ASÉPTICA PERIODÍSTICA:
    - Usa theme_minimal() para habilitar una rejilla de fondo muy sutil que permita medir las distancias y dispersión de los puntos de la nube.
    - Dentro de theme(), configura exactamente estas propiedades:
        figure_size=(10, 6),                     # Proporción rectangular estándar para análisis bidimensionales
        plot_title=element_text(face='bold', size=15, margin={'b': 8}, ha='left', x=0.01),
        plot_subtitle=element_text(size=11, margin={'b': 20}, ha='left', x=0.01),
        axis_text_x=element_text(size=10, color='#333333'), 
        axis_text_y=element_text(size=10, color='#666666'),
        axis_title_x=element_text(size=11, face='bold', margin={'t': 10}),
        axis_title_y=element_text(size=11, face='bold', margin={'r': 10}),
        plot_background=element_rect(fill='#f9f9f9', color='#f9f9f9') # Consistencia absoluta con el color de fondo de tu suite
    )
    """

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": f"Basándote en {template_tecnico}, genera el código con estas reglas estrictas de API para scatter plots estadísticos:\n{descripcion_grafico}"}
        ],
        "temperature": 0,
        "stream": False
    }


@asset
def vis_scatter_causalidad_ia(prompt_scatter_causalidad_ia, df_scatter_causalidad_tenerife_2023):
    """
    Asset que invoca a la IA para codificar el gráfico de dispersión y regresión,
    demostrando el lazo de causa-efecto del mercado laboral canario.
    """
    codigo = pedir_codigo_a_ia(prompt_scatter_causalidad_ia)
    
    lineas_limpias = []
    for linea in codigo.splitlines():
        if '.read_file(' in linea:
            linea = f"# Línea eliminada por sanitizador DataOps de Dagster: {linea.strip()}"
        lineas_limpias.append(linea)
    codigo_sanitizado = "\n".join(lineas_limpias)
    
    # Importamos las dependencias necesarias incluyendo geom_point y geom_smooth
    from plotnine import ggplot, aes, geom_point, geom_smooth, scale_x_continuous, scale_y_continuous, theme_minimal, theme, element_text, element_rect, labs
    
    entorno = globals().copy()
    entorno.update({
        'df': df_scatter_causalidad_tenerife_2023, 
        'ggplot': ggplot,
        'aes': aes,
        'geom_point': geom_point,
        'geom_smooth': geom_smooth,
        'scale_x_continuous': scale_x_continuous,
        'scale_y_continuous': scale_y_continuous,
        'theme_minimal': theme_minimal,
        'theme': theme,
        'element_text': element_text,
        'element_rect': element_rect,
        'labs': labs
    })
    
    exec(codigo_sanitizado, entorno)
    grafico = entorno['generar_plot'](df_scatter_causalidad_tenerife_2023)
    
    os.makedirs('data/outputs', exist_ok=True)
    grafico.save("data/outputs/05_scatter_causalidad_laboral.png", dpi=300)
    
    return "OK"


@asset(
    description="Filtra y prepara los datos de renta (2021 vs 2023) para comparar distribuciones de densidad."
)
def df_densidad_renta_comparativa(rentamedia_clean):
    df = rentamedia_clean.copy()
    
    # Asegurar tipos numéricos
    df['OBS_VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
    df['año'] = df['año'].astype(int)
    
    # Filtrar solo los dos años a comparar y eliminar nulos
    df_filtrado = df[
        (df['año'].isin([2021, 2023])) & 
        (df['OBS_VALUE'].notna())
    ].copy()
    
    # Convertir año a string o categórico para que plotnine cree dos curvas distintas
    df_filtrado['año'] = df_filtrado['año'].astype(str)
    
    return df_filtrado



@asset(
    description="Genera el prompt estructurado para el Gráfico de Densidades (2021 vs 2023) enfocado en la comparativa temporal."
)
def prompt_densidad_renta_ia():
    template_tecnico = """
def generar_plot(df):
    plot = (
        ggplot(df, aes(x='OBS_VALUE', fill='año', color='año'))
        + geom_density(alpha=0.3)
        + scale_x_continuous(labels=lambda l: [f"{int(float(x)/1000)}k€" for x in l])
        + scale_fill_brewer(type='qual', palette='Set1')
        + scale_color_brewer(type='qual', palette='Set1')
    )
    return plot
"""

    system_content = (
        "Eres un experto en Plotnine y Gramática de Gráficos. "
        "Tu objetivo es generar curvas de densidad comparativas con alta legibilidad estadística."
    )
    
    descripcion_grafico = """
    REGLAS SINTÁCTICAS CRÍTICAS:
    - Prohibido usar el prefijo 'p9.'.
    - Uso obligatorio de paréntesis global 'plot = ( ... )' con operador '+' al inicio de línea.

    CONTROL DE ETIQUETAS (LITERALES PRECISOS):
    Usa labs() con estos textos exactos:
    - title='Evolución de la Brecha: Distribución de la Renta (2021 vs 2023)'
    - subtitle='Densidad de secciones censales según nivel de renta: ¿Polarización o convergencia?'
    - x='Renta Media por Persona'
    - y='Densidad'

    TEMA Y ESTÉTICA ASÉPTICA PERIODÍSTICA:
    - Usa theme_minimal() con rejilla horizontal sutil.
    - Dentro de theme(), configura exactamente estas propiedades:
        figure_size=(10, 6),
        plot_title=element_text(face='bold', size=15, margin={'b': 8}, ha='left', x=0.01),
        plot_subtitle=element_text(size=11, margin={'b': 20}, ha='left', x=0.01),
        axis_text_x=element_text(size=10, color='#333333'), 
        axis_text_y=element_text(size=10, color='#666666'),
        axis_title_x=element_text(size=11, face='bold', margin={'t': 10}),
        axis_title_y=element_text(size=11, face='bold', margin={'r': 10}),
        plot_background=element_rect(fill='#f9f9f9', color='#f9f9f9'),
        legend_position='top',
        legend_title=element_blank()
    )
    """

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": f"Basándote en {template_tecnico}, genera el código con estas reglas estrictas de API para curvas de densidad:\n{descripcion_grafico}"}
        ],
        "temperature": 0,
        "stream": False
    }


@asset
def vis_densidad_renta_ia(prompt_densidad_renta_ia, df_densidad_renta_comparativa):
    """
    Asset que ejecuta el código generado por la IA para el gráfico de densidad,
    buscando dinámicamente el objeto gráfico creado.
    """
    codigo = pedir_codigo_a_ia(prompt_densidad_renta_ia)
    
    # Sanitización de seguridad
    lineas_limpias = [l for l in codigo.splitlines() if '.read_file(' not in l]
    codigo_sanitizado = "\n".join(lineas_limpias)
    
    from plotnine import ggplot, aes, geom_density, scale_x_continuous, scale_fill_brewer, scale_color_brewer, theme_minimal, theme, element_text, element_rect, element_blank, labs
    
    entorno = globals().copy()
    entorno.update({
        'df': df_densidad_renta_comparativa,
        'ggplot': ggplot, 'aes': aes, 'geom_density': geom_density,
        'scale_x_continuous': scale_x_continuous,
        'scale_fill_brewer': scale_fill_brewer, 'scale_color_brewer': scale_color_brewer,
        'theme_minimal': theme_minimal, 'theme': theme, 'labs': labs,
        'element_text': element_text, 'element_rect': element_rect, 'element_blank': element_blank
    })
    
    # Ejecutamos el código
    exec(codigo_sanitizado, entorno)
    
    # BÚSQUEDA DINÁMICA: Buscamos si la IA creó 'plot', 'generar_plot' o 'grafico'
    grafico = None
    if 'plot' in entorno:
        grafico = entorno['plot']
    elif 'generar_plot' in entorno and callable(entorno['generar_plot']):
        grafico = entorno['generar_plot'](df_densidad_renta_comparativa)
    elif 'grafico' in entorno:
        grafico = entorno['grafico']
    
    if grafico is None:
        raise ValueError("La IA no generó una variable válida llamada 'plot', 'grafico' o 'generar_plot'.")
    
    os.makedirs('data/outputs', exist_ok=True)
    grafico.save("data/outputs/06_densidad_renta_comparativa.png", dpi=300)
    
    return "OK"