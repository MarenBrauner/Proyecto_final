# Desigualdad estructural: El coste del monocultivo en Tenerife

## Objetivo
- Investigar los efectos del modelo económico adoptado en la provincia de Santa Cruz de Tenerife
- Existe una distribución homogénea de la riqueza?
- Empezando por el contexto macroeconómico en la provincia hasta poner la isla de Tenerife en el foco
- Punto de partida: Prácticas anteriores 

## Configuración y DataOps
- Procedimiento idéntico al de las prácticas anteriores
- proyecto.py contiene el código del pipeline orquestado Dagster que contiene los assets y checks
 (no se han implementado sensores)
- Un notebook auxiliar para la exploración de los datos
- Uso de la IA en la capa de visualización para generar el código de plotnine, respetando la gramática de gráficos

  <img width="646" height="604" alt="cp1" src="https://github.com/user-attachments/assets/b0a04b1b-36d3-4757-90fa-5ff59259481d" />
  <img width="617" height="588" alt="cp2" src="https://github.com/user-attachments/assets/80fa9b40-cb3c-4ee6-9d2b-02642862cc50" />

## Preprocesado
- Cuatro conjuntos de datos: distribución de la renta según la fuente de ingresos, renta media y mediana, ocupación
  (nivel del puesto) y actividad (sector del empleo) por secciones de los años 2021-2023
- Eliminación de columnas redundantes, renombrar columnas, comprobar y cambiar el tipo y formato de los datos
- Eliminar la dimensión de sexo sumando los valores de hombres y mujeres
- **Comprobación de valores nulos**: Imputar para preservar la continuidad visual (Principio de Gestalt)
- Los **checks** implementados sirven para comprobar la calidad de los datos antes de visualizarlos
  - Continuidad (--> valores nulos)
  - Veracidad visual y Similitud (--> valores absurdos y rango)
  - Carga cognitiva (--> limitar la visualización a los datos de interés)
  - Figura y fondo (--> se asegura en la generación de los gráficos)

 ## Gráficos
- Uso de facetas para evitar sobrecarga
- Contraste suficiente en la selección de las líneas y colores (paletas)
- Alineación de los títulos
- Adaptación del los tamaños y proporciones para evitar espacio blanco
- Eliminar fondos/rejillas no necesarias

 <img width="3600" height="2100" alt="01_evolucion_islas_2x2" src="https://github.com/user-attachments/assets/9e7ee0af-bdae-4fbb-8e3c-7f5e15d5e148" />
 
- Hay una recuperación económica después de la pandemia
- Incrementación de los ingresos por salarios y disminución de ingresos por desempleo

<img width="2700" height="1800" alt="02_mapa_coropletas_renta_2023" src="https://github.com/user-attachments/assets/1bf28536-286e-4016-9371-d66873e26ed0" />
<img width="3300" height="1800" alt="02b_mapa_renta_tenerife_aislado" src="https://github.com/user-attachments/assets/7d40dff9-9922-46b0-b952-f7eba683e131" />

- Zonas de rentas más altas: urbanas o turísticas
- Desigualdad, desconexión espacial

<img width="3900" height="3000" alt="03b_mapas_especializacion_facetados" src="https://github.com/user-attachments/assets/96b83ab1-279b-4c7f-b999-870557d1a3fd" />

- El sector mayoritario es el sector del servicio
- Existe un **monocultivo** sectorial

<img width="3300" height="1800" alt="04_boxplot_renta_municipal" src="https://github.com/user-attachments/assets/3147a750-df9f-4eed-b38f-393984cc701e" />

- La brecha interna es grande en las zonas urbanas
- En las zonas turísticas también hay outliers

<img width="3000" height="1800" alt="05_scatter_causalidad_laboral" src="https://github.com/user-attachments/assets/5a5e7b5d-8a49-4675-8716-53942a907af4" />

- Relación entre volumen de empleo en el sector servicios y las ocupaciones elementales o de nivel más bajo
- Cada nuevo puesto de trabajo creado en el sector servicios en Tenerife lleva aparejada una nueva plaza en una ocupación
elemental o de baja cualificación

<img width="3000" height="1800" alt="06_densidad_renta_comparativa" src="https://github.com/user-attachments/assets/141c2940-f61c-4765-b082-2c1566e1d555" />

- Una fracción ha incrementado su renta
- La masa principal de la población permanece anclada en la franja baja

La recuperación post-pandemia ha consolidado una estructura social donde la mejora es un fenómeno de unos pocos,
mientras que la mayoría social permanece en niveles económicos bajos


