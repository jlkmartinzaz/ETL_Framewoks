# Introducción

Este proyecto está diseñado para procesar datos relacionados con riesgo
de cáncer de pulmón mediante un flujo ETL (Extract, Transform, Load). Se
utiliza **Python** para la lógica de extracción y transformación de
datos, y **HDFS** para el almacenamiento distribuido de datos. Además,
se emplea **Docker** y **docker-compose** para facilitar la replicación
del entorno de desarrollo y despliegue.

# Estructura del Proyecto

El proyecto sigue la siguiente organización de carpetas:

-   `config/` - Configuraciones del proyecto, variables de entorno,
    paths.

-   `extract/` - Módulos para la extracción de datos desde CSV o APIs.

-   `transform/` - Funciones de limpieza y transformación de datos.

-   `load/` - Módulos para guardar datos transformados en HDFS o
    archivos locales.

-   `data/` - Carpeta local para almacenamiento temporal de datos.

-   `main.py` - Script principal que ejecuta el flujo ETL completo.

-   `requirements.txt` - Dependencias de Python.

-   `Dockerfile` - Definición de la imagen Docker del proyecto.

-   `docker-compose.yml` - Configuración de servicios Docker (HDFS,
    contenedor del proyecto, etc.)

# Desarrollo y Dependencias

## Dependencias Python

El proyecto requiere Python 3.9+ y las siguientes librerías:

``` {.bash language="bash"}
pyspark
        pandas
        numpy
        pyhdfs
        requests
        flask
        python-dotenv
```

Instalación rápida con pip:

``` {.bash language="bash"}
pip install -r requirements.txt
```

## Variables de Configuración

El proyecto utiliza variables de entorno para configurar HDFS y otras
rutas:

``` {.python language="python"}
HDFS_HOST = os.getenv("HDFS_HOST", "hadoop_namenode")
        HDFS_PORT = int(os.getenv("HDFS_PORT", 9000))
        HDFS_DIR = os.getenv("HDFS_DIR", "/lung_cancer")
        HDFS_URL = f"hdfs://{HDFS_HOST}:{HDFS_PORT}{HDFS_DIR}"
```

## Ejecución del proyecto

Ejecutar el flujo ETL principal:

``` {.bash language="bash"}
python main.py
```

# Comandos de Docker

A continuación, se listan los comandos Docker más importantes usados en
el proyecto:

## Construcción de imágenes

``` {.bash language="bash"}
docker build -t etl_app .
        docker build --no-cache -t etl_app .
```

## Listar imágenes y contenedores

``` {.bash language="bash"}
docker images
        docker ps
        docker ps -a
```

## Ejecutar contenedores

``` {.bash language="bash"}
docker run -d --name etl_app etl_app
        docker run -it etl_app /bin/bash
        docker run -p 8080:8080 etl_app
        docker run -v /host/path:/container/path etl_app
```

## Detener y eliminar contenedores

``` {.bash language="bash"}
docker stop etl_app
        docker rm etl_app
        docker rm -f etl_app
```

## Logs y acceso

``` {.bash language="bash"}
docker logs -f etl_app
        docker exec -it etl_app /bin/bash
```

## Docker Compose

``` {.bash language="bash"}
docker-compose up -d
        docker-compose down
        docker-compose logs -f
```

# Prompts de ChatGPT más importantes

Durante el desarrollo, se utilizaron prompts estratégicos para resolver
problemas técnicos y optimizar el flujo ETL:

-   Diagnóstico de errores en PySpark y HDFS.

-   Limpieza y transformación de CSV con Pandas.

-   Generación de scripts Docker y docker-compose optimizados.

-   Validación de seguridad en manejo de variables de entorno.

-   Creación de documentación técnica y README automatizado.

# Conclusión

Este proyecto proporciona un flujo ETL robusto utilizando Python y
Spark, con almacenamiento seguro en HDFS y contenedores Docker para
replicación de entorno. La documentación se puede generar
automáticamente en Markdown usando **Pandoc**:

``` {.bash language="bash"}
pandoc doc.tex -o README.md
```
