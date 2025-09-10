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

1.  **Construir y administrar imágenes:**

    -   `docker build -t etl_app .`

    -   `docker images`

2.  **Contenedores: crear, ejecutar, detener:**

    -   `docker-compose up -d`

    -   `docker-compose up -d etl_app`

    -   `docker-compose down`

    -   `docker stop etl_app`

    -   `docker start etl_app`

    -   `docker restart etl_app`

    -   `docker rm etl_app`

    -   `docker rmi etl_app`

3.  **Acceder a contenedores:**

    -   `docker exec -it etl_app bash`

    -   `docker exec -it etl_app python3 main.py`

4.  **Ver estado y logs:**

    -   `docker ps`

    -   `docker ps -a`

    -   `docker logs etl_app`

    -   `docker logs <CONTAINER_ID>`

5.  **Redes y volúmenes:**

    -   `docker network ls`

    -   `docker volume ls`

    -   `docker volume create etl_framewoks_db_data`

    -   `docker inspect etl_postgres_db`

6.  **Limpieza y resolución de conflictos:**

    -   `docker-compose up –remove-orphans`

    -   `docker rm -f etl_postgres_db`

    -   `docker rm -f etl_app`

    -   `docker container prune`

    -   `docker image prune -a`

7.  **Troubleshooting HDFS / Spark / Java:**

    -   `which hdfs`

    -   `hdfs version`

    -   `echo $HADOOP_HOME`

    -   `echo $JAVA_HOME`

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
