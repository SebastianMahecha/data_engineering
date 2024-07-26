# data_engineering

El  desarrollo realizado en esta proyecto, consiste en una aplicacion de consola, que permite realizar todo el procesamiento de datos; desde su generacion, hasta su analisis, por medio de diferentes librerias en Python. Por otro lado, se pretende evidenciar un flujo de trabajo cotidiano y la eficiencia de 3 librerias para el manejo de volumenes de datos grandes.

Comando para hacer el build

```
docker build -t scripts-data-eng --no-cache .	
```

Comando para correr la imagen
```
docker run --env-file .env scripts-data-eng  python main.py --type all --count 10000000
```

Si ya tienes data generada puedes ejecutar unicamente el read

```
docker run --env-file .env scripts-data-eng  python main.py --type read
```
