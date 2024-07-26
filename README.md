# data_engineering
Data Engineering

Comando para hacer el build

docker build -t scripts-data-eng --no-cache .	

Comando para correr la imagen

docker run --env-file .env scripts-data-eng  python main.py --type all --count 10000000

Si ya tienes data generada puedes ejecutar unicamente el read

docker run --env-file .env scripts-data-eng  python main.py --type read 