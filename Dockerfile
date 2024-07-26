# Usa una imagen base de Python
FROM python:3.11.9

# Establece el directorio de trabajo en el contenedor
WORKDIR /app

# Copia el script Python al contenedor
COPY . /app/

COPY requirements.txt .

# Instala las dependencias del proyecto
RUN pip install --no-cache-dir -r requirements.txt

# Define el comando por defecto
CMD ["python", "main.py"]

