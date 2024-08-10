FROM debian:bullseye-slim

# Actualizar el gestor de paquetes y asegurar la instalación de los paquetes necesarios
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY . /app

RUN pip3 install -r requirements.txt

CMD ["python3","main.py"]

