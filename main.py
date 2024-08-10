from flask import Flask, jsonify, request, send_file
import requests
import json
import csv
from collections import defaultdict
from datetime import datetime
import os
from google.cloud import storage

app = Flask(__name__)

@app.route('/', methods=['GET','POST'])
def create_files():
    # Paso 1: Consumo de la API y creación del archivo JSON
    url = "https://dummyjson.com/users"
    # Hacer la solicitud GET a la API
    response = requests.get(url)
    # Verificar si la solicitud fue exitosa
    if response.status_code == 200:
    # Convertir la respuesta en formato JSON
        data = response.json()

        # Extraer los usuarios del JSON
        data_ = data['users']

        # Variables para guardar la fecha YYYYMMDD y el nombre del archivo json
        fecha = datetime.now().strftime('%Y%m%d')
        # Nombre del archivo json
        archivo_json = f'data_{fecha}.json'

        # Crear archivo JSON data_json
        with open(archivo_json, 'w') as json_file:
            json.dump(data, json_file, indent=4)

        # Paso 2 Crear archivo CSV ETL_.csv
        # Nombre del archivo CSV
        archivo_csv = f'ETL_{fecha}.csv'
        with open(archivo_csv, 'w', newline='',) as csv_file:
            writer = csv.writer(csv_file)

            # Escribir la cabecera
            headers = data_[0].keys()
            writer.writerow(headers)

            # Escribir las filas
            for user in data_:
                writer.writerow(user.values())

        # Paso 3 crear un resumen de los datos y crear archivo summary_csv
        
        # 1. Calcular el total de registros
        total_registros = len(data['users'])

        # 2. Contar por género
        conteo_genero = defaultdict(int)
        for user in data['users']:
            conteo_genero[user['gender']] += 1

        # 3. Contar cuántas personas por sexo tienen la misma edad en un rango de 10 en 10
        conteo_edad_sexo = defaultdict(lambda: defaultdict(int))
        for user in data['users']:
            edad_rango = (user['age'] // 10) * 10
            conteo_edad_sexo[user['gender']][edad_rango] += 1

        # 4. Contar cuántas personas por sexo viven en la misma ciudad
        conteo_ciudad_sexo = defaultdict(lambda: defaultdict(int))
        for user in data['users']:
            conteo_ciudad_sexo[user['address']['city']][user['gender']] += 1

        # 5. Contar cuántos sistemas operativos y su total
        conteo_sistemas_operativos = defaultdict(int)

        for user in data['users']:
    
            user_agent = user['userAgent']

            def extract_os(user_agent):
                # Dividir la cadena usando paréntesis y espacio
                parts = user_agent.split(' ')

                # Buscar si alguna de las partes contiene la información del sistema operativo
                for part in parts:
                    if 'Windows' in part:
                        return 'Windows'
                    elif 'Macintosh' in part or 'Mac OS X' in part:
                        return 'Apple'
                    elif 'Linux' in part or 'Linux' in part:
                        return 'Linux'

                return 'Other'
    
        #Contar los sistemas operativos
            system_os = extract_os(user_agent)
            conteo_sistemas_operativos[system_os] += 1
    
        # Nombre del archivo CSV
        filename = f'summary_{fecha}.csv'

        # 1. Escribir el total de registros en el CSV
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Registre", total_registros])

        # 2. Escribir el conteo por género
            writer.writerow([])
            writer.writerow(["Gender", "Total"])
            for gender, count in conteo_genero.items():
                writer.writerow([gender, count])

        # 3. Escribir personas por sexo con la misma edad en rangos de 10 en 10
            writer.writerow([])
            writer.writerow(["Age", "Gender", "Total"])
            for gender, age_ranges in conteo_edad_sexo.items():
                for age_range, count in age_ranges.items():
                    writer.writerow([f"{age_range}-{age_range+9}", gender, count])

        # 4. Escribir personas por sexo en la misma ciudad
            writer.writerow([])
            writer.writerow(["City", "Gender", "Total"])
            for gender, cities in conteo_ciudad_sexo.items():
                for city, count in cities.items():
                    writer.writerow([gender, city, count])

        # 5. Escribir sistemas operativos y su total
            writer.writerow([])
            writer.writerow(["SO", "Total"])
            for so, count in conteo_sistemas_operativos.items():
                writer.writerow([so, count])

        #subir archivos al bucket

        # Ruta al archivo de credenciales de tu cuenta de servicio
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credenciales.json'

        # Nombre de tu bucket de almacenamiento en GCP
        bucket_name = 'ti_is_technicaltest' 
           
        # Lista de archivos que quieres subir y sus nombres de destino en GCP
        files_to_upload = [
            {'file_path': f'data_{fecha}.json', 'dest_file_name': f'data_{fecha}.json'},
            {'file_path': f'ETL_{fecha}.csv', 'dest_file_name': f'ETL_{fecha}.csv'},
            {'file_path': f'summary_{fecha}.csv', 'dest_file_name': f'summary_{fecha}.csv'},
        ]

        def upload_file(bucket_name, file_path, dest_file_name):
        # Crear una instancia de cliente de Google Cloud Storage
            storage_client = storage.Client()

        # Obtener el bucket
            bucket = storage_client.bucket(bucket_name)

        # Crear un blob (objeto) en el bucket
            blob = bucket.blob(dest_file_name)

        # Subir el archivo al blob
            blob.upload_from_filename(file_path)

            print(f'Archivo {file_path} subido a {bucket_name} como {dest_file_name}')

        def upload_multiple_files():
            for file in files_to_upload:
                upload_file(bucket_name, file['file_path'], file['dest_file_name'])
    
        # Llamar a la función para subir los archivos
        upload_multiple_files()
        
        return jsonify({"message": "Archivo data_json creado exitosamente!", "file": archivo_json},
                       {"message": "Archivo ETL_csv creado exitosamente!", "file": archivo_csv},
                       {"message": "Archivo summary_csv creado exitosamente!", "file": filename},
                       "Archivos subidos al Bucked de Google Cloud")
    else:
        return jsonify(f"Error en la solicitud. Código de estado: {response.status_code}")
    
if __name__ == '__main__':
    app.run(host= "0.0.0.0",port=4000,debug=True)