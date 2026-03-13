import urllib.request
import gzip
import csv
import json
import os
from collections import defaultdict

# URLs oficiales de los datasets de IMDb
URL_EPISODES = "https://datasets.imdbws.com/title.episode.tsv.gz"
URL_RATINGS = "https://datasets.imdbws.com/title.ratings.tsv.gz"

# Archivos temporales
FILE_EPISODES = "episodes.tsv.gz"
FILE_RATINGS = "ratings.tsv.gz"

# Carpeta de salida para los JSON
OUTPUT_DIR = "api"
PREFIX_LENGTH = 5  # Agruparemos por los primeros 5 caracteres (ej. "tt111")
MIN_VOTES = 1      # BAJADO DE 50 A 1 PARA INCLUIR SERIES ESPAÑOLAS Y CLÁSICAS

def download_file(url, filename):
    print(f"Descargando {url}...")
    urllib.request.urlretrieve(url, filename)

def process_data():
    # 1. Cargar las notas (ratings) en memoria
    print("Procesando notas...")
    ratings = {}
    with gzip.open(FILE_RATINGS, 'rt', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader) # Saltar cabecera
        for row in reader:
            if len(row) == 3:
                tconst, rating, num_votes = row
                # Filtrar capítulos con muy pocos votos (ahora mucho más permisivo)
                if num_votes.isdigit() and int(num_votes) >= MIN_VOTES:
                    # Guardamos la nota y los votos para desempatar luego
                    ratings[tconst] = (float(rating), int(num_votes))

    # 2. Agrupar capítulos por serie
    print("Procesando episodios...")
    series_data = defaultdict(list)
    with gzip.open(FILE_EPISODES, 'rt', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader) # Saltar cabecera
        for row in reader:
            if len(row) == 4:
                tconst, parent_tconst, season, episode = row
                
                # Si el capítulo tiene nota, y tiene temporada/episodio válidos
                if tconst in ratings and season.isdigit() and episode.isdigit():
                    series_data[parent_tconst].append({
                        "r": ratings[tconst][0],
                        "v": ratings[tconst][1], # Votos (para desempate interno)
                        "s": int(season),
                        "e": int(episode)
                    })

    # 3. Ordenar, quedarse con el Top 5 y agrupar por prefijos
    print("Generando Top 5 y agrupando por prefijos...")
    sharded_data = defaultdict(dict)
    
    for series_id, episodes in series_data.items():
        # Ordenar episodios por nota de mayor a menor y, en caso de empate, por mayor cantidad de votos
        episodes.sort(key=lambda x: (x["r"], x["v"]), reverse=True)
        
        top5 = episodes[:5]
        
        # Eliminar el dato de los votos ('v') antes de guardar para no engordar los JSON
        for ep in top5:
            del ep["v"]
            
        # Obtener el prefijo (ej. de tt1119644 -> tt111)
        prefix = series_id[:PREFIX_LENGTH]
        sharded_data[prefix][series_id] = top5

    # 4. Guardar en archivos JSON diminutos
    print("Guardando archivos JSON...")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    for prefix, data in sharded_data.items():
        file_path = os.path.join(OUTPUT_DIR, f"{prefix}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            # separators=(',', ':') elimina espacios en blanco para ahorrar bytes
            json.dump(data, f, separators=(',', ':'))

    print("¡Proceso completado con éxito!")

def cleanup():
    # Borrar los archivos gigantes descargados para dejar limpio el servidor
    print("Limpiando archivos temporales...")
    if os.path.exists(FILE_EPISODES): os.remove(FILE_EPISODES)
    if os.path.exists(FILE_RATINGS): os.remove(FILE_RATINGS)

if __name__ == "__main__":
    try:
        download_file(URL_RATINGS, FILE_RATINGS)
        download_file(URL_EPISODES, FILE_EPISODES)
        process_data()
    finally:
        cleanup()
