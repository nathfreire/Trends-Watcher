# Fase 1: Extracción de datos desde API
# Este script extrae tendencias de GitHub
# Este archivo .py es el que va a ir a buscar los datos

""" def main():
    pass

if __name__ == "__main__":
    main()
"""

import requests
import pandas as pd

# 1. Pedir datos a la API (Como llamar por teléfono a GitHub)
url = "https://api.github.com/search/repositories?q=language:python&sort=stars"
response = requests.get(url)
data = response.json()['items']

# 2. Seleccionar lo que nos sirve (Limpieza manual básica)
repos_list = []
for repo in data:
    repos_list.append({
        'name': repo['name'],
        'stars': repo['stargazers_count'],
        'language': repo['language']
    })

# 3. Guardar en el "Disco Duro"
df = pd.DataFrame(repos_list)
df.to_csv("data/repos_python.csv", index=False)
print("¡Archivo guardado en la carpeta data!")