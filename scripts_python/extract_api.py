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
parsed_data = []
for repo in data:
    parsed_data.append({
            # --- Campos básicos ---
            "id": repo.get("id"),
            "name": repo.get("name"),
            
            # --- NUEVOS CAMPOS---
            "owner_login": repo.get("owner", {}).get("login"), # Datos del dueño (anidado)
            "description": repo.get("description"),
            "created_at": repo.get("created_at"),              # Ideal para transformaciones en Scala
            "updated_at": repo.get("updated_at"),
            "size": repo.get("size"),
            "stargazers_count": repo.get("stargazers_count"),  # Contador de estrellas
            "watchers_count": repo.get("watchers_count"), # Diferente a stars, interesante comparar
            "language": repo.get("language"),
            "forks_count": repo.get("forks_count"),
            "open_issues_count": repo.get("open_issues_count"),
            "license_name": repo.get("license", {}).get("name") if repo.get("license") else None, # Licencia (anidado)
            "topics": ", ".join(repo.get("topics", []))        # Lista de etiquetas convertida a texto
        })


# 3. Guardar en el "Disco Duro"
df = pd.DataFrame(parsed_data)
df.to_csv("data/repos_python.csv", index=False)
print("¡Archivo guardado en la carpeta data!")