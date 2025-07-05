import yaml

# Replace with your file path
file_path = 'data_quality.yaml'

# Open and read the YAML file
with open(file_path, 'r') as file:
    data_quality = yaml.safe_load(file)


# Useful information from zip files
colspecs = [
    (0, 8),     # fecha_mat (ddmmyyyy)
    (17, 47),   # marca
    (47, 69),   # modelo
    (197, 228)  # localidad
]

colnames = [
    "fecha_matriculacion",
    "marca",
    "modelo",
    "localidad"
]