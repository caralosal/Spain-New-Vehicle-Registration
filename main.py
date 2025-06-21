from src.data_gathering import DataGatherer
from src.data_cleaning import DataCleaner
from src.utils.paths import BRONZE, SILVER

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

def main():

    # URL in which we are going to check all the files
    dgt_url = "https://www.dgt.es/menusecundario/dgt-en-cifras/matraba-listados/matriculaciones-automoviles-mensual.html"
    gatherer = DataGatherer(base_page_url= dgt_url,
                            bronze_path= BRONZE)

    # Download new data to bronze
    gatherer.download_to_bronze()

    # Process .zip file and create .ftr and move them to silver
    cleaner = DataCleaner(bronze_path = BRONZE,
                          silver_path = SILVER,
                          colspecs = colspecs,
                          colnames = colnames)
    
    cleaner.process_files_to_silver()

if __name__ == "__main__":
    main()