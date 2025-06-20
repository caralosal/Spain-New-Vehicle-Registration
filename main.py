from src.data_gathering import DataGatherer

def main():

    # URL in which we are going to check all the files
    dgt_url = "https://www.dgt.es/menusecundario/dgt-en-cifras/matraba-listados/matriculaciones-automoviles-mensual.html"
    gatherer = DataGatherer(dgt_url)

    # Download new data to bronze
    gatherer.download_to_bronze()

if __name__ == "__main__":
    main()