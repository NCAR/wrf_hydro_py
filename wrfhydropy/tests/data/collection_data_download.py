from gdrive_download import download_file_from_google_drive

def download():
    download_file_from_google_drive(
        '1VrWVve8fhYobDg2xDrgHfiAi7VBDmV9T',
        'collection_data.tar.gz'
    )

if __name__ == "__main__":
    download()
