from gdrive_download import download_file_from_google_drive, untar

def download():
    target_name = 'collection_data.tar.gz'
    download_file_from_google_drive(
        '1VrWVve8fhYobDg2xDrgHfiAi7VBDmV9T',
        target_name
    )
    untar(target_name)


if __name__ == "__main__":
    download()
