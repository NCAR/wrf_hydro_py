from gdrive_download import download_file_from_google_drive, untar


def download(version='latest'):

    id_dict = {
        'latest': '1VrWVve8fhYobDg2xDrgHfiAi7VBDmV9T'
    }
    id = id_dict[version]
    target_name = 'collection_data.tar.gz'
    download_file_from_google_drive(id, target_name)
    untar(target_name)


if __name__ == "__main__":
    download()
