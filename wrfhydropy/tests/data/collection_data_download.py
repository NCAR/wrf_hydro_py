import os
import pathlib
import shutil
from .gdrive_download import download_file_from_google_drive, untar
from wrfhydropy.core.ioutils import md5


def download(version='latest'):
    orig_dir = pathlib.Path('.').resolve()
    data_dir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(data_dir)
    id_md5_dict = {
        'latest': {
            'id': '1VrWVve8fhYobDg2xDrgHfiAi7VBDmV9T',
            'md5': '51847a29eaeea0377bfece7ea662500e'
        }
    }
    id = id_md5_dict[version]['id']
    the_md5 = id_md5_dict[version]['md5']
    target_name = pathlib.Path('collection_data.tar.gz')
    if target_name.exists() and md5(target_name) == the_md5:
        if not pathlib.Path('collection_data').exists():
            untar(str(target_name))
        os.chdir(orig_dir)
        return None
    if target_name.exists():
        target_name.unlink()
    if pathlib.Path('collection_data').exists():
        shutil.rmtree('collection_data')
    download_file_from_google_drive(id, str(target_name))
    untar(str(target_name))
    os.chdir(orig_dir)


if __name__ == "__main__":
    download()
