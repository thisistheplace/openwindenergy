import multiprocessing as mp
import time

from ..constants import *
from ..system.process import run_subprocess

LOG = mp.get_logger()


def install_fonts():
    """
    Installs fonts required for tileserver-gl
    """

    global BUILD_FOLDER, TILESERVER_FOLDER

    LOG.info("Attempting tileserver fonts installation...")

    tileserver_font_folder = TILESERVER_FOLDER / "fonts"

    if BUILD_FOLDER == "build-docker/":

        # On docker openwindenergy-fonts container copies fonts to 'fonts/' folder
        # So need to wait for it to finish this

        while True:
            if tileserver_font_folder.is_dir():
                LOG.info("Tileserver fonts folder already exists - SUCCESS")
                return True
            time.sleep(5)

    else:

        # Server build clones fonts from https://github.com/open-wind/openmaptiles-fonts.git
        if tileserver_font_folder.is_dir():
            return True

        # Download tileserver fonts

        if not Path(TILESERVER_FONTS_GITHUB.name).is_dir():

            LOG.info("Downloading tileserver fonts")

            inputs = run_subprocess(["git", "clone", TILESERVER_FONTS_GITHUB])

        working_dir = os.getcwd()
        os.chdir(TILESERVER_FONTS_GITHUB.name)

        LOG.info("Generating PBF fonts")

        if not run_subprocess(["npm", "install"], return_bool=True):
            os.chdir(working_dir)
            return False

        if not run_subprocess(["node", "./generate.js"], return_bool=True):
            os.chdir(working_dir)
            return False

        os.chdir(working_dir)

        LOG.info("Copying PBF fonts to tileserver folder")

        tileserver_font_folder_src = Path(TILESERVER_FONTS_GITHUB.name) / "_output"

        shutil.copytree(tileserver_font_folder_src, tileserver_font_folder)

        return True
