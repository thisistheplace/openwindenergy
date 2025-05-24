from ckanapi import RemoteCKAN
import multiprocessing as mp
from pathlib import Path
import urllib
import yaml

from .constants import *
from .format import format_float
from .http import download_until_success
from .postgis import tables as pgistables
from .io.dirs import make_folder
from .standardise import reformat_dataset_name

LOG = mp.get_logger()


def process_custom_config(custom_config: str | Path):
    """
    Processes custom configuration value
    """
    global OSM_MAIN_DOWNLOAD, HEIGHT_TO_TIP, BLADE_RADIUS

    custom_config = Path(custom_config)

    make_folder(CUSTOM_CONFIGURATION_FOLDER)

    config_downloaded = False
    config_basename = custom_config.name.lower()
    config_saved_path = Path(CUSTOM_CONFIGURATION_FOLDER) / (
        config_basename.replace(YAML_EXT, "") + YAML_EXT
    )
    if config_saved_path.is_file():
        os.remove(config_saved_path)

    # If '.yml' isn't ending of customconfig, can only be a custom configuration reference on CKAN

    # Open Wind Energy CKAN requires special user-agent for downloads as protection against data crawlers
    opener = urllib.request.build_opener()
    opener.addheaders = [("User-Agent", CKAN_USER_AGENT)]
    urllib.request.install_opener(opener)

    if not config_saved_path.suffix != YAML_EXT:

        LOG.info(
            f"Custom configuration: Attempting to locate '{config_basename}' on {CKAN_URL}"
        )

        ckan = RemoteCKAN(CKAN_URL, user_agent=CKAN_USER_AGENT)
        packages = ckan.action.package_list(id="data-explorer")
        config_code = reformat_dataset_name(config_basename)

        for package in packages:
            ckan_package = ckan.action.package_show(id=package)

            # Check to see if name of customconfig matches CKAN reformatted package title
            if reformat_dataset_name(ckan_package["title"].strip()) != config_code:
                continue

            # If matches, search for YML file in resources
            for resource in ckan_package["resources"]:
                if "YML" in resource["format"]:
                    download_until_success(resource["url"], config_saved_path)
                    config_downloaded = True
                    break

            if config_downloaded:
                break

    elif custom_config.startswith("http://") or custom_config.startswith("https://"):
        download_until_success(custom_config, config_saved_path)
        config_downloaded = True

    # Revert user-agent to defaults
    opener = urllib.request.build_opener()
    urllib.request.install_opener(opener)

    if not config_downloaded:
        if custom_config.is_file():
            shutil.copy(custom_config, config_saved_path)
            config_downloaded = True

    if not config_downloaded:

        LOG.info(f"Unable to access custom configuration '{custom_config}'")
        LOG.info(" --> IGNORING CUSTOM CONFIGURATION")

        return None

    yaml_content = None
    with open(config_saved_path) as stream:
        try:
            yaml_content = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            LOG.error(exc)
            raise exc

    if yaml_content is not None:

        yaml_content["configuration"] = custom_config

        # Dropping all custom configuration tables
        # If we don't do this, things gets very complicated if you start running things across many config files

        LOG.info(
            "Custom configuration: Note all generated tables for custom configuration will have '"
            + CUSTOM_CONFIGURATION_TABLE_PREFIX
            + "' prefix"
        )

        LOG.info(
            "Custom configuration: Dropping previous custom configuration database tables"
        )

        pgistables.drop_custom_tables()

    if "osm" in yaml_content:
        OSM_MAIN_DOWNLOAD = yaml_content["osm"]
        LOG.info("Custom configuration: Setting OSM download to " + yaml_content["osm"])

    if "tip-height" in yaml_content:
        height_to_tip_str = format_float(yaml_content["tip-height"])
        HEIGHT_TO_TIP = float(height_to_tip_str)
        LOG.info(f"Custom configuration: Setting tip-height to {height_to_tip_str}")

    if "blade-radius" in yaml_content:
        blade_radius_str = format_float(yaml_content["blade-radius"])
        BLADE_RADIUS = float(blade_radius_str)
        LOG.info(f"Custom configuration: Setting blade-radius to {blade_radius_str}")

    if "clipping" in yaml_content:
        LOG.info(
            "Custom configuration: Clipping area(s) ["
            + ", ".join(yaml_content["clipping"])
            + "]"
        )

    if "areas" in yaml_content:
        LOG.info(
            "Custom configuration: Selecting specific area(s) ["
            + ", ".join(yaml_content["areas"])
            + "]"
        )

    return yaml_content
