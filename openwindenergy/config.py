from ckanapi import RemoteCKAN
import multiprocessing as mp
from pathlib import Path
import urllib

from .constants import *
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
                    attemptDownloadUntilSuccess(resource["url"], config_saved_path)
                    config_downloaded = True
                    break

            if config_downloaded:
                break

    elif customconfig.startswith("http://") or customconfig.startswith("https://"):
        attemptDownloadUntilSuccess(customconfig, config_saved_path)
        config_downloaded = True

    # Revert user-agent to defaults
    opener = urllib.request.build_opener()
    urllib.request.install_opener(opener)

    if not config_downloaded:
        if isfile(customconfig):
            shutil.copy(customconfig, config_saved_path)
            config_downloaded = True

    if not config_downloaded:

        LOG.info("Unable to access custom configuration '" + customconfig + "'")
        LOG.info(" --> IGNORING CUSTOM CONFIGURATION")

        return None

    yaml_content = None
    with open(config_saved_path) as stream:
        try:
            yaml_content = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            LogFatalError(exc)

    if yaml_content is not None:

        yaml_content["configuration"] = customconfig

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

        postgisDropCustomTables()

    if "osm" in yaml_content:
        OSM_MAIN_DOWNLOAD = yaml_content["osm"]
        LOG.info("Custom configuration: Setting OSM download to " + yaml_content["osm"])

    if "tip-height" in yaml_content:
        HEIGHT_TO_TIP = float(formatValue(yaml_content["tip-height"]))
        LOG.info("Custom configuration: Setting tip-height to " + str(HEIGHT_TO_TIP))

    if "blade-radius" in yaml_content:
        BLADE_RADIUS = float(formatValue(yaml_content["blade-radius"]))
        LOG.info("Custom configuration: Setting blade-radius to " + str(BLADE_RADIUS))

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
