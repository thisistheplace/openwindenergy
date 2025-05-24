from .constants import *


def normalizeTitle(title):
    """
    Converts local variants to use same name
    eg. Areas of Special Scientific Interest -> Sites of Special Scientific Interest
    """
    title_mapping = {
        "Areas of Special Scientific Interest": "Sites of Special Scientific Interest",
        "Conservation Area Boundaries": "Conservation Areas",
        "Scheduled Historic Monument Areas": "Scheduled Ancient Monuments",
        "Priority Habitats - Woodland": "Ancient woodlands",
        "National Scenic Areas (equiv to AONB)": "Areas of Outstanding Natural Beauty",
    }
    for old, new in title_mapping:
        title = title.replace(old, new)

    return title


def removeCustomConfigurationTablePrefix(layername):
    """
    Remove CUSTOM_CONFIGURATION_TABLE_PREFIX if set
    """

    custom_config_table_style = CUSTOM_CONFIGURATION_TABLE_PREFIX.replace("-", "_")
    custom_config_dataset_style = CUSTOM_CONFIGURATION_TABLE_PREFIX.replace("_", "-")

    if layername.startswith(custom_config_table_style):
        layername = layername[len(custom_config_table_style) :]
    elif layername.startswith(custom_config_dataset_style):
        layername = layername[len(custom_config_dataset_style) :]

    return layername


def removeCustomConfigurationFilePrefix(layername):
    """
    Remove CUSTOM_CONFIGURATION_FILE_PREFIX if set
    """

    custom_configuration_prefix_table_style = CUSTOM_CONFIGURATION_FILE_PREFIX.replace(
        "-", "_"
    )
    custom_configuration_prefix_dataset_style = (
        CUSTOM_CONFIGURATION_FILE_PREFIX.replace("_", "-")
    )

    if layername.startswith(custom_configuration_prefix_table_style):
        layername = layername[len(custom_configuration_prefix_table_style) :]
    elif layername.startswith(custom_configuration_prefix_dataset_style):
        layername = layername[len(custom_configuration_prefix_dataset_style) :]

    return layername


def reformat_dataset_name(datasettitle: str) -> str:
    """
    Reformats dataset title for compatibility purposes

    - Removes .geojson or .gpkg file extension
    - Replaces spaces with hyphen
    - Replaces ' - ' with double hyphen
    - Replaces _ with hyphen
    - Standardises local variations in dataset names, eg. 'Areas of Special Scientific Interest' (Northern Ireland) -> 'Sites of Special Scientific Interest'
    - For specific very long dataset names, eg. 'Public roads, A and B roads and motorways', shorten as this breaks PostGIS when adding prefixes/suffixes
    - Remove CUSTOM_CONFIGURATION_TABLE_PREFIX and CUSTOM_CONFIGURATION_FILE_PREFIX
    """

    datasettitle = normalizeTitle(datasettitle)
    datasettitle = datasettitle.replace(".geojson", "").replace(".gpkg", "")
    datasettitle = removeCustomConfigurationTablePrefix(datasettitle)
    datasettitle = removeCustomConfigurationFilePrefix(datasettitle)
    reformat_mapping = {
        " - ": "--",
        " ": "-",
        "_": "-",
        "(": "",
        ")": "",
        "public-roads-a-and-b-roads-and-motorways": "public-roads-a-b-motorways",
        "areas-of-special-scientific-interest": "sites-of-special-scientific-interest",
        "conservation-area-boundaries": "conservation-areas",
        "scheduled-historic-monument-areas": "scheduled-ancient-monuments",
        "priority-habitats--woodland": "ancient-woodlands",
        "local-wildlife-reserves": "local-nature-reserves",
        "national-scenic-areas-equiv-to-aonb": "areas-of-outstanding-natural-beauty",
        "explosive-safeguarded-areas,-danger-areas-near-ranges": "danger-areas",
        "separation-distance-to-residential-properties": "separation-distance-from-residential",
    }
    for old, new in reformat_mapping:
        datasettitle = datasettitle.replace(old, new)

    return datasettitle
