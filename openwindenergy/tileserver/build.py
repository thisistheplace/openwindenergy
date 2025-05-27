import multiprocessing as mp
import json

from .fonts import install_fonts
from ..constants import *
from ..format import format_float
from ..postgis.tables import (
    get_table_bounds,
    reformat_table_name,
    reformat_table_name_absolute,
    build_union_table_name,
    get_final_layer_latest_name,
    create_grid_clipped_file,
)
from ..standardise import reformat_dataset_name
from ..system.files import load_json
from ..system.dirs import make_folder, list_files
from ..system.process import run_subprocess
from ..workflow.downloads import download_osm_data

LOG = mp.get_logger()


def build_files():
    """
    Builds files required for tileserver-gl
    """

    global CUSTOM_CONFIGURATION, CUSTOM_CONFIGURATION_FILE_PREFIX, LATEST_OUTPUT_FILE_PREFIX
    global OVERALL_CLIPPING_FILE, TILESERVER_URL, TILESERVER_FONTS_GITHUB, TILESERVER_SRC_FOLDER, TILESERVER_FOLDER, TILESERVER_DATA_FOLDER, TILESERVER_STYLES_FOLDER, OSM_DOWNLOADS_FOLDER, OSM_MAIN_DOWNLOAD, BUILD_FOLDER, FINALLAYERS_OUTPUT_FOLDER, FINALLAYERS_CONSOLIDATED, MAPAPP_FOLDER
    global TILEMAKER_COASTLINE_CONFIG, TILEMAKER_COASTLINE_PROCESS, TILEMAKER_OMT_CONFIG, TILEMAKER_OMT_PROCESS, SKIP_FONTS_INSTALLATION, OPENMAPTILES_HOSTED_FONTS

    # Run tileserver build process

    LOG.info("Creating tileserver files")

    make_folder(TILESERVER_FOLDER)
    make_folder(TILESERVER_DATA_FOLDER)
    make_folder(TILESERVER_STYLES_FOLDER)

    # Legacy issue: housekeeping of final output and tileserver folders due to shortening of
    # specific dataset names leaving old files with old names that cause problems
    # Also general shortening of output filenames to allow for blade radius information

    legacy_delete_items = [
        "tipheight-",
        "public-roads-a-and-b-roads-and-motorways",
        "openwind.json",
    ]
    for legacy_delete_item in legacy_delete_items:
        for fpath in list_files(FINALLAYERS_OUTPUT_FOLDER):
            if legacy_delete_item in fpath.name:
                (FINALLAYERS_OUTPUT_FOLDER / fpath.name).unlink()
        for fpath in list_files(TILESERVER_DATA_FOLDER):
            if legacy_delete_item in fpath.name:
                (TILESERVER_DATA_FOLDER / fpath.name).unlink()
        for fpath in list_files(TILESERVER_STYLES_FOLDER):
            if legacy_delete_item in fpath.name:
                (TILESERVER_STYLES_FOLDER / fpath.name).unlink()

    # Copy 'sprites' folder

    if not (TILESERVER_FOLDER / "sprites").is_dir():
        shutil.copytree(
            TILESERVER_SRC_FOLDER / "sprites", TILESERVER_FOLDER / "sprites"
        )

    # Copy index.html

    shutil.copy(TILESERVER_SRC_FOLDER / "index.html", MAPAPP_FOLDER / "index.html")

    # Modify 'openmaptiles.json' and export to tileserver folder

    openmaptiles_style_file_src = TILESERVER_SRC_FOLDER / "openmaptiles.json"
    openmaptiles_style_file_dst = TILESERVER_STYLES_FOLDER / "openmaptiles.json"
    openmaptiles_style_json = load_json(openmaptiles_style_file_src)
    openmaptiles_style_json["sources"]["openmaptiles"]["url"] = (
        TILESERVER_URL / "data" / "openmaptiles.json"
    )

    # Either use hosted version of fonts or install local fonts folder

    use_font_folder = False
    if SKIP_FONTS_INSTALLATION:
        fonts_url = OPENMAPTILES_HOSTED_FONTS
    else:
        if install_fonts():
            use_font_folder = True
            fonts_url = TILESERVER_URL / "fonts/{fontstack}/{range}.pbf"
        else:
            LOG.info("Attempt to build fonts failed, using hosted fonts instead")
            fonts_url = OPENMAPTILES_HOSTED_FONTS

    openmaptiles_style_json["glyphs"] = fonts_url

    with open(openmaptiles_style_file_dst, "w") as json_file:
        json.dump(openmaptiles_style_json, json_file, indent=4)

    attribution = (
        'Source data copyright of multiple organisations. For all data sources, see <a href="'
        + CKAN_URL
        + '" target="_blank">'
        + CKAN_URL.replace("https://", "")
        + "</a>"
    )
    openwind_style_file = TILESERVER_STYLES_FOLDER / "openwindenergy.json"
    openwind_style_json = openmaptiles_style_json
    openwind_style_json["name"] = "Open Wind Energy"
    openwind_style_json["id"] = "openwindenergy"
    openwind_style_json["sources"]["attribution"]["attribution"] += " " + attribution

    basemap_mbtiles = TILESERVER_DATA_FOLDER / OSM_MAIN_DOWNLOAD.replace(
        ".osm.pbf", ".mbtiles"
    )

    # Create basemap mbtiles

    if not basemap_mbtiles.is_file():

        download_osm_data()

        LOG.info("Creating basemap: " + basemap_mbtiles.name)

        LOG.info("Generating global coastline mbtiles...")

        bbox_entireworld = "-180,-85,180,85"
        bbox_unitedkingdom_padded = "-49.262695,38.548165,39.990234,64.848937"

        run_subprocess(
            [
                "tilemaker",
                "--input",
                OSM_DOWNLOADS_FOLDER / Path(OSM_MAIN_DOWNLOAD).name,
                "--output",
                basemap_mbtiles,
                "--bbox",
                bbox_unitedkingdom_padded,
                "--process",
                TILEMAKER_COASTLINE_PROCESS,
                "--config",
                TILEMAKER_COASTLINE_CONFIG,
            ]
        )

        LOG.info(
            "Merging "
            + Path(OSM_MAIN_DOWNLOAD).name
            + " into global coastline mbtiles..."
        )

        run_subprocess(
            [
                "tilemaker",
                "--input",
                OSM_DOWNLOADS_FOLDER + Path(OSM_MAIN_DOWNLOAD).name,
                "--output",
                basemap_mbtiles,
                "--merge",
                "--process",
                TILEMAKER_OMT_PROCESS,
                "--config",
                TILEMAKER_OMT_CONFIG,
            ]
        )

        LOG.info("Basemap mbtiles created: " + basemap_mbtiles.name)

    # Run tippecanoe regardless of whether existing mbtiles exist

    style_lookup = load_json(STYLE_LOOKUP)
    dataset_style_lookup = {}
    for style_item in style_lookup:
        dataset_id = style_item["dataset"]
        dataset_style_lookup[dataset_id] = {
            "title": style_item["title"],
            "color": style_item["color"],
            "level": style_item["level"],
            "defaultactive": style_item["defaultactive"],
        }
        if "children" in style_item:
            for child in style_item["children"]:
                child_dataset_id = child["dataset"]
                dataset_style_lookup[child_dataset_id] = {
                    "title": child["title"],
                    "color": child["color"],
                    "level": child["level"],
                    "defaultactive": child["defaultactive"],
                }

    # Get bounds of clipping area for use in tileserver-gl config file creation

    clipping_table = reformat_table_name(OVERALL_CLIPPING_FILE)
    clipping_union_table = build_union_table_name(clipping_table)
    clipping_bounds_dict = get_table_bounds(clipping_union_table)
    clipping_bounds = [
        clipping_bounds_dict["left"],
        clipping_bounds_dict["bottom"],
        clipping_bounds_dict["right"],
        clipping_bounds_dict["top"],
    ]

    output_files = list_files(FINALLAYERS_OUTPUT_FOLDER)
    styles, data = {}, {}
    styles["openwindenergy"] = {
        "style": "openwindenergy.json",
        "tilejson": {"type": "overlay", "bounds": clipping_bounds},
    }
    styles["openmaptiles"] = {
        "style": "openmaptiles.json",
        "tilejson": {"type": "overlay", "bounds": clipping_bounds},
    }
    data["openmaptiles"] = {"mbtiles": basemap_mbtiles.name}

    custom_configuration_file_prefix = ""
    if CUSTOM_CONFIGURATION is not None:
        custom_configuration_file_prefix = CUSTOM_CONFIGURATION_FILE_PREFIX

    # Insert overall constraints as first item in list so it appears as first item in tileserver-gl
    overallconstraints = (
        get_final_layer_latest_name(FINALLAYERS_CONSOLIDATED) + ".geojson"
    )

    if overallconstraints in output_files:
        output_files.remove(overallconstraints)
    if not (FINALLAYERS_OUTPUT_FOLDER / overallconstraints).is_file():
        msg = "Final overall constraints layer is missing"
        LOG.error(msg)
        raise RuntimeError(msg)

    # Set prefix for only those files we're interested in processing with Tippecanoe
    required_prefix = custom_configuration_file_prefix + LATEST_OUTPUT_FILE_PREFIX

    # Tippecanoe is used to create mbtiles for all 'latest--...' / 'custom--latest...' GeoJSONs

    output_files.insert(0, overallconstraints)
    for output_file in output_files:

        # Only process GeoJSONs with required_prefix
        if (not output_file.startswith(required_prefix)) or (
            not output_file.endswith(".geojson")
        ):
            continue

        # derived_dataset_name will begin with required_prefix, ie. 'latest--'
        # or 'custom--latest--' as we've specifically filtered on required_prefix
        derived_dataset_name = output_file.parent / output_file.stem

        # Don't process any datasets that are not in dataset_style_lookup (flat list of all used outputted datasets)
        if derived_dataset_name not in dataset_style_lookup:
            continue

        # original_table_name for all outputs will begin 'tip-...'
        # as we store pre-output geometries with these specific table names
        original_table_name = get_output_file_original_table(output_file)

        # core_dataset_name refers to essential dataset, eg. 'scheduled-ancient-monuments'
        # or 'ecology-and-wildlife', which is shared between non-custom and custom modes
        # and also across some early-stage and pre-output database tables.
        # For example:
        # derived_dataset_name = 'custom--latest--ecology-and-wildlife'
        # core_dataset_name = 'ecology-and-wildlife'
        core_dataset_name = core_dataset_name(derived_dataset_name)

        tippecanoe_output: Path = TILESERVER_DATA_FOLDER / output_file.replace(
            ".geojson", ".mbtiles"
        )

        style_id = derived_dataset_name
        style_name = dataset_style_lookup[derived_dataset_name]["title"]

        # If tippecanoe failed previously for any reason, delete the output and intermediary file

        tippecanoe_interrupted_file: Path = (
            tippecanoe_output.parent + tippecanoe_output.name + "-journal"
        )
        if tippecanoe_interrupted_file.is_file():
            tippecanoe_interrupted_file.unlink()
            if tippecanoe_output.is_file():
                tippecanoe_output.unlink()

        # Create grid-clipped version of GeoJSON to input into tippecanoe to improve mbtiles rendering and performance

        if not tippecanoe_output.is_file():

            LOG.info("Creating mbtiles for: " + output_file)

            tippecanoe_grid_clipped_file = Path(
                "tippecanoe--grid-clipped--temp.geojson"
            )

            if tippecanoe_grid_clipped_file.is_file():
                tippecanoe_grid_clipped_file.unlink()

            create_grid_clipped_file(
                original_table_name, core_dataset_name, tippecanoe_grid_clipped_file
            )

            # Check for no features as GeoJSON with no features causes problem for tippecanoe
            # If no features, add dummy point so Tippecanoe creates mbtiles

            if os.path.getsize(tippecanoe_grid_clipped_file) < 1000:
                with open(tippecanoe_grid_clipped_file, "r") as json_file:
                    geojson_content = json.load(json_file)
                if ("features" not in geojson_content) or (
                    len(geojson_content["features"]) == 0
                ):
                    geojson_content["features"] = [
                        {
                            "type": "Feature",
                            "properties": {},
                            "geometry": {"type": "Point", "coordinates": [0, 0]},
                        }
                    ]
                    with open(tippecanoe_grid_clipped_file, "w") as json_file:
                        json.dump(geojson_content, json_file)

            run_subprocess(
                [
                    "tippecanoe",
                    "-Z4",
                    "-z15",
                    "-X",
                    "--generate-ids",
                    "--force",
                    "-n",
                    style_name,
                    "-l",
                    derived_dataset_name,
                    tippecanoe_grid_clipped_file,
                    "-o",
                    tippecanoe_output,
                ]
            )

            if tippecanoe_grid_clipped_file.is_file():
                tippecanoe_grid_clipped_file.unlink()

        if not tippecanoe_output.is_file():
            msg = f"Failed to create mbtiles: {tippecanoe_output.name}"
            LOG.error(msg)
            raise RuntimeError(msg)

        LOG.info("Created tileserver-gl style file for: " + output_file)

        style_color = dataset_style_lookup[derived_dataset_name]["color"]
        style_level = dataset_style_lookup[derived_dataset_name]["level"]
        style_defaultactive = dataset_style_lookup[derived_dataset_name][
            "defaultactive"
        ]
        style_opacity = 0.8 if style_level == 1 else 0.5
        style_file: Path = TILESERVER_STYLES_FOLDER / (style_id + ".json")
        style_json = {
            "version": 8,
            "id": style_id,
            "name": style_name,
            "sources": {
                derived_dataset_name: {
                    "type": "vector",
                    "buffer": 512,
                    "url": TILESERVER_URL + "/data/" + style_id + ".json",
                    "attribution": attribution,
                }
            },
            "glyphs": fonts_url,
            "layers": [
                {
                    "id": style_id,
                    "source": style_id,
                    "source-layer": style_id,
                    "type": "fill",
                    "paint": {"fill-opacity": style_opacity, "fill-color": style_color},
                }
            ],
        }

        openwind_style_json["sources"][style_id] = style_json["sources"][
            derived_dataset_name
        ]
        with open(style_file, "w") as json_file:
            json.dump(style_json, json_file, indent=4)

        openwind_layer = style_json["layers"][0]
        # Temporary workaround as setting 'fill-outline-color'='#FFFFFF00' on individual style breaks WMTS
        openwind_layer["paint"]["fill-outline-color"] = "#FFFFFF00"
        if style_defaultactive:
            openwind_layer["layout"] = {"visibility": "visible"}
        else:
            openwind_layer["layout"] = {"visibility": "none"}

        # Hide overall constraint layer
        if core_dataset_name == FINALLAYERS_CONSOLIDATED:
            openwind_layer["layout"] = {"visibility": "none"}

        openwind_style_json["layers"].append(openwind_layer)

        styles[style_id] = {
            "style": style_file.name,
            "tilejson": {"type": "overlay", "bounds": clipping_bounds},
        }
        data[style_id] = {"mbtiles": tippecanoe_output.name}

    with open(openwind_style_file, "w") as json_file:
        json.dump(openwind_style_json, json_file, indent=4)

    # Creating final tileserver-gl config file

    config_file = TILESERVER_FOLDER + "config.json"
    if use_font_folder:
        config_json = {
            "options": {
                "paths": {
                    "root": "",
                    "fonts": "fonts",
                    "sprites": "sprites",
                    "styles": "styles",
                    "mbtiles": "data",
                }
            },
            "styles": styles,
            "data": data,
        }
    else:
        config_json = {
            "options": {
                "paths": {
                    "root": "",
                    "sprites": "sprites",
                    "styles": "styles",
                    "mbtiles": "data",
                }
            },
            "styles": styles,
            "data": data,
        }

    with open(config_file, "w") as json_file:
        json.dump(config_json, json_file, indent=4)

    LOG.info("All tileserver files created")


def get_output_file_original_table(output_file_path: Path | str):
    """
    Gets original table used to generate output file
    """
    output_file_path = Path(output_file_path)

    global HEIGHT_TO_TIP, CUSTOM_CONFIGURATION_FILE_PREFIX

    output_file_basename = output_file_path.parent / output_file_path.stem
    original_table_name = (
        reformat_table_name(output_file_basename)
        .replace("latest__", "")
        .replace(CUSTOM_CONFIGURATION_FILE_PREFIX.replace("-", "_"), "")
    )

    if "tip_" not in original_table_name:
        original_table_name = build_final_layer_table_name(original_table_name)

    return original_table_name


def build_final_layer_table_name(layername):
    """
    Builds final layer table name
    Test for whether layer is turbine-height dependent and if so incorporate HEIGHT_TO_TIP and BLADE_RADIUS parameters into name
    """

    dataset_parent = get_dataset_parent(layername)
    dataset_parent_no_custom = remove_custom_config_table_prefix(dataset_parent)

    if is_turbine_height_dependent(dataset_parent_no_custom):
        return reformat_table_name(
            build_turbine_parameters_prefix()
            + reformat_table_name_absolute(dataset_parent_no_custom)
        )
    return reformat_table_name(
        "tip_any__" + reformat_table_name_absolute(dataset_parent_no_custom)
    )


def get_dataset_parent(file_path: Path | str):
    """
    Gets dataset parent name from file path
    Parent = 'description', eg 'national-parks' in 'national-parks--scotland'
    """
    file_path = Path(file_path)
    file_basename = file_path.parent / file_path.stem
    return "--".join(file_basename.split("--")[0:1])


def remove_custom_config_table_prefix(layername):
    """
    Remove CUSTOM_CONFIGURATION_TABLE_PREFIX if set
    """

    global CUSTOM_CONFIGURATION_TABLE_PREFIX

    custom_configuration_prefix_table_style = CUSTOM_CONFIGURATION_TABLE_PREFIX.replace(
        "-", "_"
    )
    custom_configuration_prefix_dataset_style = (
        CUSTOM_CONFIGURATION_TABLE_PREFIX.replace("_", "-")
    )

    if layername.startswith(custom_configuration_prefix_table_style):
        layername = layername[len(custom_configuration_prefix_table_style) :]
    elif layername.startswith(custom_configuration_prefix_dataset_style):
        layername = layername[len(custom_configuration_prefix_dataset_style) :]

    return layername


def is_turbine_height_dependent(dataset_name):
    """
    Returns true or false, depending on whether dataset is turbine-height dependent
    """

    global FINALLAYERS_CONSOLIDATED

    structure_lookup = load_json(STRUCTURE_LOOKUP)
    dataset_name = reformat_dataset_name(dataset_name)

    # We assume overall layer is turbine-height dependent
    if dataset_name == FINALLAYERS_CONSOLIDATED:
        return True

    children_lookup = {}
    groups = list(structure_lookup.keys())
    for group in groups:
        group_children = list(structure_lookup[group].keys())
        children_lookup[group] = group_children
        for group_child in group_children:
            children_lookup[group_child] = structure_lookup[group][group_child]

    core_dataset_name = core_dataset_name(dataset_name)
    alldescendants = get_all_descendants(children_lookup, core_dataset_name)

    for descendant in alldescendants:
        if is_specific_dataset_height_dependent(descendant):
            return True
    return False


def build_turbine_parameters_prefix():
    """
    Builds turbine parameters prefix that is used in table names and output files
    """

    global HEIGHT_TO_TIP, BLADE_RADIUS

    return (
        "tip_"
        + format_float(HEIGHT_TO_TIP).replace(".", "_")
        + "m_bld_"
        + format_float(BLADE_RADIUS).replace(".", "_")
        + "m__"
    )


def core_dataset_name(file_path: Path | str):
    """
    Gets core dataset name from file path
    Core dataset = 'description--location', eg 'national-parks--scotland'
    Remove any 'custom--', 'latest--' or 'tip-..--' prefixes that may have been added to file name
    """
    file_path = Path(file_path)

    global CUSTOM_CONFIGURATION, CUSTOM_CONFIGURATION_FILE_PREFIX, LATEST_OUTPUT_FILE_PREFIX

    file_basename = file_path.stem

    if CUSTOM_CONFIGURATION is not None:
        if file_basename.startswith(CUSTOM_CONFIGURATION_FILE_PREFIX):
            file_basename = file_basename[len(CUSTOM_CONFIGURATION_FILE_PREFIX) :]

    if file_basename.startswith(LATEST_OUTPUT_FILE_PREFIX) or file_basename.startswith(
        "tip-"
    ):
        elements = file_basename.split("--")
        file_basename = "--".join(elements[1:])

    elements = file_basename.split("--")
    return "--".join(elements[0:2])


def get_all_descendants(children_lookup, dataset_name):
    """
    Gets all descendants of dataset
    """

    alldescendants = set()
    if dataset_name in children_lookup:
        for child in children_lookup[dataset_name]:
            alldescendants.add(child)
            descendants = get_all_descendants(children_lookup, child)
            for descendant in descendants:
                alldescendants.add(descendant)
        return list(alldescendants)
    else:
        return []


def is_specific_dataset_height_dependent(dataset_name: str) -> bool:
    """
    Returns true or false, depending on whether specific dataset (ignoring children) is turbine-height dependent
    """

    buffer_lookup = load_json(BUFFER_LOOKUP)
    if dataset_name in buffer_lookup:
        buffer_value = str(buffer_lookup[dataset_name])
        if "height-to-tip" in buffer_value:
            return True
        if "blade-radius" in buffer_value:
            return True
    return False
