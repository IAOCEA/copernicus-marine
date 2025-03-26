import asyncio

import pystac
from cmems_stac.conventions import FormatError, ParserError, parse_collection_id
from copernicusmarine.catalogue_parser import catalogue_parser
from rich.console import Console

console = Console()


async def fetch_catalog(staging=False):
    conn = catalogue_parser.CatalogParserConnection()
    children = await catalogue_parser.async_fetch_catalog(conn, staging=staging)
    await conn.close()

    return children


children = list(asyncio.run(fetch_catalog(staging=False)))


def fix_item(item):
    variables = item.properties.get("cube:variables")
    if variables is None:
        return item

    for var in variables.values():
        var.pop("missingValue", None)

    return item


def fix_collection(col):
    try:
        info = parse_collection_id(col.id)
        col.extra_fields.update(info.to_stac())
    except ParserError:
        console.log(f"unknown collection id format: {col.id}")
    except FormatError:
        pass
        # console.log(f"could not extract stac properties: {col.id}")

    return col


def combine_collections(children):
    for col, items in children:
        new_col = fix_collection(col.clone())

        new_col.remove_links(pystac.RelType.SELF)
        new_col.remove_links(pystac.RelType.ROOT)
        new_col.remove_links(pystac.RelType.PARENT)
        new_col.remove_links(pystac.RelType.ITEM)

        new_col.add_items([fix_item(item) for item in items])
        yield new_col


cat = pystac.Catalog(
    id="MDS",
    catalog_type=pystac.CatalogType.SELF_CONTAINED,
    title="Copernicus Marine Data Store",
    description=(
        "Data from the Copernicus Marine Data Store, in Analysis-Ready, "
        "Cloud-Optimised (ARCO) format."
    ),
)
cat.add_children(combine_collections(children))
cat.normalize_and_save(root_href="MDS")
