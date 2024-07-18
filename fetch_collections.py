import asyncio

import pystac
from copernicusmarine.catalogue_parser import catalogue_parser

loop = asyncio.get_event_loop()
# don't use orjson when saving items
pystac.stac_io.orjson = None


async def fetch_catalog(staging=False):
    conn = catalogue_parser.CatalogParserConnection()
    children = await catalogue_parser.async_fetch_catalog(conn, staging=staging)
    await conn.close()

    return children


children = list(loop.run_until_complete(fetch_catalog(staging=False)))


def combine_collections(children):
    for col, items in children:
        new_col = col.clone()

        new_col.remove_links(pystac.RelType.SELF)
        new_col.remove_links(pystac.RelType.ROOT)
        new_col.remove_links(pystac.RelType.PARENT)
        new_col.remove_links(pystac.RelType.ITEM)

        new_col.add_items(items)
        yield new_col


cat = pystac.Catalog(
    id="MDS",
    catalog_type=pystac.CatalogType.SELF_CONTAINED,
    title="Copernicus Marine Data Store",
    description="Data from the Copernicus Marine Data Store, in Analysis-Ready, Cloud-Optimised (ARCO) format.",
)
cat.add_children(combine_collections(children))
cat.normalize_and_save(root_href="MDS")
