import asyncio
from urllib.parse import urlsplit, urlunsplit

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
console.log("catalog downloaded")
int64_range = [-(2**63), 2**63]


def in_range(val, range_):
    lower, upper = range_

    if any(x is None for x in [val, lower, upper]):
        raise ValueError("invalid None found:", val, lower, upper)

    return val >= lower and val <= upper


def preprocess_asset_href(parts):
    endpoint_url = urlunsplit(parts._replace(path=""))
    url = f"s3://{parts.path.lstrip('/')}"

    return url, {"endpoint_url": endpoint_url, "anon": True}


def fix_item(item):
    variables = item.properties.get("cube:variables")
    if variables is None:
        return item

    for var in variables.values():
        if not in_range(var.get("missingValue", 0) or 0, int64_range):
            del var["missingValue"]

        if not in_range(var.get("valueMin", 0) or 0, int64_range):
            del var["valueMin"]
        if not in_range(var.get("valueMax", 0) or 0, int64_range):
            del var["valueMax"]

    for asset in item.assets.values():
        parts = urlsplit(asset.href)
        if not parts.netloc.startswith("s3."):
            continue

        asset.href, storage_options = preprocess_asset_href(parts)
        asset.extra_fields["xarray:storage_options"] = storage_options

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


def analyze(val):
    errors = []
    if isinstance(val, list):
        for el in val:
            try:
                analyze(el)
            except ExceptionGroup as e:
                errors.append(e)
    elif isinstance(val, dict):
        for k, v in val.items():
            try:
                analyze(v)
            except ExceptionGroup as e:
                e.add_note(f"failed in key: {k}")
                errors.append(e)
    else:
        try:
            orjson.dumps(val)
        except TypeError:
            errors.append(ValueError(f"failed to serialize value: {val!r}"))

    if errors:
        raise ExceptionGroup("analysis results", errors)


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
console.log("collections combined")
try:
    cat.normalize_and_save(root_href="MDS")
except TypeError:
    # check for orjson weirdness
    import orjson

    for col in cat.get_children():
        try:
            orjson.dumps(col.to_dict())
        except TypeError:
            try:
                analyze(col.to_dict())
            except Exception as e:
                e.add_note(f"failed to serialize {col.id}")
                raise

        for it in col.get_items():
            try:
                orjson.dumps(it.to_dict())
            except TypeError:
                try:
                    analyze(it.to_dict())
                except Exception as e:
                    e.add_note(f"failed to serialize the item {it.id} from {col.id}")
                    raise
