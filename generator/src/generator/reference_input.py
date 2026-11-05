"""Read the UCI Online Retail layout without a spreadsheet dependency."""

import csv
import io
import posixpath
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path


COLUMNS = ("InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country")
NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def _xlsx_rows(stream):
    with zipfile.ZipFile(stream) as workbook:
        metadata = ET.fromstring(workbook.read("xl/workbook.xml"))
        properties = metadata.find("s:workbookPr", NS)
        if properties is not None and properties.get("date1904") in {"1", "true"}:
            raise ValueError("UCI adapter requires the Excel 1900 date system")
        sheets = metadata.findall("s:sheets/s:sheet", NS)
        if len(sheets) != 1:
            raise ValueError("UCI adapter requires exactly one worksheet")
        relation_id = sheets[0].get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        relationships = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
        target = next(item.get("Target") for item in relationships if item.get("Id") == relation_id)
        sheet = target.lstrip("/") if target.startswith("/") else posixpath.normpath("xl/" + target)
        strings = []
        if "xl/sharedStrings.xml" in workbook.namelist():
            with workbook.open("xl/sharedStrings.xml") as source:
                for _, element in ET.iterparse(source, events=("end",)):
                    if element.tag == f"{{{NS['s']}}}si":
                        strings.append("".join(part.text or "" for part in element.findall(".//s:t", NS)))
                        element.clear()
        with workbook.open(sheet) as source:
            for _, row in ET.iterparse(source, events=("end",)):
                if row.tag != f"{{{NS['s']}}}row":
                    continue
                values = [""] * len(COLUMNS)
                for cell in row.findall("s:c", NS):
                    coordinate = cell.get("r", "")
                    column = "".join(c for c in coordinate if c.isalpha())
                    if column not in "ABCDEFGH" or len(column) != 1:
                        raise ValueError(f"Unexpected UCI column: {coordinate}")
                    value = cell.find("s:v", NS)
                    text = value.text if value is not None else ""
                    if cell.get("t") == "s":
                        text = strings[int(text)]
                    elif cell.get("t") == "inlineStr":
                        text = "".join(part.text or "" for part in cell.findall(".//s:t", NS))
                    elif column == "E" and text and cell.get("t") not in {"str", "d"}:
                        seconds = round(Decimal(text) * 86400)
                        text = (datetime(1899, 12, 30) + timedelta(seconds=seconds)).isoformat()
                    values[ord(column) - ord("A")] = text or ""
                yield values
                row.clear()


def read_reference_rows(path: Path):
    """Yield the eight source columns; XLSX dates become naive ISO timestamps."""
    with path.open("rb") as stream:
        if path.suffix.lower() == ".csv":
            with io.TextIOWrapper(stream, encoding="utf-8-sig", newline="") as text_stream:
                yield from _checked_rows(csv.reader(text_stream))
        elif path.suffix.lower() == ".xlsx":
            yield from _checked_rows(_xlsx_rows(stream))
        elif path.suffix.lower() == ".zip":
            with zipfile.ZipFile(stream) as archive:
                members = [name for name in archive.namelist() if name.lower().endswith(".xlsx")]
                if len(members) != 1:
                    raise ValueError("Reference ZIP must contain exactly one XLSX")
                with archive.open(members[0]) as member:
                    yield from _checked_rows(_xlsx_rows(io.BytesIO(member.read())))
        else:
            raise ValueError("Reference input must be .zip, .xlsx or UTF-8 .csv")


def _checked_rows(rows):
    header = next(rows, None)
    if tuple(header or ()) != COLUMNS:
        raise ValueError(f"Reference header must be {COLUMNS}")
    for row in rows:
        if len(row) != len(COLUMNS):
            raise ValueError("Reference row must contain exactly eight columns")
        yield dict(zip(COLUMNS, row))
