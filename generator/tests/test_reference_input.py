import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

from generator.reference_input import COLUMNS, read_reference_rows


def workbook_bytes(*, date1904=False):
    target = io.BytesIO()
    namespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    with zipfile.ZipFile(target, "w") as archive:
        archive.writestr(
            "xl/workbook.xml",
            f'<workbook xmlns="{namespace}" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><workbookPr date1904="{int(date1904)}"/><sheets><sheet name="retail" sheetId="1" r:id="rId1"/></sheets></workbook>',
        )
        archive.writestr(
            "xl/_rels/workbook.xml.rels",
            '<Relationships><Relationship Id="rId1" Target="worksheets/sheet1.xml"/></Relationships>',
        )
        archive.writestr(
            "xl/sharedStrings.xml", f'<sst xmlns="{namespace}"><si><r><t>Gi</t></r><r><t>ft</t></r></si></sst>'
        )
        header = "".join(
            f'<c r="{chr(65 + i)}1" t="inlineStr"><is><t>{escape(name)}</t></is></c>' for i, name in enumerate(COLUMNS)
        )
        body = '<c r="A2"><v>536365</v></c><c r="B2" t="inlineStr"><is><t>12345A</t></is></c><c r="C2" t="s"><v>0</v></c><c r="D2"><v>2</v></c><c r="E2"><v>40513.5</v></c><c r="F2"><v>1.25</v></c><c r="H2" t="inlineStr"><is><t>United Kingdom</t></is></c>'
        archive.writestr(
            "xl/worksheets/sheet1.xml",
            f'<worksheet xmlns="{namespace}"><sheetData><row r="1">{header}</row><row r="2">{body}</row></sheetData></worksheet>',
        )
    return target.getvalue()


class ReferenceInputTests(unittest.TestCase):
    def test_xlsx_and_official_style_zip_preserve_cells_and_dates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            xlsx = root / "source.xlsx"
            xlsx.write_bytes(workbook_bytes())
            zipped = root / "source.zip"
            with zipfile.ZipFile(zipped, "w") as archive:
                archive.writestr("Online Retail.xlsx", xlsx.read_bytes())
            for path in (xlsx, zipped):
                with self.subTest(path=path):
                    rows = list(read_reference_rows(path))
                    self.assertEqual(len(rows), 1)
                    self.assertEqual(rows[0]["Description"], "Gift")
                    self.assertEqual(rows[0]["CustomerID"], "")
                    self.assertEqual(rows[0]["StockCode"], "12345A")
                    self.assertEqual(rows[0]["InvoiceDate"], "2010-12-01T12:00:00")

    def test_other_excel_epoch_fails_explicitly(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.xlsx"
            path.write_bytes(workbook_bytes(date1904=True))
            with self.assertRaisesRegex(ValueError, "1900"):
                list(read_reference_rows(path))

    def test_incorrect_columns_fail_before_measurement(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.csv"
            path.write_text("Invoice,SKU,Quantity\n1,12345,2\n")
            with self.assertRaisesRegex(ValueError, "header"):
                list(read_reference_rows(path))


if __name__ == "__main__":
    unittest.main()
