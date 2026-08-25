import unittest

import pandas as pd
from fastapi import HTTPException

from app.services.csv_service import _load_volvo_sale_dataframe


class VolvoSaleCsvTests(unittest.TestCase):
    headers_13 = [
        "calendar",
        "region",
        "market",
        "country",
        "machine",
        "machine_line",
        "size_class",
        "brand_owner_code",
        "brand_owner",
        "brand",
        "brand_nationality",
        "source",
        "fid",
    ]
    data_13 = [
        "2024",
        "Region Asia",
        "MA Asia East",
        "Japan",
        "CEX",
        "Compact Excavators",
        "Midi",
        "VCE",
        "Volvo CE",
        "Volvo",
        "Sweden",
        "SAL",
        "2",
    ]

    def test_maps_optional_country_code_by_header(self):
        headers = self.headers_13[:4] + ["country code"] + self.headers_13[4:]
        data = self.data_13[:4] + ["JP"] + self.data_13[4:]

        result = _load_volvo_sale_dataframe(pd.DataFrame([headers, data]))

        self.assertEqual(result.iloc[0]["machine"], "CEX")
        self.assertEqual(result.iloc[0]["source"], "SAL")
        self.assertEqual(result.iloc[0]["fid"], "2")

    def test_preserves_original_13_column_layout(self):
        result = _load_volvo_sale_dataframe(
            pd.DataFrame([self.headers_13, self.data_13])
        )

        self.assertEqual(result.iloc[0]["source"], "SAL")
        self.assertEqual(result.iloc[0]["fid"], "2")

    def test_rejects_non_numeric_fid(self):
        invalid_data = self.data_13[:-1] + ["SAL"]

        with self.assertRaises(HTTPException) as context:
            _load_volvo_sale_dataframe(
                pd.DataFrame([self.headers_13, invalid_data])
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("FID must be numeric", context.exception.detail)


if __name__ == "__main__":
    unittest.main()
