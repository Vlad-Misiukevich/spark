import unittest

from pyspark.sql import Column

from src.main.python.etl import generate_hash_by_lat_lng, get_latitude


class TestEtlFunctions(unittest.TestCase):
    def test_type_hash_generating(self):
        generated_hash = generate_hash_by_lat_lng("39.244493", "-119.936437")
        expected_type = Column
        self.assertEqual(type(generated_hash), expected_type)

    def test_type_get_latitude(self):
        generated_latitude = get_latitude("US", "Brandywine", "16101 Crain Hwy")
        expected_type = Column
        self.assertEqual(type(generated_latitude), expected_type)

    def test_type_get_longitude(self):
        generated_longitude = get_latitude("US", "Brandywine", "16101 Crain Hwy")
        expected_type = Column
        self.assertEqual(type(generated_longitude), expected_type)


if __name__ == '__main__':
    unittest.main()
