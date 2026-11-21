import unittest
import uuid

from datagen.randomness import ONLINE, POS, REFERENCE, stream, uuid4_strings


def draw(rng):
    return rng.integers(0, 10**9, 100)


class RandomnessTests(unittest.TestCase):
    def test_stream_is_reproducible(self):
        self.assertTrue((draw(stream(7, ONLINE, 3)) == draw(stream(7, ONLINE, 3))).all())

    def test_streams_differ_by_batch_and_by_stream_id(self):
        self.assertFalse((draw(stream(7, ONLINE, 3)) == draw(stream(7, ONLINE, 4))).all())
        self.assertFalse((draw(stream(7, ONLINE, 3)) == draw(stream(7, POS, 3))).all())

    def test_uuid4_strings_are_valid_unique_and_seeded(self):
        values = uuid4_strings(stream(1, REFERENCE), 1000)
        self.assertEqual(values.n_unique(), 1000)
        self.assertTrue(all(uuid.UUID(value).version == 4 for value in values))
        self.assertEqual(values.to_list(), uuid4_strings(stream(1, REFERENCE), 1000).to_list())

    def test_uuid4_strings_handles_zero(self):
        self.assertEqual(len(uuid4_strings(stream(1, REFERENCE), 0)), 0)


if __name__ == "__main__":
    unittest.main()
