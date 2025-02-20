import unittest

from beta9.abstractions.base.runner import RunnerAbstraction


class TestRunner(unittest.TestCase):
    def setUp(self):
        self.runner = RunnerAbstraction()

    def test_cpu_parsing_float_input_within_range(self):
        self.assertEqual(self.runner.parse_cpu(0.5), 500)
        self.assertEqual(self.runner.parse_cpu(2.0), 2000)

    def test_cpu_parsing_integer_input_within_range(self):
        self.assertEqual(self.runner.parse_cpu(1), 1000)
        self.assertEqual(self.runner.parse_cpu(2), 2000)

    def test_cpu_parsing_string_input_within_range(self):
        self.assertEqual(self.runner.parse_cpu("1000m"), 1000)
        self.assertEqual(self.runner.parse_cpu("2000m"), 2000)

    def test_cpu_parsing_float_input_out_of_range(self):
        with self.assertRaises(ValueError):
            self.runner.parse_cpu(0.05)

        with self.assertRaises(ValueError):
            self.runner.parse_cpu(70.0)

    def test_cpu_parsing_string_input_out_of_range(self):
        with self.assertRaises(ValueError):
            self.runner.parse_cpu("50m")

        with self.assertRaises(ValueError):
            self.runner.parse_cpu("65000m")

    def test_cpu_parsing_invalid_string_format(self):
        with self.assertRaises(ValueError):
            self.runner.parse_cpu("100")

        with self.assertRaises(ValueError):
            self.runner.parse_cpu("abc")

        with self.assertRaises(ValueError):
            self.runner.parse_cpu("100mc")

    def test_cpu_parsing_non_numeric_input(self):
        with self.assertRaises(TypeError):
            self.runner.parse_cpu([])  # type:ignore[reportArgumentType]

        with self.assertRaises(TypeError):
            self.runner.parse_cpu({})  # type:ignore[reportArgumentType]
