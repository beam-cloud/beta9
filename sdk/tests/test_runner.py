import unittest

from beta9.abstractions.base.runner import RunnerAbstraction


class TestRunner(unittest.TestCase):
    def setUp(self):
        self.runner = RunnerAbstraction()

    def test_cpu_parsing_float_input_within_range(self):
        self.assertEqual(self.runner._parse_cpu_to_millicores(0.5), 500)
        self.assertEqual(self.runner._parse_cpu_to_millicores(2.0), 2000)

    def test_cpu_parsing_integer_input_within_range(self):
        self.assertEqual(self.runner._parse_cpu_to_millicores(1), 1000)
        self.assertEqual(self.runner._parse_cpu_to_millicores(2), 2000)

    def test_cpu_parsing_string_input_within_range(self):
        self.assertEqual(self.runner._parse_cpu_to_millicores("1000m"), 1000)
        self.assertEqual(self.runner._parse_cpu_to_millicores("2000m"), 2000)

    def test_cpu_parsing_float_input_out_of_range(self):
        with self.assertRaises(ValueError):
            self.runner._parse_cpu_to_millicores(0.05)

        with self.assertRaises(ValueError):
            self.runner._parse_cpu_to_millicores(70.0)

    def test_cpu_parsing_string_input_out_of_range(self):
        with self.assertRaises(ValueError):
            self.runner._parse_cpu_to_millicores("50m")

        with self.assertRaises(ValueError):
            self.runner._parse_cpu_to_millicores("65000m")

    def test_cpu_parsing_invalid_string_format(self):
        with self.assertRaises(ValueError):
            self.runner._parse_cpu_to_millicores("100")

        with self.assertRaises(ValueError):
            self.runner._parse_cpu_to_millicores("abc")

        with self.assertRaises(ValueError):
            self.runner._parse_cpu_to_millicores("100mc")

    def test_cpu_parsing_non_numeric_input(self):
        with self.assertRaises(TypeError):
            self.runner._parse_cpu_to_millicores([])  # type:ignore[reportArgumentType]

        with self.assertRaises(TypeError):
            self.runner._parse_cpu_to_millicores({})  # type:ignore[reportArgumentType]
