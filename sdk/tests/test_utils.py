import shutil
import sys
from pathlib import Path
from unittest import TestCase

import pytest

from ..src.beta9.utils import get_class_name, get_init_args_kwargs, load_module_spec


class TestUtils(TestCase):
    ### Test get_init_args_kwargs ###
    def test_get_init_args_kwargs(self):
        class ExampleClass:
            def __init__(self, a, b, c=42, d="default"):
                pass

        result = get_init_args_kwargs(ExampleClass)
        self.assertEqual(result, {"a", "b", "c", "d"})

    def test_get_init_args_kwargs_no_args(self):
        class EmptyInit:
            def __init__(self):
                pass

        result = get_init_args_kwargs(EmptyInit)
        self.assertEqual(result, set())

    def test_get_init_args_kwargs_only_kwargs(self):
        class KwargsOnly:
            def __init__(self, x=100, y="test"):
                pass

        result = get_init_args_kwargs(KwargsOnly)
        self.assertEqual(result, {"x", "y"})

    ### Test get_class_name ###
    def test_get_class_name(self):
        class SampleClass:
            pass

        instance = SampleClass()
        self.assertEqual(get_class_name(instance), "SampleClass")

    ### Test load_module_spec ###
    def setUp(self):
        """Setup a temporary module for testing load_module_spec"""
        self.temp_dir = "./temp_dir"
        Path(self.temp_dir).mkdir(exist_ok=True)
        self.module_path = Path(self.temp_dir) / "fake_module.py"

        module_content = """
def my_function():
    return "Hello, World!"
"""
        self.module_path.write_text(module_content)

        # Add temporary directory to sys.path
        sys.path.insert(0, self.temp_dir)

    def tearDown(self):
        """Cleanup the temporary module and sys.path"""
        shutil.rmtree(self.temp_dir)
        sys.path.remove(self.temp_dir)

    def test_load_module_spec_success(self):
        module_name = self.module_path
        function_name = "my_function"

        module_spec, mod_name, obj_name = load_module_spec(
            f"{self.module_path}:{function_name}", "test"
        )

        self.assertEqual(mod_name, str(module_name).replace(".py", "").replace("/", "."))
        self.assertEqual(obj_name, function_name)
        self.assertEqual(module_spec(), "Hello, World!")

    def test_load_module_spec_file_not_found(self):
        with pytest.raises(SystemExit):
            result = load_module_spec("non_existent.py:function_name", "test")
            self.assertIsNone(result)

    def test_load_module_spec_function_not_found(self):
        with pytest.raises(SystemExit):
            result = load_module_spec(f"{self.module_path}:non_existent_function", "test")
            self.assertIsNone(result)
