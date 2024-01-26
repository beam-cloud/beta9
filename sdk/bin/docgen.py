#!/usr/bin/env python

import inspect
import textwrap
from enum import Enum

from beta9 import (
    App,
    Autoscaling,
    AutoscalingType,
    GpuType,
    Image,
    Output,
    PythonVersion,
    QueueDepthAutoscaler,
    RequestLatencyAutoscaler,
    Run,
    Runtime,
    Volume,
    VolumeType,
)
from docstring_parser import parse

EXCLUDED_FUNCS = [
    "_",
    "data",
    "build_config",
]


def write_header() -> str:
    return textwrap.dedent(
        """
        ---
        title: "SDK Reference"
        icon: "code"
        ---
    """
    ).lstrip("\n")


def write_enum(obj, attrs: list, output: str) -> str:
    class_doc = parse(textwrap.dedent(obj.__doc__ or ""))
    if class_doc.short_description:
        output += f"\n{class_doc.short_description}\n\n"

    if class_doc.long_description:
        output += f"{class_doc.long_description}\n\n"

    if attrs:
        output += "#### Variables\n"
        output += "\n| Name | Type | Value |\n"
        output += "| --- | --- | --- |\n"

    for attr in attrs:
        if attr.name[0].isupper():
            value = attr.object.value.__repr__().replace("'", '"')
            sig = type(attr.object.value).__name__
            output += f"| `{attr.name}` | `{sig}` |  `{value}` |\n"

    if class_doc.examples:
        output += "\n#### Examples\n\n"
        for example in class_doc.examples:
            output += f"{example.description}\n"

    return output


def write_callable(obj, attrs: list, output: str) -> str:
    class_doc = parse(textwrap.dedent(obj.__doc__ or ""))
    if class_doc.short_description:
        output += f"\n{class_doc.short_description}\n"

    if class_doc.long_description:
        output += f"\n{class_doc.long_description}\n"

    for attr in attrs:
        if attr.name in EXCLUDED_FUNCS:
            continue

        if attr.name.startswith(tuple(EXCLUDED_FUNCS)) and not attr.name == "__init__":
            continue

        doc = parse(textwrap.dedent(attr.object.__doc__ or ""))

        if attr.kind == "method":
            f = f".{attr.name}" if attr.name != "__init__" else ""
            output += f"\n### `{obj.__name__}{f}()`\n"
        elif attr.kind == "data":
            continue

        if doc.short_description:
            output += f"\n{doc.short_description}\n\n"

        if doc.long_description:
            output += f"{doc.long_description}\n\n"

        if doc.params:
            output += "#### Parameters\n\n"
            output += "| Parameter | Required | Description |\n| --- | --- | --- |\n"

            attr_sig = inspect.signature(attr.object)

            for param in doc.params:
                param_sig = attr_sig.parameters.get(param.arg_name, None)
                param_required = (
                    True if param_sig and (param_sig.default == param_sig.empty) else False
                )
                param_desc = str(param.description or "").replace("\n", " ")
                output += "| `{}` | {} | {} |\n".format(
                    param.arg_name,
                    "Yes" if param_required else "No",
                    param_desc,
                )

        if doc.examples:
            output += "\n#### Examples\n\n"
            for example in doc.examples:
                output += f"{example.description}\n"

        output += "\n"
    return output


def main():
    object_attributes = {
        obj: inspect.classify_class_attrs(obj)
        for obj in [
            App,
            Runtime,
            Run,
            Image,
            Output,
            Volume,
            Autoscaling,
            RequestLatencyAutoscaler,
            QueueDepthAutoscaler,
            PythonVersion,
            GpuType,
            VolumeType,
            AutoscalingType,
        ]
    }

    output = write_header()

    for obj, attrs in object_attributes.items():
        output += f"\n## {obj.__name__}\n"

        if issubclass(obj, Enum):
            output = write_enum(obj, attrs, output)
            continue
        else:
            output = write_callable(obj, attrs, output)

    print(output)


if __name__ == "__main__":
    main()
