from pathlib import Path
from typing import List, Union

from beam.v1.exceptions import BeamRequirementsFileNotFound

MIN_MEM_GI = 1
MAX_MEM_GI = 64
MIN_MEM_MI = 128
MAX_MEM_MI = 999
MAX_CPU_CORES = 32


def between_or_equal(value: int, lower: int, upper: int) -> bool:
    return lower <= value <= upper


def parse_cpu(cpu: str) -> float:
    # CPU is sent to operator as '(CPU_CORES * 1000)m'
    # See compose_cpu function
    return int(cpu[:-1]) / 1000


def compose_cpu(cpu: Union[int, str, float]) -> str:
    if isinstance(cpu, str):
        cpu = parse_cpu(cpu)

    if cpu > MAX_CPU_CORES or cpu < 0.1:
        raise ValueError(f"CPU value is invalid: value must be between 0.1 and {MAX_CPU_CORES}")

    return f"{int(cpu * 1000)}m"


def parse_memory(memory: str) -> str:
    suffix = memory[-2:]

    if suffix not in ["Gi", "Mi"]:
        raise ValueError("Memory must end with Gi or Mi")

    return memory


def compose_memory(memory: str) -> str:
    memory = memory.strip()
    suffix = memory[-2:]
    number = int(memory[:-2].strip())

    if suffix.lower() not in ["gi", "mi"]:
        raise ValueError("Memory must end with Gi or Mi")

    if suffix.lower() == "gi" and not between_or_equal(number, MIN_MEM_GI, MAX_MEM_GI):
        raise ValueError(
            f"Memory value is invalid for Gi: value must be between {MIN_MEM_GI} and {MAX_MEM_GI}"
        )

    if suffix.lower() == "mi" and not between_or_equal(number, MIN_MEM_MI, MAX_MEM_MI):
        raise ValueError(
            f"Memory value is invalid for Mi: value must be between {MIN_MEM_MI} and {MAX_MEM_MI}"
        )

    return str(number) + suffix.capitalize()


def load_requirements_file(path: str) -> List[str]:
    requirements_file = Path(path)

    if requirements_file.is_file():
        with open(requirements_file, "r") as f:
            contents = f.read()
            lines = contents.split("\n")
            lines = list(filter(lambda r: r != "", lines))
            return lines
    else:
        raise BeamRequirementsFileNotFound
