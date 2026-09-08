from __future__ import annotations

from collections.abc import Iterable
from functools import lru_cache
from pathlib import Path
from typing import Any


@lru_cache(maxsize=1)
def require_nomad_forces_export() -> tuple[Any, Any]:
    """Load optional nomad-forces-export dependency on demand."""
    try:
        from nomad_forces_export import atoms_generator, write_atoms
    except ImportError as e:
        raise ImportError(
            'nomad-forces-export is required. Install with: '
            'pip install nomad-ml-workflows[atoms-export]'
        ) from e
    return atoms_generator, write_atoms


def generate_atoms_from_archives(
    archives: Iterable[dict], properties: list[str]
) -> list:
    atoms_generator, _ = require_nomad_forces_export()
    return [atom for atom in atoms_generator(archives, properties=set(properties))]


def write_atoms_to_file(
    atoms: list, output_file_path: str | Path, output_format: str = 'extxyz'
) -> None:
    _, write_atoms = require_nomad_forces_export()
    write_atoms(atoms, output_path=output_file_path, output_format=output_format)
