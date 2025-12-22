# src/pipeline/meta/meta.py
from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Dict

import pyarrow.parquet as pq

# src/pipeline/meta/schema.py
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


@dataclass
class InputMeta:
    path: str
    size: int
    hash: Optional[str] = None


@dataclass
class OutputMeta:
    path: str
    rows: Optional[int]
    size: Optional[int]
    hash: Optional[str] = None


@dataclass
class Manifest:
    step: str
    date: str
    engine_version: str
    input: InputMeta
    outputs: Dict[str, OutputMeta]  # key = symbol / shard / unit


class MetaRegistry:
    """
    融合版 Meta 系统：
    - store
    - manifest
    - validator
    """

    def __init__(
            self,
            meta_dir: Path,
            step: str,
            engine_version: str,
            input_file: Path,
    ) -> None:

        self.meta_dir = meta_dir
        self.step = step

        self.meta_file = self.meta_dir / f"{step}.json"

        self.engine_version = engine_version
        self.input_file = input_file
        #
        self._manifest: Manifest | None = None
        self._new_outputs: Dict[str, OutputMeta] = {}

    # -------------------------------------------------
    # load / input check
    # -------------------------------------------------
    def load(self) -> Manifest | None:
        if not self.meta_file.exists():
            self._manifest = None
            return None

        data = json.loads(self.meta_file.read_text())
        self._manifest = self._from_dict(data)
        return self._manifest

    def is_input_changed(self) -> bool:
        if self._manifest is None:
            return True

        cur_size = self.input_file.stat().st_size
        return cur_size != self._manifest.input.size

    # -------------------------------------------------
    # output validation
    # -------------------------------------------------
    def validate_outputs(self) -> dict[str, bool]:
        """
        return: {key: ok}
        """
        assert self._manifest is not None

        results: dict[str, bool] = {}

        for key, meta in self._manifest.outputs.items():
            path = Path(meta.path)
            results[key] = self._check_output(meta, path)

        return results

    def _check_output(self, meta: OutputMeta, path: Path) -> bool:
        if not path.exists():
            return False

        if meta.size is not None:
            if path.stat().st_size != meta.size:
                return False

        # if meta.rows is not None:
        #     try:
        #         rows = pq.ParquetFile(path).metadata.num_rows
        #     except Exception:
        #         return False
        #     if rows != meta.rows:
        #         return False

        # hash 预留
        return True

    # -------------------------------------------------
    # write new manifest
    # -------------------------------------------------
    def begin_new(self) -> None:
        self._new_outputs.clear()

    def record_output(self, key: str, path: Path) -> None:
        # pf = pq.ParquetFile(path)

        self._new_outputs[key] = OutputMeta(
            path=str(path),
            rows=-1,
            size=path.stat().st_size,
            hash=None,
        )

    def commit(self) -> None:
        manifest = Manifest(
            step=self.step,
            date=datetime.datetime.now().strftime("%Y%m%d%H%M%S"),
            engine_version=self.engine_version,
            input=InputMeta(
                path=str(self.input_file),
                size=self.input_file.stat().st_size,
                hash=None,
            ),
            outputs=self._new_outputs,
        )

        self.meta_file.parent.mkdir(parents=True, exist_ok=True)
        self.meta_file.write_text(
            json.dumps(self._to_dict(manifest), indent=2)
        )

    # -------------------------------------------------
    # helpers
    # -------------------------------------------------
    def _to_dict(self, m: Manifest) -> dict:
        return {
            "step": m.step,
            "date": m.date,
            "engine_version": m.engine_version,
            "input": m.input.__dict__,
            "outputs": {
                k: v.__dict__ for k, v in m.outputs.items()
            },
        }

    def _from_dict(self, d: dict) -> Manifest:
        return Manifest(
            step=d["step"],
            date=d["date"],
            engine_version=d["engine_version"],
            input=InputMeta(**d["input"]),
            outputs={
                k: OutputMeta(**v)
                for k, v in d["outputs"].items()
            },
        )
