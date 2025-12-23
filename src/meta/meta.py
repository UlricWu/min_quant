from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any

from src.utils.filesystem import FileSystem

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, Optional


@dataclass(frozen=True)
class MetaResult:
    """
    Result（统一事实结果，冻结版）

    表达：
      - 我从什么输入
      - 产出了什么事实文件
      - 这个事实文件的基本规模
      - （可选）附带结构性索引能力
    - input_file: 上游输入文件
    - output_file: 当前 stage 产出的事实文件
    - rows: 输出行数
    - index: 可选的结构性索引能力（如 symbol slice）
    """

    input_file: Path
    output_file: Path
    rows: int

    # 可选能力（只有 Normalize / 某些 Step 才会用）
    index: Optional[Dict[str, Tuple[int, int]]] = None


class BaseMeta:
    """
    BaseMeta（冻结 v1）

    适用于：
    统一职责：
      - 记录“在什么上游状态下”产生了某个 Result
      - 判断上游是否发生变化
      - 所有 Step 共用

    """

    def __init__(self, meta_dir: Path, stage: str = ''):
        self.meta_dir = Path(meta_dir)
        self.stage = stage

    # --------------------------------------------------
    # Fingerprint helpers（MVP：size + rows）
    # --------------------------------------------------
    @staticmethod
    def fingerprint_from_file(path: Path, rows: int) -> Dict[str, Any]:
        return {
            "size": FileSystem.get_file_size(path),
            "rows": rows,
        }

    # --------------------------------------------------
    # Manifest path
    # --------------------------------------------------
    def manifest_path(self, file_stem: str | Path, stage='') -> Path:
        if isinstance(file_stem, Path):
            file_stem = file_stem.stem
        if not stage:
            stage = self.stage
        return self.meta_dir / f"{file_stem}.{stage}.manifest.json"

    # --------------------------------------------------
    # Load / Save
    # --------------------------------------------------
    def load(self, name: str) -> Dict[str, Any] | None:
        path = self.manifest_path(name)
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def commit(
            self,
            result: MetaResult,
    ) -> None:
        payload = {
            "version": 1,
            "stage": self.stage,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "upstream": {
                "file": str(result.input_file),
                "fingerprint": {
                    "size": FileSystem.get_file_size(result.input_file),
                },
            },
            "outputs": {
                "file": str(result.output_file),
                "rows": result.rows,
                "size": FileSystem.get_file_size(result.output_file)
            },

        }
        # 只有有 index 的 Result，才记录 index
        if result.index is not None:
            answer = {
                'format': 'arrow_slice_v1',
                'type': 'symbol_slice',
                "symbols": {
                    symbol: [start, length]
                    for symbol, (start, length) in result.index.items()
                },
            }
            payload["outputs"]["index"] = answer

        data = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        FileSystem.safe_write(self.manifest_path(result.output_file.stem), data)

    # --------------------------------------------------
    # Change detection
    # --------------------------------------------------
    def upstream_changed(self, input_file: Path) -> bool:
        """
        自动判断上游是否发生变化（冻结 v1）

        True  -> 上游发生变化，需要重跑
        False -> 上游一致，可复用
        """
        name = input_file.stem
        manifest = self.load(name)

        # 1. 没有历史记录，一定要重跑
        if manifest is None:
            return True

        # --------------------------------------------------
        # 上游一致性检查
        # --------------------------------------------------
        recorded_upstream = (
            manifest
            .get("upstream", {})
            .get("fingerprint", {})
        )

        current_upstream = {
            "size": FileSystem.get_file_size(input_file),
        }

        if recorded_upstream.get("size") != current_upstream.get("size"):
            return True

        # --------------------------------------------------
        # 下游一致性检查（防止输出被篡改 / 删除 / 覆盖）
        # --------------------------------------------------
        output_file = Path(
            manifest
            .get("outputs", {})
            .get("file", "")
        )

        recorded_output = manifest.get("outputs", {})

        if recorded_output is None:
            return True

        current_output_size = FileSystem.get_file_size(output_file)

        if recorded_output.get("size") != current_output_size:
            return True

        # --------------------------------------------------
        # 上游 & 下游均一致 → 结果仍然成立
        # --------------------------------------------------
        return False
