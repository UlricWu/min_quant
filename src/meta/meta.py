from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, Any

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, Optional

from src.utils.filesystem import FileSystem


@dataclass(frozen=True)
class MetaResult:
    """
    MetaResult（冻结版 · 能力声明式）

    表达：
      - 我从什么输入
      - 产出了什么事实文件
      - 这个事实文件的基本规模
      - （可选）它是否具备结构性索引能力

    设计原则：
      - 不包含任何业务语义
      - index 是能力声明，不是 stage 专属
    """

    input_file: Path
    output_file: Path
    rows: int

    # 可选结构性能力（如 symbol slice）
    index: Optional[Dict[str, Tuple[int, int]]] = None


class BaseMeta:
    """
    BaseMeta（冻结 v1.1）

    统一职责：
      - 记录“在什么上游状态下”产生了某个 Result
      - 判断上游是否发生变化
      - 以 manifest 形式声明结果能力（如 symbol_slice）

    设计铁律：
      - 不理解业务
      - 不解析 index
      - 不区分 normalize / min / feature
    """

    def __init__(self, meta_dir: Path, stage: str):
        self.meta_dir = Path(meta_dir)
        self.stage = stage

    # --------------------------------------------------
    # Manifest path
    # --------------------------------------------------
    def manifest_path(self, file_stem: str | Path, stage: str | None = None) -> Path:
        if isinstance(file_stem, Path):
            file_stem = file_stem.stem

        stage = stage or self.stage
        name = file_stem.split(".")[0]

        return self.meta_dir / f"{name}.{stage}.manifest.json"

    # --------------------------------------------------
    # Load
    # --------------------------------------------------
    def load(self, file_stem: str | Path) -> Dict[str, Any] | None:
        path = self.manifest_path(file_stem)
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    # --------------------------------------------------
    # Commit
    # --------------------------------------------------
    def commit(self, result: MetaResult) -> None:
        if isinstance(result, dict):
            result = MetaResult(**result)
        payload: Dict[str, Any] = {
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
                "size": FileSystem.get_file_size(result.output_file),
            },
        }

        # 🔒 能力声明（可选）
        if result.index is not None:
            payload["outputs"]["index"] = {
                "type": "symbol_slice",
                "format": "arrow_slice_v1",
                "symbols": {
                    symbol: [start, length]
                    for symbol, (start, length) in result.index.items()
                },
            }

        data = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        FileSystem.safe_write(
            self.manifest_path(result.input_file.stem),
            data,
        )

    # --------------------------------------------------
    # Change detection（冻结 v1）
    # --------------------------------------------------
    def upstream_changed(self, input_file: Path) -> bool:
        """
        判断上游是否发生变化：

        True  -> 需要重跑
        False -> 可复用
        """
        manifest = self.load(input_file.stem)

        # 没有历史记录
        if manifest is None:
            return True

        recorded = (
            manifest
            .get("upstream", {})
            .get("fingerprint", {})
        )

        current = {
            "size": FileSystem.get_file_size(input_file),
        }

        if recorded.get("size") != current.get("size"):
            return True

        # 下游完整性校验
        output_file = Path(
            manifest
            .get("outputs", {})
            .get("file", "")
        )

        if not output_file.exists():
            return True

        if (
                manifest["outputs"].get("size")
                != FileSystem.get_file_size(output_file)
        ):
            return True

        return False
