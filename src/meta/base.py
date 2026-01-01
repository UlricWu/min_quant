from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, Any

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, Optional

from src import logs
from src.utils.filesystem import FileSystem
from src import logs

@dataclass(frozen=True)
class MetaOutput:
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


import json
from pathlib import Path
from typing import List, Optional


# ----------------------------------------------------------------------
# BaseMeta v2（冻结）
# ----------------------------------------------------------------------
class BaseMeta:
    META_VERSION = 1.2

    def __init__(
            self,
            meta_dir: Path,
            stage: str,
            output_slot: str,
            # inst: Optional[str] = None,
    ) -> None:
        self.meta_dir = meta_dir
        self.stage = stage
        self.output_slot = output_slot
        # self.inst = inst

    # --------------------------------------------------
    @property
    def name(self) -> str:
        parts = [self.stage, self.output_slot]
        # if self.inst:
        #     parts.append(self.inst)
        return ".".join(parts) + ".manifest.json"

    # --------------------------------------------------
    @property
    def path(self) -> Path:
        return self.meta_dir / self.name

    # --------------------------------------------------
    def exists(self) -> bool:
        return self.path.exists()

    # --------------------------------------------------
    def load(self) -> dict:
        with self.path.open("r", encoding="utf-8") as f:
            return json.load(f)

    # --------------------------------------------------
    def commit(self, result: MetaOutput | dict) -> None:
        if isinstance(result, dict):
            result = MetaOutput(**result)

        payload: Dict[str, Any] = {
            "version": self.META_VERSION,
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
            self.path,
            data,
        )

    def upstream_changed(self) -> bool:
        """
        判断上游是否发生变化：

        True  -> 需要重跑
        False -> 可复用
        """
        # # 没有历史记录
        if not self.path.exists():
            logs.debug(f'[meta] manifest not exist: {self.path.name}')
            return True

        manifest = self.load()

        recorded = (
            manifest
            .get("upstream", {})
            .get("fingerprint", {})
        )
        input_file = manifest.get("upstream", {}).get("file", '')

        if input_file is None:
            logs.debug(f'[meta] input_file not exist: {self.path.name}')
            return True

        current = {
            "size": FileSystem.get_file_size(input_file),
        }
        if recorded.get("size") != current.get("size"):
            logs.warning(f'[meta] upstream_changed size')
            return True

        # 下游完整性校验
        output_file = Path(
            manifest
            .get("outputs", {})
            .get("file", "")
        )

        if not output_file.exists():
            logs.debug(f'[meta] output_file not exist: {output_file}')
            return True

        if (
                manifest["outputs"].get("size")
                != FileSystem.get_file_size(output_file)
        ):
            logs.warning(f'[meta] output_file size change')
            return True

        return False
