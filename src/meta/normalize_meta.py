# from __future__ import annotations
#
# import json
# from pathlib import Path
# from datetime import datetime, timezone
#
# from src.utils.filesystem import FileSystem
# from src.meta.meta import MetaResult
#
#
# class NormalizeMeta:
#     """
#     Normalize Meta 工具（冻结版）
#
#     职责：
#       - 读取 manifest
#       - 判断 inputs 是否发生变化（MVP：仅 size）
#       - 原子提交 NormalizeResult（含 index slice）
#
#     约束：
#       - 不自行处理文件写入原子性
#       - 不直接使用 os / open / rename
#       - 所有 I/O 委托给 FileSystem
#     """
#
#     def __init__(self, meta_dir: Path):
#         self.meta_dir = Path(meta_dir)
#         # self.manifest_path = self.meta_dir / "manifest.json"
#         self.name = "manifest.json"
#
#     # ------------------------------------------------------------------
#     # Skip / Rerun decision
#     # ------------------------------------------------------------------
#     def should_skips(self, input_file: Path) -> bool:
#         """
#         False  -> 需要重跑 Normalize
#         True -> 可安全跳过
#
#         MVP 冻结规则：
#           - manifest 不存在 -> rerun
#           - input size 变化  -> rerun
#         """
#         if not FileSystem.file_exists(self.meta_dir):
#             return False
#
#         manifest_path = self.get_manifest(input_file.stem)
#         if not manifest_path.exists():
#             # print(f"manifest_path: {manifest_path}")
#             return False
#
#         with open(manifest_path, "r", encoding="utf-8") as f:
#             manifest = json.load(f)
#         recorded = manifest.get("outputs", {})
#
#         recorded_size = recorded.get("size")
#         current_size = FileSystem.get_file_size(input_file)
#         # print(f"current_size: {current_size}")
#         # print(f"recorded_size: {recorded_size}")
#
#         return recorded_size != current_size
#
#     # ------------------------------------------------------------------
#     # Commit NormalizeResult (唯一入口)
#     # ------------------------------------------------------------------
#     def commit(self, result: MetaResult) -> None:
#         """
#         原子提交 Normalize 结果
#
#         NormalizeResult 被视为 Normalize 的“事实对象”：
#           - Meta 负责补充文件系统指纹
#           - Meta 负责组织 manifest 结构
#           - Meta 负责持久化
#         """
#
#         output_file = result.output_file
#
#         manifest = {
#             "version": 1,
#             "stage": "normalize",
#
#             # MVP：inputs 只用于 rerun 判定（size）
#             "inputs": {
#                 "file": result.input_file,
#                 "size": None,
#                 "sha256": -1,
#                 "rows": None,
#             },
#
#             "outputs": {
#                 "file": str(output_file),
#                 "size": FileSystem.get_file_size(output_file),
#                 "sha256": -1,  # MVP 占位
#                 "rows": result.rows,
#                 "sorted_by": ["symbol", "ts"],
#             },
#
#             "index": {
#                 "type": "symbol_slice",
#                 "format": "arrow_slice_v1",
#                 "symbols": {
#                     symbol: [start, length]
#                     for symbol, (start, length) in result.index.items()
#                 },
#             },
#
#             "created_at": datetime.now(timezone.utc).isoformat(),
#         }
#
#         # 确保目录存在
#         FileSystem.ensure_dir(self.meta_dir)
#
#         # 使用统一原子写入
#         data = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
#         manifest_path = self.get_manifest(result.input_file.stem)
#         FileSystem.safe_write(manifest_path, data)
#
#     def get_manifest(self, input_file: str) -> Path:
#         manifest_path = self.meta_dir / f'{input_file}.{self.name}'
#         return manifest_path
#
#     def normalize_changed(self, name: str) -> bool:
#         """
#         后续 Step 使用：
#         - True  -> Normalize 发生变化，需要重跑
#         - False -> 可安全复用
#         """
#         manifest_path = self.get_manifest(name)
#         if not manifest_path.exists():
#             return True
#
#         # MVP：只要 manifest 存在，就认为稳定
#         # 未来可扩展：
#         # - 读取 version
#         # - 读取 inputs / outputs hash
#         return False
