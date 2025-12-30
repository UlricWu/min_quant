# tests/meta/test_base_meta.py
from pathlib import Path

from src.meta.base import BaseMeta, MetaOutput


def test_commit_and_load_manifest(tmp_path: Path):
    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()

    meta = BaseMeta(
        meta_dir=meta_dir,
        stage="download",
        output_slot="test",
    )

    input_file = tmp_path / "a.txt"
    output_file = tmp_path / "b.txt"

    input_file.write_text("input", encoding="utf-8")
    output_file.write_text("output", encoding="utf-8")

    meta.commit(
        MetaOutput(
            input_file=input_file,
            output_file=output_file,
            rows=0,
        )
    )

    # manifest 文件存在
    assert meta.path.exists()

    manifest = meta.load()
    assert manifest["stage"] == "download"
    assert manifest["upstream"]["file"] == str(input_file)
    assert manifest["outputs"]["file"] == str(output_file)
def test_upstream_changed_when_no_manifest(tmp_path: Path):
    meta = BaseMeta(
        meta_dir=tmp_path / "meta",
        stage="csv_convert",
        output_slot="x",
    )

    assert meta.upstream_changed() is True
def test_upstream_unchanged(tmp_path: Path):
    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()

    meta = BaseMeta(
        meta_dir=meta_dir,
        stage="csv_convert",
        output_slot="x",
    )

    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "out.parquet"

    input_file.write_text("12345", encoding="utf-8")
    output_file.write_text("parquet", encoding="utf-8")

    meta.commit(
        MetaOutput(
            input_file=input_file,
            output_file=output_file,
            rows=10,
        )
    )

    assert meta.upstream_changed() is False
def test_upstream_changed_on_size_change(tmp_path: Path):
    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()

    meta = BaseMeta(
        meta_dir=meta_dir,
        stage="csv_convert",
        output_slot="x",
    )

    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "out.parquet"

    input_file.write_text("12345", encoding="utf-8")
    output_file.write_text("parquet", encoding="utf-8")

    meta.commit(
        MetaOutput(
            input_file=input_file,
            output_file=output_file,
            rows=10,
        )
    )

    # 修改上游文件（size 变化）
    input_file.write_text("123456789", encoding="utf-8")

    assert meta.upstream_changed() is True
def test_output_changed_triggers_rerun(tmp_path: Path):
    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()

    meta = BaseMeta(
        meta_dir=meta_dir,
        stage="normalize",
        output_slot="x",
    )

    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "out.parquet"

    input_file.write_text("input", encoding="utf-8")
    output_file.write_text("output", encoding="utf-8")

    meta.commit(
        MetaOutput(
            input_file=input_file,
            output_file=output_file,
            rows=100,
        )
    )

    # 破坏输出
    output_file.write_text("CORRUPTED", encoding="utf-8")

    assert meta.upstream_changed() is True
def test_upstream_changed_when_output_deleted(tmp_path: Path):
    """
    不变式：
      - 如果 manifest 记录的 output 文件被删除
      - upstream_changed() 必须返回 True
    """
    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()

    meta = BaseMeta(
        meta_dir=meta_dir,
        stage="csv_convert",
        output_slot="x",
    )

    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "out.parquet"

    input_file.write_text("input data", encoding="utf-8")
    output_file.write_text("parquet data", encoding="utf-8")

    meta.commit(
        MetaOutput(
            input_file=input_file,
            output_file=output_file,
            rows=100,
        )
    )

    # 刚提交，应当可复用
    assert meta.upstream_changed() is False

    # 删除 output
    output_file.unlink()
    assert not output_file.exists()

    # 必须触发重跑
    assert meta.upstream_changed() is True
