from pathlib import Path

from src.meta.meta import BaseMeta, MetaResult


def test_commit_and_load_manifest(tmp_path: Path):
    meta_dir = tmp_path / "meta"
    meta = BaseMeta(meta_dir, stage="download")

    input_file = tmp_path / "a.txt"
    output_file = tmp_path / "b.txt"

    input_file.write_text("input", encoding="utf-8")
    output_file.write_text("output", encoding="utf-8")

    result = MetaResult(
        input_file=input_file,
        output_file=output_file,
        rows=0,
    )

    meta.commit(result)

    # manifest key = upstream (input)
    manifest_path = meta.manifest_path(input_file.stem)
    assert manifest_path.exists()

    manifest = meta.load(input_file.stem)
    assert manifest is not None
    assert manifest["stage"] == "download"
    assert manifest["upstream"]["file"] == str(input_file)
    assert manifest["outputs"]["file"] == str(output_file)

    # ❗ output_file 不是 manifest key
    assert meta.load(output_file.stem) is None
def test_upstream_changed_when_no_manifest(tmp_path: Path):
    meta = BaseMeta(tmp_path / "meta", stage="csv_convert")

    input_file = tmp_path / "input.csv"
    input_file.write_text("data", encoding="utf-8")

    assert meta.upstream_changed(input_file) is True

def test_upstream_unchanged(tmp_path: Path):
    meta = BaseMeta(tmp_path / "meta", stage="csv_convert")

    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "out.parquet"

    input_file.write_text("12345", encoding="utf-8")
    output_file.write_text("parquet", encoding="utf-8")

    meta.commit(
        MetaResult(
            input_file=input_file,
            output_file=output_file,
            rows=10,
        )
    )

    assert meta.upstream_changed(input_file) is False

def test_upstream_changed_on_size_change(tmp_path: Path):
    meta = BaseMeta(tmp_path / "meta", stage="csv_convert")

    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "out.parquet"

    input_file.write_text("12345", encoding="utf-8")
    output_file.write_text("parquet", encoding="utf-8")

    meta.commit(
        MetaResult(
            input_file=input_file,
            output_file=output_file,
            rows=10,
        )
    )

    # 修改上游
    input_file.write_text("123456789", encoding="utf-8")

    assert meta.upstream_changed(input_file) is True

def test_output_changed_triggers_rerun(tmp_path: Path):
    meta = BaseMeta(tmp_path / "meta", stage="normalize")

    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "out.parquet"

    input_file.write_text("input", encoding="utf-8")
    output_file.write_text("output", encoding="utf-8")

    meta.commit(
        MetaResult(
            input_file=input_file,
            output_file=output_file,
            rows=100,
        )
    )

    # 破坏输出
    output_file.write_text("CORRUPTED", encoding="utf-8")

    assert meta.upstream_changed(input_file) is True
def test_upstream_changed_when_output_deleted(tmp_path: Path):
    """
    不变式：
      - 如果 manifest 记录的 output 文件被删除
      - upstream_changed() 必须返回 True
    """
    meta = BaseMeta(tmp_path / "meta", stage="csv_convert")

    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "out.parquet"

    input_file.write_text("input data", encoding="utf-8")
    output_file.write_text("parquet data", encoding="utf-8")

    # 提交一次成功结果
    meta.commit(
        MetaResult(
            input_file=input_file,
            output_file=output_file,
            rows=100,
        )
    )

    # sanity check：刚提交完，应当可复用
    assert meta.upstream_changed(input_file) is False

    # --------------------------------------------------
    # 模拟下游文件被误删
    # --------------------------------------------------
    output_file.unlink()
    assert not output_file.exists()

    # --------------------------------------------------
    # 核心断言：必须触发重跑
    # --------------------------------------------------
    assert meta.upstream_changed(input_file) is True