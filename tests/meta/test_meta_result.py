from pathlib import Path
from src.meta.base import MetaOutput


def test_meta_result_basic_fields(tmp_path: Path):
    input_file = tmp_path / "a.txt"
    output_file = tmp_path / "b.txt"

    result = MetaOutput(
        input_file=input_file,
        output_file=output_file,
        rows=123,
    )

    assert result.input_file == input_file
    assert result.output_file == output_file
    assert result.rows == 123
    assert result.index is None

def test_meta_result_with_index(tmp_path: Path):
    input_file = tmp_path / "a.txt"
    output_file = tmp_path / "b.txt"

    result = MetaOutput(
        input_file=input_file,
        output_file=output_file,
        rows=10,
        index={"000001": (0, 10)},
    )

    assert "000001" in result.index
