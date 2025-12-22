import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch
import pyarrow as pa
from logs.engines import ExtractorEngine
import pytest

def test_read_header_reads_first_line_only():
    fake_proc = MagicMock()
    fake_proc.stdout.readline.return_value = b"col1,col2,col3\n"

    with patch.object(subprocess, "Popen", return_value=fake_proc):
        header = ExtractorEngine._read_header(Path("dummy.csv.7z"))

    assert header == ["col1", "col2", "col3"]
    fake_proc.kill.assert_called_once()

def test_open_reader_requires_csv_7z():
    with pytest.raises(ValueError):
        ExtractorEngine.open_reader(Path("bad.txt"))


def test_cast_strings_forces_string_schema():
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array([1.1, 2.2, 3.3], type=pa.float64()),
        ],
        names=["a", "b"],
    )

    out = ExtractorEngine.cast_strings(batch)

    assert all(
        field.type == pa.string()
        for field in out.schema
    )

def test_open_reader_outputs_string_batches():
    fake_reader = MagicMock()
    fake_reader.__iter__.return_value = iter([
        pa.record_batch(
            [pa.array(["1", "2"])],
            names=["col"],
        )
    ])

    with patch.object(ExtractorEngine, "_read_header", return_value=["col"]), \
         patch("pyarrow.csv.open_csv", return_value=fake_reader), \
         patch("subprocess.Popen"):
        reader = ExtractorEngine.open_reader(Path("a.csv.7z"))

        batch = next(iter(reader))
        assert batch.schema.field("col").type == pa.string()