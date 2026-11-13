"""测试用临时 SQLite 数据目录。

必须在任何 app 模块 import 之前设置 DATA_DIR——app.db 在 import 时即绑定路径。
"""

import os
import shutil
import tempfile

_TMP_ROOT = tempfile.mkdtemp(prefix="changeflow-test-")
os.environ["DATA_DIR"] = _TMP_ROOT

import pytest  # noqa: E402

from app.config import DATA_DIR, DB_FILENAME  # noqa: E402
from app.seed import init_db  # noqa: E402


@pytest.fixture()
def db():
    path = DATA_DIR / DB_FILENAME
    if path.exists():
        path.unlink()
    init_db()
    return path


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(_TMP_ROOT, ignore_errors=True)
