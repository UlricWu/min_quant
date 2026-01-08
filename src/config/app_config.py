#!filepath: src/config/app_config.py
import yaml
from pydantic import BaseModel
from dotenv import load_dotenv
import os

from .log_config import LogConfig
from .data_config import DataConfig
from .model_config import ModelConfig
from .pipeline_config import PipelineConfig
from .secret_config import SecretConfig
from .backtest_config import BacktestConfig
from .training_config import TrainingConfig
from src import logs
def project_root() -> str:
    """
    返回项目根目录（基于当前文件位置推导）:
    src/config/app_config.py → src/config → src → project_root
    """
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))


def load_env_auto(root: str):
    """
    根据环境变量 ENV 自动加载不同 .env 文件
    - ENV=dev → .env.dev
    - ENV=prod → .env.prod
    - 默认 ENV=dev
    """
    env = os.getenv("ENV", "dev").lower()

    env_file_map = {
        "dev": ".env.dev",
        "prod": ".env.prod",
        "test": ".env.test",
    }

    if env not in env_file_map:
        raise ValueError(f"Unknown ENV={env}, expected one of {list(env_file_map.keys())}")

    env_file = env_file_map[env]
    env_path = os.path.join(root, env_file)

    if not os.path.exists(env_path):
        raise FileNotFoundError(f"Env file not found: {env_path}")

    logs.info(f"[AppConfig] Loading ENV={env} from {env_file}")
    load_dotenv(env_path)

class AppConfig(BaseModel):
    log: LogConfig
    data: DataConfig
    model: ModelConfig
    pipeline: PipelineConfig
    secret: SecretConfig
    backtest: BacktestConfig
    training: TrainingConfig

    @classmethod
    def load(
            cls,
            *,
            path: str | None = None,
            override: dict | None = None,
    ) -> "AppConfig":
        """
        Load AppConfig with optional dict override.

        Priority:
        - YAML base
        - override dict (deep merge)
        """
        root = project_root()

        load_env_auto(root)

        # 1) Load YAML base
        if path is None:
            path = os.path.join(root, "src/config/base.yml")

        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        # 2) Inject secret from env
        raw["secret"] = {
            "ftp_host": os.getenv("FTP_HOST"),
            "ftp_port": os.getenv("FTP_PORT"),
            "ftp_user": os.getenv("FTP_USER"),
            "ftp_password": os.getenv("FTP_PASSWORD"),
            "tushare_token": os.getenv("TUSHARE_TOKEN"),
        }

        # 3) Apply override (if any)
        if override:
            raw = _deep_merge(raw, override)

        return cls(**raw)

def _deep_merge(base: dict, override: dict) -> dict:
    """
    Recursively merge override into base (immutable).
    """
    out = dict(base)
    for k, v in override.items():
        if (
            k in out
            and isinstance(out[k], dict)
            and isinstance(v, dict)
        ):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out