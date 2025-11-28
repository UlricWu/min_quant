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
from src import logs
def project_root() -> str:
    """
    返回项目根目录（基于当前文件位置推导）:
    src/config/app_config.py → src/config → src → project_root
    """
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))



class AppConfig(BaseModel):
    log: LogConfig
    data: DataConfig
    model: ModelConfig
    pipeline: PipelineConfig
    secret: SecretConfig

    @classmethod
    def load(cls, path: str | None = None) -> "AppConfig":
        """
        加载 YAML 配置 + .env
        - 默认使用 <project_root>/config/base.yml
        - 不依赖当前工作目录
        """
        root = project_root()

        # 1) 先加载 .env（在项目根目录下）
        env_path = os.path.join(root, ".env")
        load_dotenv(env_path)

        # 2) 决定配置文件路径
        if path is None:
            # 统一：config/base.yml（和你说的一致）
            path = os.path.join(root, "src/config/base.yml")

        if not os.path.exists(path):
            raise FileNotFoundError(f"Config file not found: {path}")

        # 3) 读取 YAML
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
            # 3. 从 env 注入 secret
            raw["secret"] = {
                "ftp_host": os.getenv("FTP_HOST"),
                "ftp_port": os.getenv("FTP_PORT"),
                "ftp_user": os.getenv("FTP_USER"),
                "ftp_password": os.getenv("FTP_PASSWORD"),
                "tushare_token": os.getenv("TUSHARE_TOKEN"),
            }
        return cls(**raw)
