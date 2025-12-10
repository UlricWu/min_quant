# !filepath: src/config/environment.py

import os
from dotenv import load_dotenv

def load_env_auto():
    """
    根据环境变量 ENV 自动加载 .env 文件。
    """

    env = os.getenv("ENV")

    # 默认环境：dev
    if env is None:
        env = "dev"

    # 映射环境到文件
    env_file_map = {
        "dev": ".env.dev",
        "prod": ".env.prod",
        "test": ".env.test",
    }

    env_file = env_file_map.get(env)

    if env_file is None:
        raise ValueError(f"Unknown ENV={env}")

    # 判断文件是否存在
    if not os.path.exists(env_file):
        raise FileNotFoundError(f"{env_file} not found.")

    print(f"[ENV] Using environment: {env}")
    print(f"[ENV] Loading file: {env_file}")

    load_dotenv(env_file)
