from pydantic import BaseModel


class SecretConfig(BaseModel):
    ftp_host: str | None = None
    ftp_port: int | None = None
    ftp_user: str | None = None
    ftp_password: str | None = None
    tushare_token: str | None = None
