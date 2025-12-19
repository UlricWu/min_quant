from pydantic import BaseModel


class SecretConfig(BaseModel):
    ftp_host: str = ''
    ftp_port: int = ''
    ftp_user: str = ''
    ftp_password: str = ''
    tushare_token: str = ''
    remote_root: str = ''
