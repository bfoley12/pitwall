from pydantic_settings import BaseSettings


class ClientSettings(BaseSettings):
    connect: float = 5.0
    read: float = 10.0
    max_connections: int = 10
    max_keepalive_connections: int = 5
    follow_redirects: bool = True
    total_timeout: float = 5.0
    request_timeout: float = 10
