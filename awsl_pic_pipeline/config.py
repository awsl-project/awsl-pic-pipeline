import os
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    db_url: str
    migration_limit: int = 50
    awsl_storage_url: Optional[str] = None
    awsl_storage_api_token: Optional[str] = None
    awsl_storage_chat_id: Optional[str] = None
    enable_delete: bool = False

    class Config:
        env_file = os.environ.get("ENV_FILE", ".env")


settings = Settings()
