from typing import Optional

from pydantic import BaseSettings


class Config(BaseSettings):
    TABLE_NAME: str = ""
    DYNAMODB_URL: Optional[str] = None
