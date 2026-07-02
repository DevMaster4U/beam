"""Control server configuration."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_data_dir() -> Path:
    root = Path(__file__).resolve().parents[1]
    return root / "data" / "control-server"


class ControlServerSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    host: str = Field(default="0.0.0.0", validation_alias="CONTROL_SERVER_HOST")
    port: int = Field(default=8010, validation_alias="CONTROL_SERVER_PORT")
    secret: str = Field(..., validation_alias="CONTROL_SERVER_SECRET")
    data_dir: Path = Field(
        default_factory=_default_data_dir,
        validation_alias="CONTROL_SERVER_DATA_DIR",
    )
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    @property
    def miners_dir(self) -> Path:
        return self.data_dir / "miners"

    @property
    def cache_dir(self) -> Path:
        return self.data_dir / "cache"

    @property
    def wallets_dir(self) -> Path:
        return self.data_dir / "wallets"

    @property
    def predefined_etag_cache_path(self) -> Path:
        return self.cache_dir / "predefined_etag_chunks.json"


@lru_cache
def get_settings() -> ControlServerSettings:
    root = Path(__file__).resolve().parents[1]
    env_file = root / "config" / "control-server.env"
    if env_file.is_file():
        settings = ControlServerSettings(_env_file=str(env_file))
    else:
        settings = ControlServerSettings()
    settings.miners_dir.mkdir(parents=True, exist_ok=True)
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    settings.wallets_dir.mkdir(parents=True, exist_ok=True)
    if not settings.predefined_etag_cache_path.is_file():
        settings.predefined_etag_cache_path.write_text(
            '{"entries": {}, "updated_at": null}\n',
            encoding="utf-8",
        )
    return settings
