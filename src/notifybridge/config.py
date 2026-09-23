from pydantic import Field, HttpUrl, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql+asyncpg://notify:notify@localhost:5432/notify"
    rabbitmq_url: str = "amqp://notify:notify@rabbitmq:5672/"
    admin_token: SecretStr = Field(min_length=24)
    provider_url: HttpUrl = HttpUrl("http://provider:8000")
    provider_timeout: float = Field(default=3, ge=0.1, le=30)
    lease_seconds: int = Field(default=15, ge=2, le=300)
    poll_seconds: float = Field(default=0.5, ge=0.05, le=30)
    retry_base: float = Field(default=1, ge=0.1, le=60)
    max_attempts: int = Field(default=5, ge=1, le=20)
    worker_lane: str = "all"

    @model_validator(mode="after")
    def validate_runtime(self):
        if self.lease_seconds <= self.provider_timeout + 1:
            raise ValueError("Срок аренды должен превышать таймаут провайдера минимум на секунду")
        url = self.provider_url
        if url.username or url.password or url.query or url.fragment or url.path not in (None, "/"):
            raise ValueError("PROVIDER_URL должен содержать только схему, хост и порт")
        if self.worker_lane not in ("all", "transactional"):
            raise ValueError("WORKER_LANE: all или transactional")
        return self
