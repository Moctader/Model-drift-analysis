from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    LOG_LEVEL: str
    FASTAPI_PORT: int
    PROJECT_NAME: str

    LOGGER_CONSOLE: bool
    DEBUG: bool = False

    VERSION: str = "0.1.0"
    DESCRIPTION: str = "API description"
    DOCS_URL: str = "/docs"
    REDOCS_URL: str = "/redoc"
    OPENAPI_URL: str = "/openapi.json"
    API_PREFIX: str = "/api/v1"

    model_config = SettingsConfigDict(
        case_sensitive=True, env_file=".env", extra="allow")


settings = Settings()
