from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str
    SYNC_DATABASE_URL: str
    
    # Redis
    REDIS_URL: str
    CACHE_REDIS_URL: str
    
    # Security
    SECRET_KEY: str = "your-super-secret-key-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    API_KEY: str
    
    # Celery
    CELERY_BROKER_URL: str
    CELERY_RESULT_BACKEND: str
    
    # GeoIP - paths to MaxMind GeoLite2 database files
    GEOIP_COUNTRY_PATH: Optional[str] = "/opt/geoip/GeoLite2-Country.mmdb"
    GEOIP_ASN_PATH: Optional[str] = "/opt/geoip/GeoLite2-ASN.mmdb"
    MAXMIND_ACCOUNT_ID: Optional[str] = None   # your MaxMind account ID
    MAXMIND_LICENSE_KEY: Optional[str] = None   # your MaxMind license key
    
    # App Config
    PROJECT_NAME: str = "VPN Load Balancer API"
    VERSION: str = "1.0.0"
    DEBUG: bool = True
    ALLOWED_HOSTS: str = '["*"]'
    ALLOWED_ORIGINS: str = "http://localhost:3000"
    
    @property
    def cors_origins(self):
        """Convert comma-separated string to list"""
        return [origin.strip() for origin in self.ALLOWED_ORIGINS.split(",")]
    
    class Config:
        env_file = ".env"
        case_sensitive = False  # Allows lowercase/uppercase env vars
        extra = "allow"  # Allows extra fields without errors


@lru_cache()
def get_settings():
    return Settings()


settings = get_settings()