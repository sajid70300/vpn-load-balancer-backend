from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


# ==================== Auth / User Schemas ====================

class RegisterRequest(BaseModel):
    name: str
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: "UserResponse"


class UserResponse(BaseModel):
    id: int
    name: str
    email: str
    role: str
    status: str
    created_at: datetime
    last_login_at: Optional[datetime] = None


class UserRoleUpdate(BaseModel):
    role: str  # 'admin' or 'user'


# ==================== App Management Schemas ====================

class AppCreate(BaseModel):
    name: str
    app_id: str


class AppResponse(BaseModel):
    id: int
    name: str
    app_id: str
    status: str
    created_at: datetime
    updated_at: datetime
    active_users: int = 0
    total_servers: int = 0


class AppAnalytics(BaseModel):
    app_id: str
    name: str
    current_load_pct: float
    active_sessions: int
    total_servers: int
    active_servers: int
    total_capacity: int


# ==================== Connection Feedback ====================

class ConnectionFeedback(BaseModel):
    """
    Client sends this after a full connection attempt cycle.

    Rules:
    - Primary succeeded  → fill primary_* only; leave secondary_* as None
    - Primary failed, secondary succeeded → fill both
    - Both failed → fill both with success=False → triggers cooldown

    server_id is the single unified row id (same for both protocols).
    user_id is required only when shadowsocks is the successful protocol.
    OpenVPN sessions are tracked automatically by the Celery monitoring task.
    """
    server_id: int
    country: Optional[str] = None
    asn: Optional[str] = None
    network_type: Optional[str] = None   # wifi | mobile
    user_id: Optional[str] = None        # required only when shadowsocks succeeds

    primary_protocol: str                # 'openvpn' or 'shadowsocks'
    primary_success: bool
    primary_connect_time_ms: Optional[float] = None

    secondary_protocol: Optional[str] = None
    secondary_success: Optional[bool] = None
    secondary_connect_time_ms: Optional[float] = None


class ShadowsocksDisconnect(BaseModel):
    """Client sends this when user disconnects from Shadowsocks."""
    user_id: str
    server_id: int


class ProtocolConfig(BaseModel):
    """Protocol-specific configuration returned to the client."""
    protocol: str
    server_id: int       # always the unified VPNServer row id
    server_name: str
    ip_address: str

    # OpenVPN specific
    ovpn_base64: Optional[str] = None
    management_port: Optional[int] = None

    # Shadowsocks specific
    ss_port: Optional[int] = None
    ss_password: Optional[str] = None
    ss_encryption: Optional[str] = None


class BestServerDecision(BaseModel):
    """Decision engine response with primary and fallback protocol configs."""
    app_name: Optional[str]

    primary_protocol: str
    primary_config: ProtocolConfig
    primary_score: float

    fallback_protocol: str
    fallback_config: ProtocolConfig
    fallback_score: float

    # Both configs share the same physical server
    server_type: str
    server_city: Optional[str]
    server_country: Optional[str]
    flag_image_url: Optional[str]

    cpu_usage: float
    ram_usage: float
    ping_ms: float
    load_score: float
    current_users: int
    max_capacity: int

    retry_policy: dict = Field(
        default={
            "max_primary_retries": 2,
            "max_fallback_retries": 2,
            "retry_delay_seconds": 3,
            "new_server_after_failures": 4
        }
    )


# ==================== Server Management Schemas ====================

class ServerResponse(BaseModel):
    id: int
    name: str
    ip_address: str
    is_active: bool
    server_type: str
    app_name: Optional[str]
    server_city: Optional[str]
    server_country: Optional[str]

    # OpenVPN
    management_port: Optional[int] = None
    ovpn_base64: Optional[str] = None
    config_tag: Optional[str] = None
    cn_match: Optional[str] = None

    # Shadowsocks
    ss_port: Optional[int] = None
    ss_password: Optional[str] = None
    ss_encryption: Optional[str] = None

    cpu_usage: float
    ram_usage: float
    ping_latency_ms: float
    load_score: float
    current_users: int
    max_capacity: int

    is_priority_group: bool
    last_health_check: Optional[datetime]


# ==================== Metrics Schemas ====================

class ProtocolMetricsResponse(BaseModel):
    server_id: int
    server_name: str
    protocol: str
    country: Optional[str]
    asn: Optional[str]

    success_count: int
    failure_count: int
    total_attempts: int
    success_rate: float

    avg_connect_time_ms: float
    consecutive_failures: int

    cooldown_until: Optional[datetime]
    cooldown_level: str

    last_success_at: Optional[datetime]
    last_failure_time: Optional[datetime]
    last_failure_reason: Optional[str]


# ==================== Policy Schemas ====================

class CountryPolicyCreate(BaseModel):
    app_name: Optional[str] = None
    country: str
    preferred_protocol: Optional[str] = None
    fallback_protocol: Optional[str] = None
    protocol_bias_score: float = 0.0
    notes: Optional[str] = None


class CountryPolicyResponse(BaseModel):
    id: int
    app_name: Optional[str]
    country: str
    preferred_protocol: Optional[str]
    fallback_protocol: Optional[str]
    protocol_bias_score: float
    is_active: bool
    notes: Optional[str]
    created_at: datetime
    updated_at: datetime


class ISPPolicyCreate(BaseModel):
    app_name: Optional[str] = None
    country: str
    asn: str
    protocol: str
    status: str = "normal"  # preferred, degraded, blocked
    bias_score: float = 0.0
    expiry: Optional[datetime] = None
    notes: Optional[str] = None


class ISPPolicyResponse(BaseModel):
    id: int
    app_name: Optional[str]
    country: str
    asn: str
    protocol: str
    status: str
    bias_score: float
    expiry: Optional[datetime]
    notes: Optional[str]
    created_at: datetime


# ==================== Global Settings Schemas ====================

class GlobalSettingsResponse(BaseModel):
    protocol_mode: str
    disable_new_connections: bool
    enforce_country_policies: bool
    enforce_isp_policies: bool
    cooldown_soft_seconds: int
    cooldown_medium_seconds: Optional[int] = None
    cooldown_hard_seconds: int
    failure_rate_threshold: float
    cooldown_country_block_asn_threshold: int
    updated_at: Optional[datetime] = None


class GlobalSettingsUpdate(BaseModel):
    protocol_mode: Optional[str] = None
    disable_new_connections: Optional[bool] = None
    enforce_country_policies: Optional[bool] = None
    enforce_isp_policies: Optional[bool] = None
    cooldown_soft_seconds: Optional[int] = None
    cooldown_medium_seconds: Optional[int] = None
    cooldown_hard_seconds: Optional[int] = None
    failure_rate_threshold: Optional[float] = None
    cooldown_country_block_asn_threshold: Optional[int] = None


# ==================== Legacy / Public API Schemas ====================

class BestServerResponse(BaseModel):
    app_name: Optional[str]
    server: str
    ip_address: str
    max_capacity: int
    current_users: int
    load_score: float
    cpu_usage: float
    ram_usage: float
    ping_ms: float
    server_type: str
    server_city: Optional[str]
    flag_image_url: Optional[str]
    ovpn_base64: Optional[str]


class ServerLoadItem(BaseModel):
    server: str
    app_name: Optional[str]
    ip_address: str
    max_capacity: int
    server_type: str
    server_city: Optional[str]
    connected_users: int | str
    load_percentage: float | str
    load_score: float
    cpu_usage: float
    ram_usage: float
    ping_ms: float
    last_health_check: Optional[datetime]
    total_bytes_received: int
    total_bytes_sent: int


class LoadSummary(BaseModel):
    total_servers: int
    active_servers: int
    total_capacity: int
    total_connected_users: int
    overall_load_percentage: float
    total_bytes_received: int
    total_bytes_sent: int


class ServersLoadResponse(BaseModel):
    servers_load: list[ServerLoadItem]
    summary: LoadSummary


class UserSession(BaseModel):
    user_id: str
    device_ip: str
    bytes_received: int
    bytes_sent: int
    connected_time: datetime
    server_name: str
    server_type: str
    app_name: Optional[str]
    config_tag: Optional[str]
    protocol: str = "openvpn"


class AllUsersResponse(BaseModel):
    users: list[UserSession]


class ServerConfig(BaseModel):
    app_name: Optional[str]
    server: str
    ip_address: str
    is_active: bool
    server_type: str
    server_city: Optional[str]
    flag_image_url: Optional[str]
    ovpn_base64: Optional[str]


class ServerConfigDecision(BaseModel):
    """
    Response for /servers_config/ when country/asn/network_type are provided.
    Both primary and fallback reference the same unified server row.
    """
    app_name: Optional[str]
    server: str
    ip_address: str
    is_active: bool
    server_type: str
    server_city: Optional[str]
    flag_image_url: Optional[str]

    primary_protocol: str
    primary_config: ProtocolConfig
    primary_score: float

    fallback_protocol: str
    fallback_config: ProtocolConfig
    fallback_score: float