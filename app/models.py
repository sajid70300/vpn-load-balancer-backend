from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, BigInteger, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base
import enum


class UserRole(str, enum.Enum):
    SUPERADMIN = "superadmin"
    ADMIN = "admin"
    USER = "user"


class UserStatus(str, enum.Enum):
    PENDING = "pending"
    ACTIVE = "active"
    REJECTED = "rejected"


class DashboardUser(Base):
    """
    Dashboard users with role-based access.
    First registered user becomes superadmin automatically.
    Subsequent users start as 'pending' until approved by superadmin.
    """
    __tablename__ = "dashboard_users"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    email = Column(String(255), nullable=False, unique=True, index=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(20), default=UserRole.USER.value, index=True)
    status = Column(String(20), default=UserStatus.PENDING.value, index=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_login_at = Column(DateTime(timezone=True), nullable=True)


class App(Base):
    """Represents a VPN application/brand (tenant)."""
    __tablename__ = "apps"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    app_id = Column(String(100), nullable=False, unique=True, index=True)
    status = Column(String(20), default='active')  # active, inactive

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class PhysicalMachine(Base):
    """
    Bare physical server record — created in Server Management (Step 1).
    VPNServer rows are created in AppConfigure (Step 2).
    """
    __tablename__ = "physical_machines"

    id             = Column(Integer, primary_key=True, index=True)
    name           = Column(String(100), nullable=False)
    ip_address     = Column(String(45),  nullable=False, unique=True, index=True)
    server_type    = Column(String(10),  default='free', index=True)   # free | premium
    server_city    = Column(String(100), nullable=True)
    server_country = Column(String(100), nullable=True)
    flag_image_url = Column(String(500), nullable=True)
    max_capacity   = Column(Integer, default=100)
    monitoring_api_url = Column(String(500), nullable=True)
    is_active      = Column(Boolean, default=True, index=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    vpn_servers = relationship("VPNServer", back_populates="physical_machine",
                               cascade="all, delete-orphan")


class VPNServer(Base):
    """
    One row per physical-machine + app combination.
    Both OpenVPN and Shadowsocks config fields live on the same row.
    Sessions for both protocols are linked to this single server_id.
    """
    __tablename__ = "vpn_status_vpnserver"

    id = Column(Integer, primary_key=True, index=True)
    physical_machine_id = Column(Integer, ForeignKey('physical_machines.id', ondelete='CASCADE'),
                                 nullable=True, index=True)
    name          = Column(String(100), nullable=False)
    ip_address    = Column(String(45), nullable=False, index=True)
    is_active     = Column(Boolean, default=True, index=True)
    max_capacity  = Column(Integer, default=100)
    server_type   = Column(String(10), default='free', index=True)

    # OpenVPN fields
    management_port = Column(Integer, default=7505)
    ovpn_base64     = Column(Text, nullable=True)
    config_tag      = Column(String(50), nullable=True)
    cn_match        = Column(String(100), nullable=True)

    # Shadowsocks fields
    ss_port       = Column(Integer, nullable=True)
    ss_password   = Column(String(255), nullable=True)
    ss_encryption = Column(String(50), nullable=True)

    app_name          = Column(String(100), nullable=True, index=True)
    server_city       = Column(String(100), nullable=True)
    server_country    = Column(String(100), nullable=True)
    flag_image_url    = Column(String(500), nullable=True)
    is_priority_group = Column(Boolean, default=False, index=True)
    display_order     = Column(Integer, default=0)
    monitoring_api_url = Column(String(500), nullable=True)

    cpu_usage       = Column(Float, default=0.0)
    ram_usage       = Column(Float, default=0.0)
    ping_latency_ms = Column(Float, default=0.0)
    last_health_check = Column(DateTime(timezone=True), nullable=True)
    load_score      = Column(Float, default=0.0, index=True)

    peak_users      = Column(Integer, default=0)
    peak_users_time = Column(DateTime(timezone=True), nullable=True)
    peak_cpu        = Column(Float, default=0.0)
    peak_cpu_time   = Column(DateTime(timezone=True), nullable=True)
    peak_ram        = Column(Float, default=0.0)
    peak_ram_time   = Column(DateTime(timezone=True), nullable=True)

    sessions         = relationship("VPNUserSession", back_populates="server", cascade="all, delete-orphan")
    protocol_metrics = relationship("ProtocolMetrics", back_populates="server", cascade="all, delete-orphan")
    physical_machine = relationship("PhysicalMachine", back_populates="vpn_servers")

    __table_args__ = (
        Index('idx_active_type_app', 'is_active', 'server_type', 'app_name'),
        Index('idx_active_priority', 'is_active', 'is_priority_group'),
    )


class VPNUserSession(Base):
    __tablename__ = "vpn_status_vpnusersession"

    id             = Column(Integer, primary_key=True, index=True)
    server_id      = Column(Integer, ForeignKey('vpn_status_vpnserver.id', ondelete='CASCADE'), index=True)
    user_id        = Column(String(255), nullable=False, index=True)
    device_ip      = Column(String(45), nullable=False)
    connected_time = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    bytes_received = Column(BigInteger, default=0)
    bytes_sent     = Column(BigInteger, default=0)
    config_tag     = Column(String(50), nullable=True, index=True)
    protocol       = Column(String(20), default='openvpn')  # which protocol this session used

    server = relationship("VPNServer", back_populates="sessions")

    __table_args__ = (
        Index('idx_server_config', 'server_id', 'config_tag'),
        Index('idx_session_protocol', 'protocol'),
    )


class ProtocolMetrics(Base):
    """
    Per-protocol performance metrics per server, country, and ISP.
    protocol column here still tracks openvpn vs shadowsocks for each metric row.
    """
    __tablename__ = "protocol_metrics"

    id        = Column(Integer, primary_key=True, index=True)
    server_id = Column(Integer, ForeignKey('vpn_status_vpnserver.id', ondelete='CASCADE'), index=True)
    app_name  = Column(String(100), nullable=True, index=True)
    protocol  = Column(String(20), nullable=False, index=True)  # 'openvpn' | 'shadowsocks'

    country      = Column(String(10), nullable=True, index=True)
    asn          = Column(String(50), nullable=True, index=True)
    network_type = Column(String(20), nullable=True)

    success_count  = Column(Integer, default=0)
    failure_count  = Column(Integer, default=0)
    total_attempts = Column(Integer, default=0)
    success_rate   = Column(Float, default=0.0)

    avg_connect_time_ms  = Column(Float, default=0.0)
    last_connect_time_ms = Column(Float, nullable=True)

    last_failure_reason  = Column(String(500), nullable=True)
    last_failure_time    = Column(DateTime(timezone=True), nullable=True)
    consecutive_failures = Column(Integer, default=0)

    cooldown_until = Column(DateTime(timezone=True), nullable=True)
    cooldown_level = Column(String(20), default='none')

    created_at    = Column(DateTime(timezone=True), server_default=func.now())
    updated_at    = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    last_success_at = Column(DateTime(timezone=True), nullable=True)

    server = relationship("VPNServer", back_populates="protocol_metrics")

    __table_args__ = (
        Index('idx_metrics_lookup', 'app_name', 'protocol', 'country', 'asn'),
        Index('idx_server_protocol', 'server_id', 'protocol'),
        Index('idx_cooldown', 'cooldown_until'),
    )


class CountryPolicy(Base):
    """Country-level protocol preferences. Global across all apps and servers."""
    __tablename__ = "country_policies"

    id      = Column(Integer, primary_key=True, index=True)
    app_name = Column(String(100), nullable=True, index=True)
    country = Column(String(10), nullable=False, index=True)

    preferred_protocol  = Column(String(20), nullable=True)
    fallback_protocol   = Column(String(20), nullable=True)
    protocol_bias_score = Column(Float, default=0.0)

    notes     = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index('idx_country_app', 'country', 'app_name'),
    )


class GlobalSettings(Base):
    """Single-row table for global routing policy settings."""
    __tablename__ = "global_settings"

    id = Column(Integer, primary_key=True, default=1)

    protocol_mode = Column(String(20), default='auto')
    # 'auto' | 'force_openvpn' | 'force_shadowsocks'

    disable_new_connections          = Column(Boolean, default=False)
    enforce_country_policies         = Column(Boolean, default=True)
    enforce_isp_policies             = Column(Boolean, default=True)

    cooldown_soft_seconds            = Column(Integer, default=300)
    cooldown_medium_seconds          = Column(Integer, default=900)
    cooldown_hard_seconds            = Column(Integer, default=3600)

    failure_rate_threshold                = Column(Float, default=10.0)
    cooldown_country_block_asn_threshold  = Column(Integer, default=3)

    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class ISPPolicy(Base):
    """ISP (ASN) level protocol policies. Global across all apps and servers."""
    __tablename__ = "isp_policies"

    id       = Column(Integer, primary_key=True, index=True)
    app_name = Column(String(100), nullable=True, index=True)
    country  = Column(String(10), nullable=False)
    asn      = Column(String(50), nullable=False, index=True)
    protocol = Column(String(20), nullable=False)

    status     = Column(String(20), default='normal')  # preferred, degraded, blocked
    bias_score = Column(Float, default=0.0)

    expiry = Column(DateTime(timezone=True), nullable=True)
    notes  = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index('idx_isp_protocol', 'asn', 'protocol', 'app_name'),
    )


class AuditLog(Base):
    """Audit trail for all write operations."""
    __tablename__ = "audit_logs"

    id         = Column(Integer, primary_key=True, index=True)
    user_id    = Column(Integer, nullable=True, index=True)
    user_email = Column(String(255), nullable=True)
    user_role  = Column(String(20), nullable=True)

    action        = Column(String(50), nullable=False, index=True)
    resource_type = Column(String(50), nullable=False, index=True)
    resource_id   = Column(String(100), nullable=True)
    app_name      = Column(String(100), nullable=True, index=True)

    details   = Column(Text, nullable=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), index=True)


class Notification(Base):
    """System-generated notifications for server_down / capacity_reached events."""
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True, index=True)
    type = Column(String(50), nullable=False, index=True)

    server_id = Column(Integer, ForeignKey('vpn_status_vpnserver.id', ondelete='SET NULL'),
                       nullable=True, index=True)

    server_name = Column(String(100), nullable=True)
    server_ip   = Column(String(45),  nullable=True)
    app_name    = Column(String(100), nullable=True)
    message     = Column(String(500), nullable=False)

    is_read    = Column(Boolean, default=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)