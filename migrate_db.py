#!/usr/bin/env python3
"""
Simple Database Migration Script
Applies new tables and columns without Alembic
Run this after updating models.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, inspect, text
from app.config import settings
from app.models import Base, VPNServer, ProtocolMetrics, CountryPolicy, ISPPolicy, App, GlobalSettings
from app.database import sync_engine


def check_column_exists(engine, table_name, column_name):
    """Check if a column exists in a table"""
    inspector = inspect(engine)
    columns = [col['name'] for col in inspector.get_columns(table_name)]
    return column_name in columns


def check_table_exists(engine, table_name):
    """Check if a table exists"""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


def migrate():
    """Run migrations"""
    engine = sync_engine
    
    print("=" * 60)
    print("🚀 VPN Load Balancer - Database Migration")
    print("=" * 60)
    print()
    
    try:
        with engine.begin() as conn:
            # Step 1: Add new columns to existing vpn_status_vpnserver table
            print("📋 Step 1: Checking VPNServer table...")
            
            if check_table_exists(engine, 'vpn_status_vpnserver'):
                print("   ✅ Table 'vpn_status_vpnserver' exists")
                
                # Add protocol column
                if not check_column_exists(engine, 'vpn_status_vpnserver', 'protocol'):
                    print("   ➕ Adding 'protocol' column...")
                    conn.execute(text("""
                        ALTER TABLE vpn_status_vpnserver 
                        ADD COLUMN protocol VARCHAR(20) DEFAULT 'openvpn'
                    """))
                    conn.execute(text("""
                        CREATE INDEX idx_protocol_app ON vpn_status_vpnserver(protocol, app_name)
                    """))
                    print("      ✅ Added 'protocol' column with index")
                else:
                    print("      ⏭️  Column 'protocol' already exists")
                
                # Add Shadowsocks fields
                shadowsocks_columns = [
                    ('ss_port', 'INTEGER'),
                    ('ss_password', 'VARCHAR(255)'),
                    ('ss_encryption', 'VARCHAR(50)'),
                    ('server_country', 'VARCHAR(100)')
                ]
                
                for col_name, col_type in shadowsocks_columns:
                    if not check_column_exists(engine, 'vpn_status_vpnserver', col_name):
                        print(f"   ➕ Adding '{col_name}' column...")
                        conn.execute(text(f"""
                            ALTER TABLE vpn_status_vpnserver 
                            ADD COLUMN {col_name} {col_type}
                        """))
                        print(f"      ✅ Added '{col_name}' column")
                    else:
                        print(f"      ⏭️  Column '{col_name}' already exists")
            else:
                print("   ⚠️  Table 'vpn_status_vpnserver' does not exist, will create")
            
            # Step 2: Add protocol column to sessions table
            print("\n📋 Step 2: Checking VPNUserSession table...")
            
            if check_table_exists(engine, 'vpn_status_vpnusersession'):
                print("   ✅ Table 'vpn_status_vpnusersession' exists")
                
                if not check_column_exists(engine, 'vpn_status_vpnusersession', 'protocol'):
                    print("   ➕ Adding 'protocol' column...")
                    conn.execute(text("""
                        ALTER TABLE vpn_status_vpnusersession 
                        ADD COLUMN protocol VARCHAR(20) DEFAULT 'openvpn'
                    """))
                    conn.execute(text("""
                        CREATE INDEX idx_protocol ON vpn_status_vpnusersession(protocol)
                    """))
                    print("      ✅ Added 'protocol' column with index")
                else:
                    print("      ⏭️  Column 'protocol' already exists")
            else:
                print("   ⚠️  Table 'vpn_status_vpnusersession' does not exist, will create")
            
            # Step 3: Create new tables
            print("\n📋 Step 3: Creating new tables...")
            
            # Protocol Metrics table
            if not check_table_exists(engine, 'protocol_metrics'):
                print("   ➕ Creating 'protocol_metrics' table...")
                Base.metadata.tables['protocol_metrics'].create(engine)
                print("      ✅ Created 'protocol_metrics' table")
            else:
                print("      ⏭️  Table 'protocol_metrics' already exists")
            
            # Country Policies table
            if not check_table_exists(engine, 'country_policies'):
                print("   ➕ Creating 'country_policies' table...")
                Base.metadata.tables['country_policies'].create(engine)
                print("      ✅ Created 'country_policies' table")
            else:
                print("      ⏭️  Table 'country_policies' already exists")
            
            # ISP Policies table
            if not check_table_exists(engine, 'isp_policies'):
                print("   ➕ Creating 'isp_policies' table...")
                Base.metadata.tables['isp_policies'].create(engine)
                print("      ✅ Created 'isp_policies' table")
            else:
                print("      ⏭️  Table 'isp_policies' already exists")

            # Apps table
            if not check_table_exists(engine, 'apps'):
                print("   ➕ Creating 'apps' table...")
                Base.metadata.tables['apps'].create(engine)
                print("      ✅ Created 'apps' table")
            else:
                print("      ⏭️  Table 'apps' already exists")

            # Global Settings table
            if not check_table_exists(engine, 'global_settings'):
                print("   ➕ Creating 'global_settings' table...")
                Base.metadata.tables['global_settings'].create(engine)
                conn.execute(text("""
                    INSERT INTO global_settings (
                        id, protocol_mode, disable_new_connections,
                        enforce_country_policies, enforce_isp_policies,
                        cooldown_soft_seconds,
                        cooldown_medium_seconds, cooldown_hard_seconds,
                        failure_rate_threshold
                    ) VALUES (
                        1, 'auto', 0,
                        1, 1, 300,
                        900, 3600,
                        10.0
                    )
                """))
                print("      ✅ Created 'global_settings' table with default row")
            else:
                print("      ⏭️  Table 'global_settings' already exists")
                # Add enforce_isp_policies column if it was added after initial migration
                if not check_column_exists(engine, 'global_settings', 'enforce_isp_policies'):
                    print("   ➕ Adding 'enforce_isp_policies' column to 'global_settings'...")
                    conn.execute(text("""
                        ALTER TABLE global_settings
                        ADD COLUMN enforce_isp_policies BOOLEAN DEFAULT 1
                    """))
                    print("      ✅ Added 'enforce_isp_policies' column")
            
            print()
            print("=" * 60)
            print("✨ Migration completed successfully!")
            print("=" * 60)
            print()
            print("Next steps:")
            print("1. Restart your FastAPI server")
            print("2. Restart Celery workers")
            print("3. Add Shadowsocks servers via /admin/shadowsocks/servers")
            print("4. Test decision engine via /v2/best_server/")
            print("5. Manage global routing settings via /admin/settings/")
            print()
    
    except Exception as e:
        print()
        print("=" * 60)
        print(f"❌ Migration failed: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    migrate()