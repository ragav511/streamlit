import psycopg2
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import shutil
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class DualDatabaseManager:
    def __init__(self):
        self.pg_config = {
            'dbname': os.getenv("DB_NAME"),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'host': os.getenv("DB_HOST"),
            'port': os.getenv("DB_PORT")
        }
        
        # Setup paths
        self.desktop_path = os.path.join(os.path.expanduser("~"), "Desktop", "BOQ_Backups")
        self.server_path = r"\\192.168.1.8\backup\BOQ_Backups"
        
        # Create only the desktop directory
        os.makedirs(self.desktop_path, exist_ok=True)
    
    def _create_server_directory(self):
        """Create server directory with authentication"""
        try:
            # Try to create server directory
            if not os.path.exists(self.server_path):
                os.makedirs(self.server_path, exist_ok=True)
            logger.info("✅ Server directory accessible")
        except Exception as e:
            logger.warning(f"⚠️ Server directory not accessible: {e}")
    
    def get_connection(self):
        """Get PostgreSQL connection"""
        return psycopg2.connect(**self.pg_config)
    
    def save_to_excel(self, table_name, data, columns=None):
        """Save data to Excel files on both desktop and server"""
        if not data:
            logger.warning(f"No data to save for {table_name}")
            return
        
        try:
            # Create DataFrame
            if columns:
                df = pd.DataFrame(data, columns=columns)
            else:
                df = pd.DataFrame(data)
            
            # Get today's date for filename
            today = datetime.now().strftime("%Y-%m-%d")
            filename = f"{table_name}_{today}.xlsx"
            
            # Save to desktop
            desktop_file = os.path.join(self.desktop_path, filename)
            df.to_excel(desktop_file, index=False)
            logger.info(f"✅ Saved {filename} to desktop")
            
            # Check and create server directory, then save if accessible
            if os.path.exists(self.server_path):
                self._create_server_directory()
                server_file = os.path.join(self.server_path, filename)
                df.to_excel(server_file, index=False)
                logger.info(f"✅ Saved {filename} to server")
            else:
                logger.warning(f"⚠️ Server path {self.server_path} is offline or inaccessible, skipping server backup")
                
        except Exception as e:
            logger.error(f"❌ Error creating Excel file for {table_name}: {e}")
    
    def backup_table(self, table_name, custom_query=None):
        """Backup a complete table to Excel"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Use custom query or default SELECT ALL
            if custom_query:
                cursor.execute(custom_query)
            else:
                cursor.execute(f"SELECT * FROM {table_name}")
            
            # Get data and column names
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            # Save to Excel
            self.save_to_excel(table_name, data, columns)
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ Error backing up {table_name}: {e}")
    
    def backup_all_tables(self):
        """Backup all main tables"""
        tables = [
            'projects',
            'boq_items', 
            'suppliers',
            'bill_to_companies',
            'ship_to_addresses',
            'locations',
            'po_counters'
        ]
        
        logger.info("🔄 Starting full backup...")
        for table in tables:
            self.backup_table(table)
        logger.info("✅ Full backup completed!")
    
    def backup_project_data(self, project_id):
        """Backup specific project data"""
        custom_query = f"SELECT * FROM boq_items WHERE project_id = {project_id}"
        self.backup_table(f"project_{project_id}_boq_items", custom_query)
    
    def execute_with_backup(self, query, params=None, table_name=None):
        """Execute query and automatically backup affected table"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Execute the query
            cursor.execute(query, params)
            conn.commit()
            
            # If it's an INSERT/UPDATE/DELETE and table specified, backup
            if table_name and any(keyword in query.upper() for keyword in ['INSERT', 'UPDATE', 'DELETE']):
                self.backup_table(table_name)
            
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Query execution failed: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

# Create global instance
db_manager = DualDatabaseManager()

def get_connection():
    """Legacy function for backward compatibility"""
    return db_manager.get_connection()

def save_project_data(project_id, project_name, boq_data):
    """Save project data with automatic Excel backup"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Insert project
        cursor.execute("INSERT INTO projects (name) VALUES (%s) RETURNING id", (project_name,))
        project_id = cursor.fetchone()[0]
        
        # Insert BOQ items
        for item in boq_data:
            cursor.execute("""
                INSERT INTO boq_items (
                    project_id, boq_ref, description, make, model, unit, boq_qty, rate, amount,
                    delivered_qty_1, delivered_qty_2, delivered_qty_3, delivered_qty_4, delivered_qty_5,
                    delivered_qty_6, delivered_qty_7, delivered_qty_8, delivered_qty_9, delivered_qty_10,
                    total_delivery_qty, balance_to_deliver
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, item)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Automatically backup affected tables
        db_manager.backup_table('projects')
        db_manager.backup_table('boq_items')
        
        logger.info(f"✅ Project '{project_name}' saved and backed up")
        return project_id
        
    except Exception as e:
        logger.error(f"❌ Error saving project: {e}")
        return None

def save_supplier_data(supplier_data):
    """Save supplier with automatic backup"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO suppliers (name, address, gst_number, contact_person, contact_number)
            VALUES (%s, %s, %s, %s, %s)
        """, supplier_data)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Automatically backup suppliers table
        db_manager.backup_table('suppliers')
        
        logger.info("✅ Supplier saved and backed up")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving supplier: {e}")
        return False

def save_purchase_order_data(po_data):
    """Save purchase order with automatic backup"""
    try:
        # Save PO logic here (your existing code)
        # After successful save:
        
        # Backup all affected tables
        db_manager.backup_table('boq_items')  # Updated delivery quantities
        
        # Create PO summary for Excel
        po_summary = [{
            'PO_Number': po_data.get('po_number'),
            'Date': po_data.get('po_date'),
            'Supplier': po_data.get('supplier_name'),
            'Total_Amount': po_data.get('total_amount'),
            'Created_At': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }]
        
        db_manager.save_to_excel('purchase_orders', po_summary)
        
        logger.info("✅ Purchase Order saved and backed up")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving purchase order: {e}")
        return False

def manual_backup():
    """Manual backup function for testing"""
    db_manager.backup_all_tables()

def test_server_connection():
    """Test server connectivity"""
    try:
        test_file = os.path.join(db_manager.server_path, "connection_test.txt")
        with open(test_file, 'w') as f:
            f.write(f"Connection test successful at {datetime.now()}")
        
        # Clean up test file
        if os.path.exists(test_file):
            os.remove(test_file)
            
        logger.info("✅ Server connection test successful")
        return True
        
    except Exception as e:
        logger.error(f"❌ Server connection test failed: {e}")
        return False

# Utility functions for easy access
def backup_now():
    """Quick backup function"""
    return db_manager.backup_all_tables()

def get_backup_status():
    """Get backup status information"""
    desktop_files = len([f for f in os.listdir(db_manager.desktop_path) if f.endswith('.xlsx')]) if os.path.exists(db_manager.desktop_path) else 0
    
    try:
        server_files = len([f for f in os.listdir(db_manager.server_path) if f.endswith('.xlsx')]) if os.path.exists(db_manager.server_path) else 0
        server_status = "✅ Connected"
    except:
        server_files = 0
        server_status = "❌ Not Connected"
    
    return {
        'desktop_files': desktop_files,
        'server_files': server_files,
        'server_status': server_status,
        'last_backup': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }