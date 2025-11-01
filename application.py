from flask import Flask, request, jsonify
from flask_cors import CORS
import pyodbc
import uuid
from datetime import datetime
import logging
import os
import json

# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Database configuration for Azure Web App Production
def get_db_connection():
    # Get from Azure Application Settings
    server = os.environ["DB_SERVER"]
    database = os.environ["DB_NAME"]
    username = os.environ["DB_USERNAME"]
    password = os.environ["DB_PASSWORD"]
    
    # Production connection string
    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )
    
    try:
        conn = pyodbc.connect(connection_string)
        logger.info("Production: Connected to Azure SQL Database")
        return conn
    except Exception as e:
        logger.error(f"Production: Database connection failed: {str(e)}")
        raise

def init_database():
    """Initialize database tables in production"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        create_table_query = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FlowMeterData' AND xtype='U')
        CREATE TABLE FlowMeterData (
            Id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
            DeviceId NVARCHAR(50) NOT NULL,
            FT1MassFlow DECIMAL(18,4),
            FT1Masstotal DECIMAL(18,4),
            FT1VolumeFlow DECIMAL(18,4),
            FT1Volumetotal DECIMAL(18,4),
            FT1Temp DECIMAL(18,4),
            FT1Density DECIMAL(18,4),
            FT2MassFlow DECIMAL(18,4),
            FT2Masstotal DECIMAL(18,4),
            FT2VolumeFlow DECIMAL(18,4),
            FT2Volumetotal DECIMAL(18,4),
            FT2Temp DECIMAL(18,4),
            FT2Density DECIMAL(18,4),
            FT3MassFlow DECIMAL(18,4),
            FT3Masstotal DECIMAL(18,4),
            FT3VolumeFlow DECIMAL(18,4),
            FT3Volumetotal DECIMAL(18,4),
            FT3Temp DECIMAL(18,4),
            FT3Density DECIMAL(18,4),
            FT4MassFlow DECIMAL(18,4),
            FT4Masstotal DECIMAL(18,4),
            FT4VolumeFlow DECIMAL(18,4),
            FT4Volumetotal DECIMAL(18,4),
            FT4Temp DECIMAL(18,4),
            FT4Density DECIMAL(18,4),
            FT5MassFlow DECIMAL(18,4),
            FT5Masstotal DECIMAL(18,4),
            FT5VolumeFlow DECIMAL(18,4),
            FT5Volumetotal DECIMAL(18,4),
            FT5Temp DECIMAL(18,4),
            FT5Density DECIMAL(18,4),
            FT6MassFlow DECIMAL(18,4),
            FT6Masstotal DECIMAL(18,4),
            FT6VolumeFlow DECIMAL(18,4),
            FT6Volumetotal DECIMAL(18,4),
            FT6Temp DECIMAL(18,4),
            FT6Density DECIMAL(18,4),
            FT7MassFlow DECIMAL(18,4),
            FT7Masstotal DECIMAL(18,4),
            FT7VolumeFlow DECIMAL(18,4),
            FT7Volumetotal DECIMAL(18,4),
            FT7Temp DECIMAL(18,4),
            FT7Density DECIMAL(18,4),
            FT8MassFlow DECIMAL(18,4),
            FT8Masstotal DECIMAL(18,4),
            FT8VolumeFlow DECIMAL(18,4),
            FT8Volumetotal DECIMAL(18,4),
            FT8Temp DECIMAL(18,4),
            FT8Density DECIMAL(18,4),
            FT9MassFlow DECIMAL(18,4),
            FT9Masstotal DECIMAL(18,4),
            FT9VolumeFlow DECIMAL(18,4),
            FT9Volumetotal DECIMAL(18,4),
            FT9Temp DECIMAL(18,4),
            FT9Density DECIMAL(18,4),
            CreatedAt DATETIME2 DEFAULT GETUTCDATE()
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        conn.close()
        logger.info("Production: Database table initialized")
        
    except Exception as e:
        logger.error(f"Production: Database initialization failed: {str(e)}")

def parse_float(value):
    """Parse SCADA data values"""
    if value is None or not isinstance(value, str):
        return None
    cleaned_value = value.replace('$$', '').strip()
    if not cleaned_value:
        return None
    try:
        return float(cleaned_value)
    except (ValueError, TypeError):
        return None

@app.route('/api/flowmeter/upload', methods=['POST'])
def upload_data():
    """Production endpoint for SCADA data ingestion"""
    try:
        # Log request info for monitoring
        client_ip = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
        logger.info(f"Production: Data upload request from {client_ip}")
        
        if not request.is_json:
            return jsonify({"success": False, "message": "Content-Type must be application/json"}), 400
        
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "message": "No JSON data received"}), 400
        
        device_id = data.get('deviceid')
        if not device_id:
            return jsonify({"success": False, "message": "Device ID is required"}), 400
        
        # Parse all available SCADA fields
        parsed_data = {'deviceid': device_id}
        for i in range(1, 10):  # FT1 to FT9
            parsed_data[f'FT{i}MassFlow'] = parse_float(data.get(f'FT{i}MassFlow'))
            parsed_data[f'FT{i}Masstotal'] = parse_float(data.get(f'FT{i}Masstotal'))
            parsed_data[f'FT{i}VolumeFlow'] = parse_float(data.get(f'FT{i}VolumeFlow'))
            parsed_data[f'FT{i}Volumetotal'] = parse_float(data.get(f'FT{i}Volumetotal'))
            parsed_data[f'FT{i}Temp'] = parse_float(data.get(f'FT{i}Temp'))
            parsed_data[f'FT{i}Density'] = parse_float(data.get(f'FT{i}Density'))
        
        # Save to Azure SQL Database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build dynamic insert query for production
        columns = ['DeviceId']
        values = [device_id]
        placeholders = ['?']
        
        field_mapping = {}
        for i in range(1, 10):
            field_mapping.update({
                f'FT{i}MassFlow': f'FT{i}MassFlow',
                f'FT{i}Masstotal': f'FT{i}Masstotal',
                f'FT{i}VolumeFlow': f'FT{i}VolumeFlow',
                f'FT{i}Volumetotal': f'FT{i}Volumetotal',
                f'FT{i}Temp': f'FT{i}Temp',
                f'FT{i}Density': f'FT{i}Density'
            })
        
        for key, column in field_mapping.items():
            if parsed_data.get(key) is not None:
                columns.append(column)
                values.append(parsed_data[key])
                placeholders.append('?')
        
        insert_query = f"""
        INSERT INTO FlowMeterData ({', '.join(columns)})
        VALUES ({', '.join(placeholders)})
        """
        
        cursor.execute(insert_query, values)
        conn.commit()
        conn.close()
        
        logger.info(f"Production: Data stored successfully for device {device_id}")
        
        return jsonify({
            "success": True,
            "message": "Data stored successfully in Azure SQL Database",
            "device_id": device_id,
            "timestamp": datetime.utcnow().isoformat(),
            "environment": "production"
        }), 200
        
    except Exception as e:
        logger.error(f"Production: Error processing SCADA data: {str(e)}")
        return jsonify({
            "success": False, 
            "message": "Internal server error",
            "environment": "production"
        }), 500

@app.route('/api/flowmeter/data', methods=['GET'])
def get_data():
    """Production endpoint for data retrieval"""
    try:
        device_id = request.args.get('device_id')
        limit = min(int(request.args.get('limit', 100)), 1000)  # Limit for production safety
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if device_id:
            cursor.execute(
                'SELECT TOP (?) * FROM FlowMeterData WHERE DeviceId = ? ORDER BY CreatedAt DESC', 
                (limit, device_id)
            )
        else:
            cursor.execute('SELECT TOP (?) * FROM FlowMeterData ORDER BY CreatedAt DESC', (limit,))
        
        # Convert to JSON serializable format
        columns = [column[0] for column in cursor.description]
        data = []
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            if 'Id' in row_dict: 
                row_dict['Id'] = str(row_dict['Id'])
            if 'CreatedAt' in row_dict and row_dict['CreatedAt']: 
                row_dict['CreatedAt'] = row_dict['CreatedAt'].isoformat()
            data.append(row_dict)
        
        conn.close()
        
        return jsonify({
            "success": True, 
            "data": data,
            "count": len(data),
            "environment": "production"
        }), 200
        
    except Exception as e:
        logger.error(f"Production: Error retrieving data: {str(e)}")
        return jsonify({
            "success": False, 
            "message": "Error retrieving data",
            "environment": "production"
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Production health check endpoint"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        conn.close()
        
        return jsonify({
            "status": "healthy", 
            "database": "connected",
            "environment": "production",
            "timestamp": datetime.utcnow().isoformat(),
            "service": "Azure Web App"
        }), 200
    except Exception as e:
        logger.error(f"Production: Health check failed: {str(e)}")
        return jsonify({
            "status": "unhealthy", 
            "error": "Database connection failed",
            "environment": "production",
            "timestamp": datetime.utcnow().isoformat()
        }), 500

@app.route('/')
def home():
    """Production root endpoint"""
    return jsonify({
        "message": "SCADA Flow Meter API - Production",
        "status": "running",
        "version": "1.0.0",
        "environment": "production",
        "service": "Azure Web App",
        "endpoints": {
            "upload_data": "POST /api/flowmeter/upload",
            "get_data": "GET /api/flowmeter/data",
            "health_check": "GET /health"
        }
    })

# Initialize database when app starts in production
try:
    init_database()
except Exception as e:
    logger.error(f"Production: Failed to initialize database on startup: {str(e)}")

if __name__ == '__main__':
    # Production settings
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)
