from flask import Flask, request, jsonify
from flask_cors import CORS
import pymssql
import uuid
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Database configuration
def get_db_connection():
    server = os.getenv("DB_SERVER", "your-server.database.windows.net")
    database = os.getenv("DB_NAME", "FlowMeterDB")
    username = os.getenv("DB_USERNAME", "your-username")
    password = os.getenv("DB_PASSWORD", "your-password")
    
    try:
        conn = pymssql.connect(
            server=server,
            user=username,
            password=password,
            database=database,
            as_dict=True
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

def parse_float(value):
    """Parse string value to float, handle None and placeholder values"""
    if value is None or not isinstance(value, str):
        return None
    
    # Remove $$ placeholders if present
    cleaned_value = value.replace('$$', '').strip()
    
    if not cleaned_value:
        return None
    
    try:
        return float(cleaned_value)
    except (ValueError, TypeError):
        return None

@app.route('/api/flowmeter/upload', methods=['POST'])
def upload_data():
    """
    Receive data from SCADA HTTP uploader
    Expected JSON format with $$ placeholders
    """
    try:
        # Log raw request data for debugging
        raw_data = request.get_data(as_text=True)
        logger.info(f"Raw request data: {raw_data}")
        
        # Parse JSON data
        if not request.is_json:
            return jsonify({
                "success": False,
                "message": "Content-Type must be application/json"
            }), 400
        
        data = request.get_json()
        
        if not data:
            return jsonify({
                "success": False,
                "message": "No JSON data received"
            }), 400
        
        logger.info(f"Received data from SCADA: {json.dumps(data, indent=2)}")
        
        # Validate device ID
        device_id = data.get('deviceid')
        if not device_id:
            return jsonify({
                "success": False,
                "message": "Device ID is required"
            }), 400
        
        # Parse all flow meter values
        parsed_data = {
            'deviceid': device_id,
            'FT1MassFlow': parse_float(data.get('FT1MassFlow')),
            'FT1Masstotal': parse_float(data.get('FT1Masstotal')),
            'FT1VolumeFlow': parse_float(data.get('FT1VolumeFlow')),
            'FT1Volumetotal': parse_float(data.get('FT1Volumetotal')),
            'FT1Temp': parse_float(data.get('FT1Temp')),
            'FT1Density': parse_float(data.get('FT1Density')),
            'FT2MassFlow': parse_float(data.get('FT2MassFlow')),
            'FT2Masstotal': parse_float(data.get('FT2Masstotal')),
            'FT2VolumeFlow': parse_float(data.get('FT2VolumeFlow')),
            'FT2Volumetotal': parse_float(data.get('FT2Volumetotal')),
            'FT2Temp': parse_float(data.get('FT2Temp')),
            'FT2Density': parse_float(data.get('FT2Density')),
            'FT3MassFlow': parse_float(data.get('FT3MassFlow')),
            'FT3Masstotal': parse_float(data.get('FT3Masstotal')),
            'FT3VolumeFlow': parse_float(data.get('FT3VolumeFlow')),
            'FT3Volumetotal': parse_float(data.get('FT3Volumetotal')),
            'FT3Temp': parse_float(data.get('FT3Temp')),
            'FT3Density': parse_float(data.get('FT3Density')),
            'FT4MassFlow': parse_float(data.get('FT4MassFlow')),
            'FT4Masstotal': parse_float(data.get('FT4Masstotal')),
            'FT4VolumeFlow': parse_float(data.get('FT4VolumeFlow')),
            'FT4Volumetotal': parse_float(data.get('FT4Volumetotal')),
            'FT4Temp': parse_float(data.get('FT4Temp')),
            'FT4Density': parse_float(data.get('FT4Density')),
            'FT5MassFlow': parse_float(data.get('FT5MassFlow')),
            'FT5Masstotal': parse_float(data.get('FT5Masstotal')),
            'FT5VolumeFlow': parse_float(data.get('FT5VolumeFlow')),
            'FT5Volumetotal': parse_float(data.get('FT5Volumetotal')),
            'FT5Temp': parse_float(data.get('FT5Temp')),
            'FT5Density': parse_float(data.get('FT5Density')),
            'FT6MassFlow': parse_float(data.get('FT6MassFlow')),
            'FT6Masstotal': parse_float(data.get('FT6Masstotal')),
            'FT6VolumeFlow': parse_float(data.get('FT6VolumeFlow')),
            'FT6Volumetotal': parse_float(data.get('FT6Volumetotal')),
            'FT6Temp': parse_float(data.get('FT6Temp')),
            'FT6Density': parse_float(data.get('FT6Density')),
            'FT7MassFlow': parse_float(data.get('FT7MassFlow')),
            'FT7Masstotal': parse_float(data.get('FT7Masstotal')),
            'FT7VolumeFlow': parse_float(data.get('FT7VolumeFlow')),
            'FT7Volumetotal': parse_float(data.get('FT7Volumetotal')),
            'FT7Temp': parse_float(data.get('FT7Temp')),
            'FT7Density': parse_float(data.get('FT7Density')),
            'FT8MassFlow': parse_float(data.get('FT8MassFlow')),
            'FT8Masstotal': parse_float(data.get('FT8Masstotal')),
            'FT8VolumeFlow': parse_float(data.get('FT8VolumeFlow')),
            'FT8Volumetotal': parse_float(data.get('FT8Volumetotal')),
            'FT8Temp': parse_float(data.get('FT8Temp')),
            'FT8Density': parse_float(data.get('FT8Density')),
            'FT9MassFlow': parse_float(data.get('FT9MassFlow')),
            'FT9Masstotal': parse_float(data.get('FT9Masstotal')),
            'FT9VolumeFlow': parse_float(data.get('FT9VolumeFlow')),
            'FT9Volumetotal': parse_float(data.get('FT9Volumetotal')),
            'FT9Temp': parse_float(data.get('FT9Temp')),
            'FT9Density': parse_float(data.get('FT9Density'))
        }
        
        # Save to database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        record_id = str(uuid.uuid4())
        
        insert_query = """
        INSERT INTO FlowMeterData (
            Id, DeviceId, FT1MassFlow, FT1Masstotal, FT1VolumeFlow, FT1Volumetotal, FT1Temp, FT1Density,
            FT2MassFlow, FT2Masstotal, FT2VolumeFlow, FT2Volumetotal, FT2Temp, FT2Density,
            FT3MassFlow, FT3Masstotal, FT3VolumeFlow, FT3Volumetotal, FT3Temp, FT3Density,
            FT4MassFlow, FT4Masstotal, FT4VolumeFlow, FT4Volumetotal, FT4Temp, FT4Density,
            FT5MassFlow, FT5Masstotal, FT5VolumeFlow, FT5Volumetotal, FT5Temp, FT5Density,
            FT6MassFlow, FT6Masstotal, FT6VolumeFlow, FT6Volumetotal, FT6Temp, FT6Density,
            FT7MassFlow, FT7Masstotal, FT7VolumeFlow, FT7Volumetotal, FT7Temp, FT7Density,
            FT8MassFlow, FT8Masstotal, FT8VolumeFlow, FT8Volumetotal, FT8Temp, FT8Density,
            FT9MassFlow, FT9Masstotal, FT9VolumeFlow, FT9Volumetotal, FT9Temp, FT9Density
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            record_id, parsed_data['deviceid'],
            parsed_data['FT1MassFlow'], parsed_data['FT1Masstotal'],
            parsed_data['FT1VolumeFlow'], parsed_data['FT1Volumetotal'],
            parsed_data['FT1Temp'], parsed_data['FT1Density'],
            parsed_data['FT2MassFlow'], parsed_data['FT2Masstotal'],
            parsed_data['FT2VolumeFlow'], parsed_data['FT2Volumetotal'],
            parsed_data['FT2Temp'], parsed_data['FT2Density'],
            parsed_data['FT3MassFlow'], parsed_data['FT3Masstotal'],
            parsed_data['FT3VolumeFlow'], parsed_data['FT3Volumetotal'],
            parsed_data['FT3Temp'], parsed_data['FT3Density'],
            parsed_data['FT4MassFlow'], parsed_data['FT4Masstotal'],
            parsed_data['FT4VolumeFlow'], parsed_data['FT4Volumetotal'],
            parsed_data['FT4Temp'], parsed_data['FT4Density'],
            parsed_data['FT5MassFlow'], parsed_data['FT5Masstotal'],
            parsed_data['FT5VolumeFlow'], parsed_data['FT5Volumetotal'],
            parsed_data['FT5Temp'], parsed_data['FT5Density'],
            parsed_data['FT6MassFlow'], parsed_data['FT6Masstotal'],
            parsed_data['FT6VolumeFlow'], parsed_data['FT6Volumetotal'],
            parsed_data['FT6Temp'], parsed_data['FT6Density'],
            parsed_data['FT7MassFlow'], parsed_data['FT7Masstotal'],
            parsed_data['FT7VolumeFlow'], parsed_data['FT7Volumetotal'],
            parsed_data['FT7Temp'], parsed_data['FT7Density'],
            parsed_data['FT8MassFlow'], parsed_data['FT8Masstotal'],
            parsed_data['FT8VolumeFlow'], parsed_data['FT8Volumetotal'],
            parsed_data['FT8Temp'], parsed_data['FT8Density'],
            parsed_data['FT9MassFlow'], parsed_data['FT9Masstotal'],
            parsed_data['FT9VolumeFlow'], parsed_data['FT9Volumetotal'],
            parsed_data['FT9Temp'], parsed_data['FT9Density']
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Data saved successfully for device: {device_id}, Record ID: {record_id}")
        
        return jsonify({
            "success": True,
            "message": "Data received and stored successfully",
            "id": record_id,
            "device_id": device_id,
            "timestamp": datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing SCADA data: {str(e)}")
        return jsonify({
            "success": False,
            "message": "Internal server error"
        }), 500

@app.route('/api/flowmeter/data', methods=['GET'])
def get_data():
    """Retrieve historical flow meter data with filtering"""
    try:
        # Get query parameters
        device_id = request.args.get('device_id', type=str)
        start_date = request.args.get('start_date', type=str)
        end_date = request.args.get('end_date', type=str)
        page = request.args.get('page', 1, type=int)
        page_size = request.args.get('page_size', 100, type=int)
        
        # Validate pagination
        if page < 1:
            page = 1
        if page_size < 1 or page_size > 1000:
            page_size = 100
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build WHERE clause
        where_conditions = []
        params = []
        
        if device_id:
            where_conditions.append("DeviceId = %s")
            params.append(device_id)
        
        if start_date:
            try:
                start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                where_conditions.append("CreatedAt >= %s")
                params.append(start_dt)
            except ValueError:
                return jsonify({
                    "success": False,
                    "message": "Invalid start_date format. Use ISO format."
                }), 400
        
        if end_date:
            try:
                end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                where_conditions.append("CreatedAt <= %s")
                params.append(end_dt)
            except ValueError:
                return jsonify({
                    "success": False,
                    "message": "Invalid end_date format. Use ISO format."
                }), 400
        
        where_clause = " AND ".join(where_conditions)
        if where_clause:
            where_clause = "WHERE " + where_clause
        
        # Count total records
        count_query = f"SELECT COUNT(*) as count FROM FlowMeterData {where_clause}"
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()['count']
        
        # Get paginated data
        offset = (page - 1) * page_size
        data_query = f"""
            SELECT * FROM FlowMeterData 
            {where_clause}
            ORDER BY CreatedAt DESC 
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """
        
        cursor.execute(data_query, params + [offset, page_size])
        data = cursor.fetchall()
        
        # Convert data types for JSON serialization
        for item in data:
            item['Id'] = str(item['Id'])
            item['CreatedAt'] = item['CreatedAt'].isoformat()
        
        conn.close()
        
        return jsonify({
            "success": True,
            "totalCount": total_count,
            "page": page,
            "pageSize": page_size,
            "data": data
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving data: {str(e)}")
        return jsonify({
            "success": False,
            "message": "Error retrieving data"
        }), 500

# Add other endpoints similarly...

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        conn.close()
        
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "database": "connected"
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "database": "disconnected",
            "error": str(e)
        }), 500

@app.route('/', methods=['GET'])
def home():
    """Home page with API information"""
    return jsonify({
        "message": "SCADA Flow Meter API",
        "version": "1.0.0",
        "endpoints": {
            "upload_data": "POST /api/flowmeter/upload",
            "get_data": "GET /api/flowmeter/data",
            "get_stats": "GET /api/flowmeter/stats",
            "get_latest": "GET /api/flowmeter/latest",
            "health_check": "GET /health"
        }
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)