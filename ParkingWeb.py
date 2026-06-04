from flask import Flask, render_template_string, jsonify
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

app = Flask(__name__)

# MongoDB connection setup
try:
    client = MongoClient('mongodb+srv://danielmarinpachecoalu_db_user:VbQH99nDlwuElgTF@cluster0.2nypmso.mongodb.net/?appName=Cluster0')
    client.admin.command('ismaster')
    db = client['ProyectoUD1']
    collection = db['Sensors_lastupdate']
    print("MongoDB connection successful.")
except Exception as e:
    print(f"Could not connect to MongoDB: {e}")
    client = None
    db = None
    collection = None

def get_latest_parking_status_from_mongo():
    """
    Fetches all documents from Sensors_lastupdate collection.
    Each document represents the latest status for a unique bay_id.
    """
    if collection is None:
        print("MongoDB collection not available. Returning empty data.")
        return {}

    latest_statuses = {}
    try:
        # Simple find() query since each bay_id has only one document
        results = collection.find({})
        
        for doc in results:
            bay_id = doc.get('bay_id')
            if bay_id:
                latest_statuses[bay_id] = {
                    'occupied': doc.get('state', False),
                    'temp': doc.get('temperature', 0),
                    'battery': doc.get('battery_pct', 0)
                }
        
        print(f"Retrieved {len(latest_statuses)} parking spots from MongoDB")
    except Exception as e:
        print(f"Error querying MongoDB: {e}")
    
    return latest_statuses

@app.route('/status')
def get_status():
    """Provides the latest parking status as JSON."""
    latest_statuses = get_latest_parking_status_from_mongo()
    all_spot_ids = ['A1','A2','A3','A4','A5','B1','B2','B3','B4','B5']
    
    spots_data = {}
    for spot_id in all_spot_ids:
        data = latest_statuses.get(spot_id, {'occupied': False, 'temp': 0, 'battery': 0})
        spots_data[spot_id] = {
            'status': 'occupied' if data.get('occupied', False) else 'free',
            'temp': data.get('temp', 0),
            'battery': data.get('battery', 0)
        }
    return jsonify(spots_data)

@app.route('/')
def home():
    latest_statuses = get_latest_parking_status_from_mongo()

    spots = []
    all_spot_ids = ['A1','A2','A3','A4','A5','B1','B2','B3','B4','B5']
    
    for spot_id in all_spot_ids:
        data = latest_statuses.get(spot_id, {'occupied': False, 'temp': 0, 'battery': 0})
        spots.append({
            'label': spot_id,
            'status': 'occupied' if data.get('occupied', False) else 'free',
            'temp': data.get('temp', 0),
            'battery': data.get('battery', 0)
        })
    
    html = """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <title>Parking Web</title>
        <style>
            body { background: #f4f4f4; font-family: Arial, sans-serif; margin: 0; padding: 0; }
            .container { max-width: 600px; margin: 40px auto; background: #fff; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); padding: 30px 20px 40px 20px; text-align: center; }
            h1 { color: #2c3e50; }
            .parking-lot { display: grid; grid-template-columns: repeat(5, 60px); grid-gap: 18px; justify-content: center; margin: 30px 0 10px 0; }
            .parking-spot { width: 60px; height: 120px; background: #e0e0e0; border: 2px solid #b0b0b0; border-radius: 8px; position: relative; display: flex; flex-direction: column; align-items: center; justify-content: center; font-weight: bold; color: #555; transition: background 0.3s, border-color 0.3s; }
            .parking-spot.occupied { background: #e74c3c; color: #fff; border-color: #c0392b; }
            .parking-spot.free { background: #2ecc40; color: #fff; border-color: #27ae60; }
            .spot-label { position: absolute; bottom: 6px; left: 0; width: 100%; font-size: 14px; text-align: center; opacity: 0.8; }
            .spot-info { font-size: 13px; margin-top: 10px; color: #222; }
            .legend { margin-top: 20px; display: flex; justify-content: center; gap: 30px; }
            .legend-item { display: flex; align-items: center; gap: 8px; font-size: 15px; }
            .legend-color { width: 22px; height: 22px; border-radius: 4px; display: inline-block; border: 2px solid #b0b0b0; }
            .legend-free { background: #2ecc40; border-color: #27ae60;}
            .legend-occupied { background: #e74c3c; border-color: #c0392b;}
            .status-indicator { position: fixed; top: 10px; right: 10px; padding: 8px 15px; background: #27ae60; color: white; border-radius: 5px; font-size: 14px; }
            .status-indicator.updating { background: #f39c12; }
        </style>
    </head>
    <body>
        <div class="status-indicator" id="statusIndicator">● En línea</div>
        <div class="container">
            <h1>Mapa del Parking</h1>
            <div class="parking-lot">
                {% for spot in spots %}
                <div class="parking-spot {{ spot.status }}" data-spot-id="{{ spot.label }}">
                    <div class="spot-info">T: {{ spot.temp }}°C<br>B: {{ spot.battery }}%</div>
                    <span class="spot-label">{{ spot.label }}</span>
                </div>
                {% endfor %}
            </div>
            <div class="legend">
                <div class="legend-item"><span class="legend-color legend-free"></span> Libre</div>
                <div class="legend-item"><span class="legend-color legend-occupied"></span> Ocupado</div>
            </div>
        </div>
        
        <script>
            const statusIndicator = document.getElementById('statusIndicator');
            
            function refreshParkingStatus() {
                // Cambiar indicador a "actualizando"
                statusIndicator.textContent = '● Actualizando...';
                statusIndicator.classList.add('updating');
                
                fetch('/status')
                    .then(response => response.json())
                    .then(data => {
                        // Actualizar cada plaza de parking
                        Object.keys(data).forEach(spotId => {
                            const spotData = data[spotId];
                            const spotElement = document.querySelector(`[data-spot-id="${spotId}"]`);
                            
                            if (spotElement) {
                                // Actualizar clase (occupied/free)
                                spotElement.className = 'parking-spot ' + spotData.status;
                                
                                // Actualizar información (temperatura y batería)
                                const infoElement = spotElement.querySelector('.spot-info');
                                infoElement.innerHTML = `T: ${spotData.temp}°C<br>B: ${spotData.battery}%`;
                            }
                        });
                        
                        // Indicador de éxito
                        statusIndicator.textContent = '● En línea';
                        statusIndicator.classList.remove('updating');
                    })
                    .catch(error => {
                        console.error('Error fetching parking status:', error);
                        statusIndicator.textContent = '● Error de conexión';
                        statusIndicator.style.background = '#e74c3c';
                    });
            }
            
            // Actualizar cada 5 segundos (5000 milisegundos)
            // Puedes cambiar este valor según necesites: 3000 = 3 segundos, 10000 = 10 segundos
            setInterval(refreshParkingStatus, 5000);
            
            // Primera actualización después de 2 segundos de cargar la página
            setTimeout(refreshParkingStatus, 2000);
        </script>
    </body>
    </html>
    """
    return render_template_string(html, spots=spots)

if __name__ == '__main__':
    app.run(debug=True)