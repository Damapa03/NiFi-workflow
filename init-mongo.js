db = db.getSiblingDB('NiFiworkflow');

db.createCollection('Sensors_historic');
db.createCollection('Sensors_lastupdate');

print("Base de datos NiFiworkflow y colecciones creadas con éxito.");