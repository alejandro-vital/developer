import json
import os
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaException, Producer
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
# Configuración del consumidor
config = {
    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS_CONSUMER"),  # Reemplaz con la dirección y puerto de tu broker Kafka
    'group.id': 'pago_pedimento',       # Identificador único para el grupo de consumidores
    'auto.offset.reset': 'latest'  ,        # Puedes ajustar esto según tus necesidades
    'enable.auto.commit': False 
}

configproducer = {
    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS_PRODUCER"),  # Reemplaza con la dirección y puerto de tu broker Kafka
    'client.id': 'python-producer'
}

def logs_error(contenido, nombre_archivo):
    try:
        directorio_actual = os.getcwd()
        try:  
            os.mkdir(f'{directorio_actual}/logs') 
        except OSError as error:  
            pass
        directorio_actual = f'{directorio_actual}/logs/'
        ruta_completa = os.path.join(directorio_actual, nombre_archivo)
        with open(ruta_completa, 'w') as archivo:
            archivo.write(contenido)
        return True
    except Exception as e:
        print(f"Error al generar y guardar el archivo: {e}")
        return False

def establecer_conexion_mongodb():
    """
    Establece una conexión con MongoDB utilizando las variables de entorno.
    """
    try:
        # Variables de conexión
        load_dotenv()
        usuario = os.getenv("MONGO_USER")
        contrasena = os.getenv("MONGO_PWD")
        nombre_base_de_datos = os.getenv("MONGO_DATABASE")
        db = f'{os.getenv("MONGO_HOST")}:{os.getenv("MONGO_PORT")}'

        # Armado de cadena de conexión
        cadena_conexion = f'mongodb://{usuario}:{contrasena}@{db}/?directConnection=true&readPreference=primary&replicaSet=rs0&authSource={nombre_base_de_datos}'

        # Establecer conexión
        client = MongoClient(cadena_conexion)
        coleccion = client[nombre_base_de_datos]['pedimentos']
        print("\n[-----------------------------------------------------------------------------------]")
        print("_Conexión exitosa con MongoDB_\n")
        return coleccion
    except Exception as e:
        logs_error(f'Error al establecer conexión con MongoDB\n----------\n{str(e)}', f'logs_{datetime.now().strftime("%d-%m-%Y %H_%M_%S")}.err')
        print(f"Error al establecer conexión con MongoDB: {e}")
        return None

# Buscar en MongoDB
def buscar_en_mongo(patente_autorizacion, numero_pedimento, aduana_de_despacho, coleccion):
    """Busca un documento en MongoDB utilizando los filtros proporcionados."""
    filtros = {
        "inicio_pedimento.patente_autorizacion": patente_autorizacion,
        "inicio_pedimento.numero_pedimento": numero_pedimento,
        "inicio_pedimento.aduana_de_despacho": aduana_de_despacho
    }

    try:
        documento = coleccion.find_one(filtros, {'inicio_pedimento': 1, '_id': 1})
        
        if documento:
            print(f"Documento localizado con los datos: patente_autorizacion - {patente_autorizacion}, numero_pedimento - {numero_pedimento}, aduana_de_despacho - {aduana_de_despacho}")
            return documento
        else:
            print(f"Documento no encontradoco los datos: patente_autorizacion - {patente_autorizacion}, numero_pedimento - {numero_pedimento}, aduana_de_despacho - {aduana_de_despacho}")
            return None
    except Exception as e:
        logs_error(f'Error al buscar documento de MongoDB\n----------\n{str(e)}', f'logs_{datetime.now().strftime("%d-%m-%Y %H_%M_%S")}.err')
        print(f"Error al buscar documento en MongoDB: {e}")
        return None
   
def val_pedimento(mensaje_json):
    """Valida y procesa el pedimento del mensaje JSON recibido."""
    try:
     
        pago = mensaje_json.get('pago', 0)
        patente_autorizacion    = pago.pop('patente_autorizacion', 0)
        numero_pedimento        = pago.pop('numero_pedimento', 0)
        aduana_de_despacho      = pago.pop('aduana_de_despacho', 0)

        #Conexión mongoDB
        coleccion = establecer_conexion_mongodb()
        #Buscar pedimento
        documento_encontrado = buscar_en_mongo(int(patente_autorizacion),int(numero_pedimento),int(aduana_de_despacho), coleccion)

        if documento_encontrado:
            resultado_actualizacion = coleccion.update_one(
                {"_id": documento_encontrado["_id"]},
                {"$set": {"inicio_pedimento.pago": pago}}
            )
            if resultado_actualizacion.modified_count > 0:
                print(f"Se ha agregado el objeto 'pago' correctamente dentro de inicio_pedimento en el documento: patente_autorizacion - {patente_autorizacion}, numero_pedimento - {numero_pedimento}, aduana_de_despacho - {aduana_de_despacho}")
            else:
                print("No se realizó ninguna actualización en el documento.")
                
    except Exception as e:
        producer = Producer(configproducer)
        topic = 'pago_pedimento'
        fecha_inicio = datetime.now()
        fecha_fin = datetime.now()
        mensaje = f'FAIL|{patente_autorizacion}|{numero_pedimento}|{aduana_de_despacho}|{fecha_inicio.strftime("%Y-%m-%d %H:%M:%S")}|{fecha_fin.strftime("%Y-%m-%d %H:%M:%S")}'
        producer.produce(topic, value=mensaje)
        producer.flush()
        logs_error(f'Termina {patente_autorizacion}, {numero_pedimento}, {aduana_de_despacho} con error\n----------\n{str(e)}', f'logs_{datetime.now().strftime("%d-%m-%Y %H_%M_%S")}.err')
        print(f"Termina {patente_autorizacion}, {numero_pedimento}, {aduana_de_despacho} con error.")

def main():
    # Crea un consumidor
    consumer = Consumer(config)
    # Suscribe al topic
    topic = 'pago_pedimento'  # Reemplaza con el nombre de tu topic
    consumer.subscribe([topic])
    print(config)
    try:
        while True:
            msg = consumer.poll(20.0)
            if msg is None:
                continue
            if msg.error():
                print(f'Error en el mensaje: {msg.error()}')
                break
            if msg.value() is not None:
                try:
                    mensaje_json = json.loads(msg.value().decode('utf-8'))
                    print("Mensaje recibido:", mensaje_json)
                    val_pedimento(mensaje_json)
                except json.JSONDecodeError as e:
                    print(f"Error al decodificar el mensaje JSON: {e}")
                    print(f"Mensaje original: {msg.value()}")
            consumer.commit()
    except KeyboardInterrupt:
        pass
    except KafkaException as e:
        logs_error(f'Error en Kafka: {e}', f'logs_kafka_{datetime.now().strftime("%d-%m-%Y %H_%M_%S")}.err')
        print(e)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
