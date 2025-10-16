import os
import threading
import json
import time
import io
from typing import Callable, Dict, Any
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .cache_updater import update_business_data

# --- Configuración desde variables de entorno ---
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
SCHEMA_REGISTRY_URL = os.getenv("KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8081")
CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "orquestador-app-development")
TOPIC = os.getenv("KAFKA_TOPIC", "businesses.fct.update.0")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "orquestador-llm-consumer-group")
# ---

# Definimos el esquema de AVRO directamente aquí para no depender del registro de esquemas
# Esto es una versión simplificada, solo necesitamos el RUT
VALUE_SCHEMA = """
{
    "type": "record",
    "name": "Update",
    "namespace": "businesses.fct",
    "fields": [
        {"name": "businessId", "type": "int"},
        {"name": "actionType", "type": "string"},
        {"name": "rut", "type": "string"},
        {"name": "mobileDefaultChannel", "type": "string"},
        {"name": "name", "type": ["string", "null"]},
        {"name": "team", "type": ["string", "null"]},
        {"name": "fantasyName", "type": ["string", "null"]},
        {"name": "legalName", "type": ["string", "null"]},
        {"name": "phone", "type": ["string", "null"]},
        {"name": "email", "type": ["string", "null"]},
        {"name": "address", "type": ["string", "null"]},
        {"name": "commune", "type": ["string", "null"]},
        {"name": "economicActivity", "type": ["string", "null"]},
        {"name": "status", "type": ["int", "null"]},
        {"name": "adminUserId", "type": ["int", "null"]},
        {"name": "clientDocumentDayExpiration", "type": ["int", "null"]},
        {"name": "providerDocumentDayExpiration", "type": ["int", "null"]},
        {"name": "deletedAt", "type": ["long", "null"]},
        {"name": "adminUser", "type": ["null", {
            "type": "record",
            "name": "adminUserRecord",
            "fields": [
                {"name": "adminUserId", "type": "int"},
                {"name": "name", "type": ["string", "null"]},
                {"name": "email", "type": ["string", "null"]}
            ]
        }]},
        {"name": "subscriptions", "type": {
            "type": "array", 
            "items": {
                "type": "record",
                "name": "subscription",
                "fields": [
                    {"name": "subscriptionId", "type": "int"},
                    {"name": "active", "type": ["boolean", "null"]},
                    {"name": "planId", "type": ["int", "null"]},
                    {"name": "deletedAt", "type": ["long", "null"]}
                ]
            }
        }},
        {"name": "users", "type": {
            "type": "array", 
            "items": {
                "type": "record",
                "name": "user",
                "fields": [
                    {"name": "userId", "type": "int"},
                    {"name": "rut", "type": ["string", "null"]},
                    {"name": "role", "type": ["string", "null"]},
                    {"name": "uid", "type": ["string", "null"]},
                    {"name": "name", "type": ["string", "null"]},
                    {"name": "email", "type": ["string", "null"]},
                    {"name": "phone", "type": ["string", "null"]},
                    {"name": "messagePreferences", "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "messagePreference",
                            "fields": [
                                {"name": "userId", "type": "int"},
                                {"name": "channels", "type": {"type": "array", "items": "string"}},
                                {"name": "subscribed", "type": "boolean"},
                                {"name": "messagesEventId", "type": "int"},
                                {"name": "eventName", "type": "string"}
                            ]
                        }
                    }}
                ]
            }
        }},
        {"name": "settings", "type": {
            "type": "array", 
            "items": {
                "type": "record",
                "name": "setting",
                "fields": [
                    {"name": "settingId", "type": "int"},
                    {"name": "active", "type": ["boolean", "null"]},
                    {"name": "key", "type": ["string", "null"]},
                    {"name": "name", "type": ["string", "null"]},
                    {"name": "value", "type": ["int", "null"]},
                    {"name": "visualized", "type": ["boolean", "null"]},
                    {"name": "businessId", "type": ["int", "null"]}
                ]
            }
        }}
    ]
}
"""

KEY_SCHEMA = """
{
    "type": "record",
    "name": "UpdateKey",
    "namespace": "businesses.fct",
    "fields": [
        {"name": "businessId", "type": "int"}
    ]
}
"""

class KafkaBusinessConsumer:
    def __init__(self):
        """Inicializa el consumidor de Kafka para actualizaciones de negocios."""
        self.running = False
        self.consumer_thread = None
        self._init_consumer()
        
    def _init_consumer(self):
        """Configura el consumidor de Kafka con Schema Registry."""
        # Configuración del consumidor
        self.consumer_config = {
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'group.id': GROUP_ID,
            'client.id': CLIENT_ID,
            'auto.offset.reset': 'earliest',  # Configurable: 'earliest' o 'latest'
            'enable.auto.commit': True  # Commit automático de offsets
        }
        
        # Configuración para Schema Registry
        self.schema_registry_conf = {
            'url': SCHEMA_REGISTRY_URL
        }
        
        try:
            # Inicializar conexión a Schema Registry para consultas
            self.schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)
            
            # Inicializar deserializadores Avro con esquemas embebidos
            self.value_deserializer = AvroDeserializer(
                schema_str=VALUE_SCHEMA,
                schema_registry_client=self.schema_registry_client
            )
            self.key_deserializer = AvroDeserializer(
                schema_str=KEY_SCHEMA,
                schema_registry_client=self.schema_registry_client
            )
            
            print(f"[Kafka] Deserializadores Avro inicializados con esquemas embebidos")
        except Exception as e:
            print(f"[Kafka] Error al inicializar schema registry o deserializadores Avro: {e}")
            self.schema_registry_client = None
            self.value_deserializer = None
            self.key_deserializer = None
        
        # Inicializar consumidor
        try:
            self.consumer = Consumer(self.consumer_config)
            print(f"[Kafka] Consumidor configurado para tópico {TOPIC} en {BOOTSTRAP_SERVERS}")
        except Exception as e:
            print(f"[Kafka] Error al inicializar consumidor: {e}")
            self.consumer = None

    def _extract_message_data(self, msg):
        """
        Extrae datos de un mensaje de Kafka usando varias estrategias.
        """
        value_bytes = msg.value()
        
        # Imprimir primeros bytes para debug
        if value_bytes and len(value_bytes) > 10:
            print(f"[Kafka] Primeros bytes del mensaje: {value_bytes[:10].hex()}")
        
        # Estrategia 1: Usar el deserializador Avro con Schema Registry
        if self.value_deserializer and value_bytes:
            try:
                # Crear el contexto de deserialización
                ctx = SerializationContext(msg.topic(), MessageField.VALUE)
                
                # Intentar deserializar con Avro
                value = self.value_deserializer(value_bytes, ctx)
                print(f"[Kafka] Mensaje deserializado con Avro")
                return value
            except Exception as e:
                print(f"[Kafka] Error en deserialización Avro: {e}")
                # Caer a la siguiente estrategia
        
        # Estrategia 2: Intentar como JSON
        try:
            # Decodificar bytes a string y parsear como JSON
            if isinstance(value_bytes, bytes):
                value_str = value_bytes.decode('utf-8')
            else:
                value_str = str(value_bytes)
            
            value = json.loads(value_str)
            print(f"[Kafka] Mensaje deserializado como JSON")
            return value
        except Exception as e:
            print(f"[Kafka] Error al deserializar como JSON: {e}")
        
        # Estrategia 3: Devolver None si todo falla
        print(f"[Kafka] No se pudo extraer datos del mensaje, devolviendo None")
        return None

    def _handle_message(self, msg):
        """Procesa un mensaje recibido de Kafka."""
        try:
            # Extraer datos usando las estrategias de deserialización
            data = self._extract_message_data(msg)
            
            # Si tenemos datos, procesarlos
            if data:
                # Extraer el RUT del negocio
                rut = data.get('rut')
                business_id = data.get('businessId')
                action_type = data.get('actionType')
                
                print(f"[Kafka] Recibido: Business ID: {business_id}, RUT: {rut}, Action: {action_type}")
                
                # Actualizar la caché para este RUT
                if rut:
                    print(f"[Kafka] Actualizando caché para RUT: {rut}")
                    update_business_data(rut)
                else:
                    print(f"[Kafka] Mensaje recibido sin RUT válido: {data}")
            else:
                print(f"[Kafka] No se pudo extraer datos del mensaje")
        
        except Exception as e:
            print(f"[Kafka] Error al procesar mensaje: {e}")
            # Imprimir el mensaje raw para debug si hay error
            value_bytes = msg.value()
            if value_bytes:
                print(f"[Kafka] Mensaje raw (hex): {value_bytes[:50].hex() if len(value_bytes) > 50 else value_bytes.hex()}")

    def _consume_loop(self):
        """Loop principal del consumidor que corre en un thread separado."""
        if not self.consumer:
            print("[Kafka] No se puede iniciar el consumo: consumidor no inicializado")
            return
            
        try:
            # Suscribirse al tópico
            self.consumer.subscribe([TOPIC])
            
            print(f"[Kafka] Consumidor iniciado y suscrito a {TOPIC}")
            
            # Loop principal de consumo
            while self.running:
                # Poll para nuevos mensajes
                msg = self.consumer.poll(timeout=1.0)
                
                # No hay mensaje nuevo
                if msg is None:
                    continue
                
                # Error en Kafka
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"[Kafka] Reached end of partition {msg.partition()}")
                    else:
                        print(f"[Kafka] Error: {msg.error()}")
                    continue
                
                # Procesar el mensaje válido
                self._handle_message(msg)
                
        except KafkaException as e:
            print(f"[Kafka] Error en el consumidor: {e}")
        except Exception as e:
            print(f"[Kafka] Error inesperado en el loop del consumidor: {e}")
        finally:
            # Cerrar el consumidor al salir del loop
            try:
                self.consumer.close()
                print("[Kafka] Consumidor cerrado correctamente")
            except:
                print("[Kafka] Error al cerrar el consumidor")

    def start(self):
        """Inicia el consumidor de Kafka en un thread separado."""
        if not self.running:
            self.running = True
            self.consumer_thread = threading.Thread(target=self._consume_loop)
            self.consumer_thread.daemon = True  # El thread se cerrará cuando el programa principal termine
            self.consumer_thread.start()
            print("[Kafka] Thread del consumidor iniciado")
            return True
        return False

    def stop(self):
        """Detiene el consumidor de Kafka."""
        if self.running:
            self.running = False
            # Si hay un thread corriendo, esperar a que termine
            if self.consumer_thread and self.consumer_thread.is_alive():
                self.consumer_thread.join(timeout=5.0)
            print("[Kafka] Consumidor detenido")
            return True
        return False

# Instancia singleton del consumidor
kafka_consumer = None

def get_kafka_consumer():
    """Devuelve la instancia singleton del consumidor de Kafka."""
    global kafka_consumer
    if kafka_consumer is None:
        kafka_consumer = KafkaBusinessConsumer()
    return kafka_consumer

def start_kafka_consumer():
    """Inicia el consumidor de Kafka."""
    consumer = get_kafka_consumer()
    return consumer.start()

def stop_kafka_consumer():
    """Detiene el consumidor de Kafka."""
    if kafka_consumer:
        return kafka_consumer.stop()
    return False