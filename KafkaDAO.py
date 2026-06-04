from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
from typing import Any, Dict, Optional, Union, Callable

class KafkaDAO:
    """
    Data Access Object para Kafka Producer, optimizado para entornos multihilo.
    
    Esta clase es thread-safe. Se recomienda crear UNA ÚNICA instancia de KafkaDAO
    y compartirla entre todos los hilos (sensores) de la aplicación para una
    gestión eficiente de las conexiones y el batching de mensajes.
    
    Para enviar datos de forma eficiente desde múltiples hilos, utilice el método `send_async`.
    """
    
    def __init__(
        self,
        bootstrap_servers: Union[str, list],
        client_id: str = "kafka-dao-producer",
        **kwargs
    ):
        """
        Inicializa el productor de Kafka.
        
        Args:
            bootstrap_servers: Servidor(es) de Kafka (ej: 'localhost:9092').
            client_id: Identificador del cliente.
            **kwargs: Configuraciones adicionales para KafkaProducer.
        """
        self.logger = logging.getLogger(__name__)
        self.bootstrap_servers = bootstrap_servers
        self._closed = False  # Flag para controlar el estado
        self.producer = None
        
        default_config = {
            'bootstrap_servers': bootstrap_servers,
            'client_id': client_id,
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': 'all',
            'retries': 3,
            'max_in_flight_requests_per_connection': 5,
            'linger_ms': 20,
            'batch_size': 65536
        }
        
        default_config.update(kwargs)
        
        try:
            self.producer = KafkaProducer(**default_config)
            self.logger.info(f"Kafka Producer inicializado: {bootstrap_servers}")
        except Exception as e:
            self.logger.error(f"Error al inicializar Kafka Producer: {e}")
            self._closed = True  # Marcar como cerrado si la inicialización falla
            raise

    def _check_closed(self):
        """Comprueba si el productor está cerrado y lanza una excepción si lo está."""
        if self._closed or not self.producer:
            raise ConnectionError("KafkaProducer already closed!")

    def send(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Envía un mensaje a un topic de Kafka de forma síncrona (bloqueante).
        """
        try:
            self._check_closed()
            kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()] if headers else None
            
            future = self.producer.send(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms,
                headers=kafka_headers
            )
            
            record_metadata = future.get(timeout=10)
            
            self.logger.debug(
                f"Mensaje síncrono enviado a topic '{topic}' - "
                f"Partición: {record_metadata.partition}, "
                f"Offset: {record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            self.logger.error(f"Error de Kafka al enviar mensaje: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error inesperado al enviar mensaje: {e}")
            return False
    
    def send_async(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        callback: Optional[Callable] = None
    ):
        """
        Envía un mensaje de forma asíncrona. Ideal para alto rendimiento.
        """
        try:
            self._check_closed()
        except ConnectionError as e:
            self.logger.error(f"Error al iniciar envío asíncrono: {e}")
            if callback:
                callback(False, e)
            return

        def on_send_success(record_metadata):
            self.logger.debug(
                f"Mensaje asíncrono enviado - Topic: {record_metadata.topic}, "
                f"Partición: {record_metadata.partition}, Offset: {record_metadata.offset}"
            )
            if callback:
                callback(True, record_metadata)
        
        def on_send_error(exc):
            self.logger.error(f"Error al enviar mensaje asíncrono: {exc}")
            if callback:
                callback(False, exc)
        
        try:
            self.producer.send(topic, value=value, key=key)\
                .add_callback(on_send_success)\
                .add_errback(on_send_error)
        except Exception as e:
            self.logger.error(f"Error al iniciar envío asíncrono: {e}")
            if callback:
                callback(False, e)
    
    def flush(self, timeout: Optional[float] = None):
        """
        Fuerza el envío de todos los mensajes en buffer.
        """
        if self._closed or not self.producer:
            self.logger.warning("Intento de hacer flush en un productor cerrado o no inicializado.")
            return
        try:
            self.producer.flush(timeout=timeout)
            self.logger.info("Buffer de mensajes vaciado exitosamente.")
        except Exception as e:
            self.logger.error(f"Error al vaciar buffer: {e}")
    
    def close(self):
        """Cierra la conexión del productor de forma segura."""
        if self._closed:
            return
        
        self.logger.info("Cerrando Kafka Producer...")
        self._closed = True  # Marcar como cerrado inmediatamente
        
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close(timeout=10)
                self.logger.info("Kafka Producer cerrado correctamente.")
            except Exception as e:
                self.logger.error(f"Error durante el cierre del productor de Kafka: {e}")
            finally:
                self.producer = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    #     self.close()

