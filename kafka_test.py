import time
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
import logging
import sys

# Конфигурация Kafka-консьюмера
consumer_config = {
    'bootstrap.servers': 'kafka:9093',
    'group.id': 'reports-group',
    'auto.offset.reset': 'earliest',
    'session.timeout.ms': 60000,
    'retries': 5
}

# Конфигурация Kafka-администратора
admin_config = {
    'bootstrap.servers': 'kafka:9093'
}

# Создание экземпляра администратора
admin_client = AdminClient(admin_config)

# Имя топика
topic = 'create_file'

# Настраиваем логгер
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Создаем обработчик для стандартного вывода
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)

# Формат логов
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Добавляем обработчик к логгеру
logger.addHandler(handler)

# Пример использования
logger.info("Это сообщение будет выведено в логи Docker контейнера.")

def topic_exists(topic_name):
    try:
        # Получаем метаданные о топиках
        metadata = admin_client.list_topics(timeout=10)
        return topic_name in metadata.topics
    except KafkaException as e:
        logger.error(f"Error while checking topic existence: {e}")
        return False

while not topic_exists(topic):
    logger.info("Ожидание создания топика")
    time.sleep(1)

consumer = Consumer(consumer_config)
consumer.subscribe([topic])

try:
    while True:
        # Чтение сообщений с таймаутом ожидания
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            # Нет сообщения, просто продолжаем
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Достигнут конец партиции, продолжаем
                continue
            else:
                # Обработка других ошибок
                logger.error(f"Error: {msg.error()}")
                break

        # Обработка полученного сообщения
        logger.info(f"Received message: {msg.value().decode('utf-8')} from topic: {msg.topic()}")

except KeyboardInterrupt:
    # Завершаем работу при нажатии Ctrl+C
    pass
finally:
    # Закрываем консьюмер корректно
    consumer.close()
