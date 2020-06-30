FROM python:3.8-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV RABBITMQ_HOST=some-host-name-or-ip \
    RABBITMQ_PORT=7777 \
    RABBITMQ_VHOST=someVhost \
    RABBITMQ_USER=some_user \
    RABBITMQ_PASS=some_pass \
    RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY=some_exchange \
    ROUTING_KEYS='{"mq_key1", "mq_key2"}' \
    EVENT_STORAGE=event-store-host-name-or-ip:9999 \
    COMPARATOR_URI=utility-host-name-or-ip:9999 \
    CACHE_SIZE=10 \
    RECON_TIMEOUT=5 \
    TIME_INTERVAL=10;

CMD [ "python", "./src/main.py", "log_config.conf"]