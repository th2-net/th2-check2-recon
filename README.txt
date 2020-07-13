This component provide feature to compare two or more queues of parsed message via user rules. Every rule should implement two methods: hash(message) and check(messages).
Rule.hash - assepts parsed message and should returns string genrated with message fields values.
Rule.check - assepts parsed message collection which contains message from every listened queue with equal hash. This method should compare messages and return TH2 Event with comparison details and results.

Example of Recon component env variables:
RABBITMQ_HOST=some-host-name-or-ip
RABBITMQ_PORT=7777
RABBITMQ_VHOST=someVhost
RABBITMQ_USER=some_user
RABBITMQ_PASS=some_pass
RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY=some_exchange
ROUTING_KEYS='{"mq_key1", "mq_key2"}' - format will be changed to JSON '["mq_key1", "mq_key2"]'
EVENT_STORAGE=event-store-host-name-or-ip:9999 - it will be split to TH2_EVENT_STORAGE_GRPC_HOST, TH2_EVENT_STORAGE_GRPC_PORT
COMPARATOR_URI=utility-host-name-or-ip:9999 - it will be split to TH2_UTILITY_GRPC_HOST, TH2_UTILITY_GRPC_PORT
CACHE_SIZE=10 - Max number of messages in cache for every queue
RECON_TIMEOUT=5 - Timeout in seconds to check incoming messages from queue (need more detail)
TIME_INTERVAL=10 - Window size in seconds for observing messages in cache (rewrite)
RECON_NAME=DEMO - Name Recon report after symbol '_'. 'Recon_<RECON_NAME>'.
RULES_CONFIGURATIONS_FILE - path to the rule configuration file