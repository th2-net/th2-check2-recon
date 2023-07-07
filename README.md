This repository is a library for creating th2-check2-recon applications.

## Installation
```
pip install th2-check2-recon
```
This package can be found on [PyPI](https://pypi.org/project/th2-check2-recon/ "th2-check2-recon").

## Configuration

- *use_kafka* - defines if kafka client should be passed to user defined rules
- *kafka_configuration* - kafka connectivity details

### Kafka Configuration
- *topic* - default topic for published events.
- *key* - add default key as configuration option.
- *fail_on_connection_failure* - defines if service should fall when kafka connection cannot be established.
- *bootstrap.servers* - comma separated list of host:port of kafka broker instances. Default: ['localhost:9092']

Full configuration list can be found [here](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)

Useful configurations from full list are:
- security.protocol - Protocol used to communicate with brokers. Default value: plaintext. Possible values: [ plaintext, ssl, sasl_plaintext, sasl_ssl ]
- sasl.mechanism - SASL mechanism to use for authentication. Should be configured if `security.protocol` is based on SASL. Possible values: [GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER]
- sasl.username - SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms
- sasl.password - SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms
- group.id - Client group id string. All clients sharing the same group.id belong to the same group.


