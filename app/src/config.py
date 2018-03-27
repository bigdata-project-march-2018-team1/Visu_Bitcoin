import logging
import elastic_config

config = {
    'elasticsearch': {
        'hosts': ['db'], # TODO put it in elasticsearch_config
        'username': 'elastic',
        'password': elasticsearch_config.password
    },
    'logger': {
        'level': logging.INFO
    }
}
