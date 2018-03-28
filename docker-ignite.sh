COMPOSE_ARGUMENT='--project-directory . -f docker-compose/docker-compose.yml -f docker-compose/docker-compose-tx.yml'

docker-compose $COMPOSE_ARGUMENT down
docker-compose $COMPOSE_ARGUMENT build # TODO should be replaced x-smart-recreate
docker-compose $COMPOSE_ARGUMENT up -d
