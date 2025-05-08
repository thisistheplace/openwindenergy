#! /bin/bash -l

if ! [ -f ".env" ]; then
    cp .env-template .env
fi

docker compose up -d
docker exec -ti openwindenergy-build /usr/src/openwindenergy/build-cli.sh "$@"
#docker exec -ti openwindenergy-build /bin/bash
docker compose down

