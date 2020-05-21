offset=${1-1}
echo starting rabbit on port $((5672+$offset-1))
docker run  -t -i --rm  --hostname my-rabbit-$offset --name some-rabbit-$offset -p $((8080+$offset-1)):15672 -p $((5672+$offset-1)):5672  rabbitmq:3-management
