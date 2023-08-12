docker build -t postgres-shard .

shards=${SHARDS:-3}

do
for (( i=0; i<$shards; i++ ))
  shard_name="postgres-shard-$i"
  docker stop $shard_name || true
  docker rm $shard_name || true
  docker run -d -p "555$i:5432" --name $shard_name postgres-shard

done