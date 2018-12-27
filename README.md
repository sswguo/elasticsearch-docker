# elasticsearch-docker

Set the max_map_count:
````
sudo sysctl -w vm.max_map_count=262144
````

Start the containers:
````
sudo docker-compose up
````

Try the kabana:

http://{IP}:5601
