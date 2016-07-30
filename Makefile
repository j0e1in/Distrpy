all:
	python analyze.py
ps:
	docker ps
exec:
	docker exec -it redis0 /bin/bash
# Attach worker
worker1:
	docker run -it -v /var/demo/Distrpy:/Distrpy -w /Distrpy/example -p 23001:23001 --name distrpy_worker_0 distrpy/python:1.1 python start_worker.py 23001

worker2:
	docker run -it -v /var/demo/Distrpy:/Distrpy -w /Distrpy/example -p 23005:23005 --name distrpy_worker_1 distrpy/python:1.1 python start_worker.py 23005

# Detached worker
worker1-d:
	docker run -d -v /var/demo/Distrpy:/Distrpy -w /Distrpy/example -p 23001:23001 --name distrpy_worker_0 distrpy/python:1.1 python start_worker.py 23001

worker2-d:
	docker run -d -v /var/demo/Distrpy:/Distrpy -w /Distrpy/example -p 23005:23005 --name distrpy_worker_1 distrpy/python:1.1 python start_worker.py 23005

#Close worker container
close2:
	docker stop distrpy_worker_1
	docker rm distrpy_worker_1
close1:
	docker stop distrpy_worker_0
	docker rm distrpy_worker_0


# Attach worker
worker3:
	docker run -it -v /var/demo/Distrpy:/Distrpy -w /Distrpy/example -p 23001:23001 --name distrpy_worker_3 distrpy/python:1.1 python start_worker.py 23001

worker4:
	docker run -it -v /var/demo/Distrpy:/Distrpy -w /Distrpy/example -p 23005:23005 --name distrpy_worker_4 distrpy/python:1.1 python start_worker.py 23005

# Detached worker
worker3-d:
	docker run -d -v /var/demo/Distrpy:/Distrpy -w /Distrpy/example -p 23001:23001 --name distrpy_worker_3 distrpy/python:1.1 python start_worker.py 23001

worker4-d:
	docker run -d -v /var/demo/Distrpy:/Distrpy -w /Distrpy/example -p 23005:23005 --name distrpy_worker_4 distrpy/python:1.1 python start_worker.py 23005


