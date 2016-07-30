# Build with $ docker build -t distrpy/worker .
# Run container with $ docker run -it -v /path/to/project:/distrpy --name [container_name] distrpy/worker bash
# ex. $ docker run -it -v ~/Distrpy:/Distrpy -w /Distrpy --name worker_dev distrpy/worker bash
FROM python
RUN apt-get update
RUN pip install -U pip
CMD ["pip", "install", "-r", "requirements.txt"]

# Add sudo user `docker`
# RUN apt-get -y install sudo
# RUN useradd -m docker && echo "docker:docker" | chpasswd && adduser docker sudo
# USER docker

EXPOSE 5012
EXPOSE 15012

VOLUME /distrpy
WORKDIR /distrpy
