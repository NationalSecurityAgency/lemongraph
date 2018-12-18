FROM debian:9-slim AS base

RUN apt-get update && apt-get install -y -q python3-dateutil python3-six python3-setuptools python3-cffi && apt-get clean



FROM base AS build

RUN apt-get update
RUN apt-get install -y -q python3-pip zlib1g-dev python3-dev python3-cffi curl git

COPY . /lemongraph
WORKDIR /lemongraph
RUN python3 setup.py bdist_wheel
RUN pip3 install dist/*.whl



FROM base

COPY --from=build /usr/local/lib/python3.5/dist-packages /usr/local/lib/python3.5/dist-packages

EXPOSE 8000
WORKDIR /data
ENTRYPOINT [ "python3", "-mLemonGraph.server", "-i", "0.0.0.0" ]
