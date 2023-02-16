# default base image - should also work for:
#	debian:11-slim
#	ubuntu:focal
#	fedora:35
ARG IMAGE=alpine:3.17

# build the base runtime image
FROM ${IMAGE} AS runtime

# populate if base image needs some initial configuration changes
ARG CONFIGURE

# optionally apply supplied configuration changes
RUN [ -z "${CONFIGURE}" ] || (${CONFIGURE})

# ensure exactly one of apk/apt-get/yum exists
RUN n=0; for x in apk apt-get yum; do ! type $x 2>/dev/null || n=$((n+1)); done; [ $n -eq 1 ]

# use package manager to pull in runtime dependencies
ARG APK_RUNTIME="py3-cffi py3-dateutil py3-lazy py3-msgpack py3-setuptools py3-six py3-ujson"
ARG APT_RUNTIME="python3-cffi python3-dateutil python3-msgpack python3-setuptools python3-six python3-ujson"
ARG YUM_RUNTIME="python3-cffi python3-dateutil python3-msgpack python3-setuptools python3-six python3-ujson"
RUN ! type apk     2>/dev/null || apk add --no-cache ${APK_RUNTIME}
RUN ! type apt-get 2>/dev/null || (apt-get update && apt-get install -y -q ${APT_RUNTIME} && apt-get clean)
RUN ! type yum     2>/dev/null || (yum install -y ${YUM_RUNTIME} && yum clean all)

# switch to intermediate build stage
FROM runtime AS build

# use package manager to pull in build dependencies
ARG APK_BUILD="bash curl gcc git libffi-dev make musl-dev python3-dev py3-pip zlib-dev"
ARG APT_BUILD="bash curl gcc git libffi-dev make python3-dev python3-pip unzip zlib1g-dev"
ARG YUM_BUILD="bash curl gcc git libffi-devel make python3-devel python3-pip unzip zlib-devel"
RUN ! type apk     2>/dev/null || apk add ${APK_BUILD}
RUN ! type apt-get 2>/dev/null || (apt-get update && apt-get install -y -q ${APT_BUILD})
RUN ! type yum     2>/dev/null || yum install -y ${YUM_BUILD}

# stage 'lazy' into /install if runtime didn't provide it
RUN pip3 show lazy || pip3 install --root=/install lazy

# do the build
COPY . /build
WORKDIR /build
RUN make deps
RUN CPPFLAGS=-DHAVE_FDATASYNC=1 python3 setup.py bdist

# stage LG into /install
WORKDIR /install
RUN tar xvf /build/dist/LemonGraph-*

# overlay build stage's /install onto runtime
FROM runtime AS install
COPY --from=build /install /

# test LG as installed
FROM install AS test
COPY --from=build /build/test.py /
RUN python3 < /test.py

# and bless install stage
FROM install

# force test stage to run
COPY --from=test /mnt /mnt

# LG listens on port 8000 by default
EXPOSE 8000

# all LG writes default to cwd
WORKDIR /data
VOLUME /data

# pass any extra args on command line - I recommend at least: -s
ENTRYPOINT [ "python3", "-mLemonGraph.server", "-i", "0.0.0.0" ]
