FROM ubuntu:16.04



RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y python3-pip python3-dev \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && pip3 install --upgrade pip
RUN apt-get install -y m4

RUN mkdir /build 
WORKDIR /build 

# copy build files
COPY ./submodules/sip-processor /build/sip-processor

# build the sip processor
# we build gmp first since it takes so bloody long
WORKDIR /build/sip-processor/src/schmib_q
RUN make gmp-6.0.0/lib/libgmp.a ; exit 0

# build everything else
WORKDIR /build/sip-processor/src/euca-cutils
RUN make ; exit 0
WORKDIR /build/sip-processor/src/mio
RUN make ; exit 0
WORKDIR /build/sip-processor/src/schmib_q
RUN make ; exit 0
WORKDIR /build/sip-processor/src/pred-duration
RUN make ; exit 0
WORKDIR /build/sip-processor/src/spot-predictions 
RUN make ; exit 0

# run the pip install
RUN pip install aiohttp aiobotocore aioamqp motor 

# setup sip worker 
CMD mkdir /sip 
WORKDIR /sip

COPY . /sip 

# copy over the binaries
RUN cp -r /build/sip-processor/src/schmib_q/bmbp_ts /sip/bin/bmbp_ts
RUN cp -r /build/sip-processor/src/pred-duration/pred-distribution /sip/bin/pred-distribution
RUN cp -r /build/sip-processor/src/pred-duration/pred-distribution-fast /sip/bin/pred-distribution-fast
RUN cp -r /build/sip-processor/src/pred-duration/pred-duration /sip/bin/pred-duration
RUN cp -r /build/sip-processor/src/spot-predictions/spot-price-aggregate /sip/bin/spot-price-aggregate

CMD ["python", "main.py"]

