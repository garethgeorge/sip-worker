FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y make gcc g++ build-essential m4
RUN apt-get install -y python3-pip python3-dev \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && pip3 install --upgrade pip

RUN mkdir /build 
WORKDIR /build 

# copy build files
COPY ./submodules/sip-processor /build/sip-processor

RUN make -C /build/sip-processor/src/euca-cutils && \
  make -C /build/sip-processor/src/mio && \
  make -C /build/sip-processor/src/schmib_q && \
  make -C /build/sip-processor/src/pred-duration && \
  make -C /build/sip-processor/src/spot-predictions ; exit 0

RUN mkdir -p /sip/bin 

RUN cp /build/sip-processor/src/euca-cutils/ptime /sip/bin/ && \
  cp /build/sip-processor/src/euca-cutils/convert_time /sip/bin/ && \
  cp /build/sip-processor/src/schmib_q/bmbp_ts /sip/bin/ && \
  cp /build/sip-processor/src/schmib_q/bmbp_index /sip/bin/ && \
  cp /build/sip-processor/src/pred-duration/pred-distribution /sip/bin/ && \
  cp /build/sip-processor/src/pred-duration/pred-distribution-fast /sip/bin/ && \
  cp /build/sip-processor/src/spot-predictions/spot-price-aggregate /sip/bin/ && \
  cp /build/sip-processor/src/pred-duration/pred-duration /sip/bin/

# run the pip install
RUN pip install aiohttp aiobotocore aioamqp motor 

# setup sip worker 
COPY . /sip 

WORKDIR /sip

RUN rm -rf ./state ./tmp

CMD ["python", "-u", "main.py"]
