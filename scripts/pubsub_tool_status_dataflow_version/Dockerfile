FROM apache/beam_python3.9_sdk:2.47.0 AS beam_python

FROM python:3.9.7

RUN pip install --no-cache-dir --upgrade pip

ADD requirements.txt ./
RUN pip install -r requirements.txt

COPY --from=beam_python /opt/apache/beam /opt/apache/beam

ENTRYPOINT [ "/opt/apache/beam/boot" ]





























































