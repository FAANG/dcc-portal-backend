FROM python:3.9.7
RUN python -m pip install --upgrade pip
WORKDIR /scripts
COPY ./scripts /scripts
ADD ./requirements.txt ./
RUN pip install -r requirements.txt