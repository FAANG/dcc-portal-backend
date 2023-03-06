FROM python:3.9.7
RUN python -m pip install --upgrade pip
WORKDIR /scripts
COPY ./fetch_articles_production.py /scripts/
COPY ./utils_fetch_articles.py /scripts/
COPY ./constants.py /scripts/
COPY .env /scripts/
ADD ./requirements.txt ./
RUN pip install -r requirements.txt
