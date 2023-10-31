FROM bitnami/spark:3.3

USER root
RUN apt-get update -y && \
    apt-get install -y gconf-service libasound2 libatk1.0-0 libcairo2 libcups2 libfontconfig1 libgdk-pixbuf2.0-0 libgtk-3-0 libnspr4 libpango-1.0-0 libxss1 fonts-liberation libappindicator1 libnss3 lsb-release xdg-utils && \
    apt-get install -y chromium &&\
    apt-get install -y chromium-driver;

USER 1001

RUN pip install --upgrade pip
COPY requirements/requirements.txt .
RUN pip install -r requirements.txt

CMD ["sleep","infinity"]