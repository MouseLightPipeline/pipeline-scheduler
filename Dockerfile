FROM node:12.19

WORKDIR /app

COPY dist .

RUN yarn install --production=true

RUN groupadd -g 1097 mousebrainmicro
RUN adduser -u 7700649 --disabled-password --gecos '' mluser
RUN usermod -a -G mousebrainmicro mluser

USER mluser

CMD ["./docker-entry.sh"]

EXPOSE  3002
