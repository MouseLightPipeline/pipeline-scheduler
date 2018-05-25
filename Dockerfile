FROM node:7.10

WORKDIR /app

COPY dist .

RUN yarn install

CMD ["./docker-entry.sh"]

EXPOSE  3002
