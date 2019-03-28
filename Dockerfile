FROM node:8.12

WORKDIR /app

COPY dist .

RUN yarn install

CMD ["./docker-entry.sh"]

EXPOSE  3002
