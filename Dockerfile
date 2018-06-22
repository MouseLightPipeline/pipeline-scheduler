FROM node:8.11.3

WORKDIR /app

COPY dist .

RUN yarn install

CMD ["./docker-entry.sh"]

EXPOSE  3002
