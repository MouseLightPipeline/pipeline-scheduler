FROM node:7.10

WORKDIR /app

COPY dist .

RUN yarn global add sequelize-cli

RUN yarn install

CMD ["./docker-entry.sh"]

EXPOSE  3000
