FROM node:16-alpine

WORKDIR /app

COPY package.json /app/
RUN npm install

COPY . /app/

EXPOSE 3500

CMD ["node", "index.js"]
