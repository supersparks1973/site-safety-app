FROM node:20
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN mkdir -p uploads
EXPOSE 3000
ENV NODE_ENV=production
CMD ["node", "server.js"]
