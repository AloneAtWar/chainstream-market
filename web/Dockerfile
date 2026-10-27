# The frontend as static files behind nginx, which also proxies /v1 (REST
# and the WebSocket) to market-api, so the page and the API share an origin.
FROM node:22-alpine AS build
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.29-alpine
COPY nginx.conf /etc/nginx/templates/default.conf.template
COPY --from=build /src/dist /usr/share/nginx/html
ENV MARKET_API=http://market-api:8080
EXPOSE 80
