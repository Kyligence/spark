FROM bitnami/node:10.24.1 as build-stage
WORKDIR /app
COPY .npmrc ./
COPY package*.json ./
RUN npm install --unsafe-perm
COPY . .
RUN npm run build

# production stage
FROM nginx:stable-alpine as production-stage
COPY --from=build-stage /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
