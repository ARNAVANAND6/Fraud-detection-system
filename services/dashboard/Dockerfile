FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci || npm install
COPY . .
ARG VITE_API_URL=http://localhost:8000
ARG VITE_API_KEY=dev-key
ENV VITE_API_URL=$VITE_API_URL VITE_API_KEY=$VITE_API_KEY
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /app/dist /usr/share/nginx/html
