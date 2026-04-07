FROM node:20-alpine

WORKDIR /app

# Instalación de dependencias del microservicio
COPY package.json ./
RUN npm install --omit=dev

# Copia del código fuente
COPY server.js ./

ENV NODE_ENV=production
ENV PORT=3001

EXPOSE 3001

# Healthcheck requerido por el contrato
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:3001/health || exit 1

CMD ["npm", "start"]