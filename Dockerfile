# Stage 1: Build the React frontend
FROM node:20.12-bookworm as build-stage
WORKDIR /app
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ .
RUN npm run build

# Stage 2: Build the Express.js backend
FROM node:20.12-bookworm as production-stage
WORKDIR /app
COPY --from=build-stage /app/dist /app/public  # Copy built frontend files to the backend static folder
COPY backend/package*.json ./
RUN npm install
COPY backend/ .

# Final stage: Set up the production environment
FROM node:20.12-bookworm
WORKDIR /app

# Install Python and supervisord
RUN apt-get update && apt-get install -y python3 python3-pip supervisor && \
    rm -rf /var/lib/apt/lists/*
COPY --from=production-stage /app /app

# Install Python dependencies
COPY scripts/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the Python script
COPY scripts/data_processor.py .

# Copy supervisord configuration file
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Expose the port the app runs on
EXPOSE 3000

# Start supervisord
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
