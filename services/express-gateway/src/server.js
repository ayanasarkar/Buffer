import dotenv from 'dotenv';
import http from 'http';
import app from './app.js';

// Load environment variables from .env file
dotenv.config();

const PORT = process.env.PORT || 5000;

// Create HTTP Server
const server = http.createServer(app);

// Start listening for incoming network requests
server.listen(PORT, () => {
  console.log(`
  ======================================================
  🚀 Buffer Express API Gateway Started!
  📡 Server listening on: http://localhost:${PORT}
  🏥 Health check: http://localhost:${PORT}/health
  ======================================================
  `);
});
