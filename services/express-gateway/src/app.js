import express from 'express';
import cors from 'cors';

const app = express();

// Middleware: Enable CORS (Allows frontend mobile app & website to talk to Express)
app.use(cors());

// Middleware: Parse incoming JSON payloads sent by the frontend
app.use(express.json());

// Health Check Endpoint (To verify Express is running)
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'OK',
    message: 'Buffer Express API Gateway is running smoothly! 🚀',
    timestamp: new Date().toISOString()
  });
});

// Root Endpoint Welcome Message
app.get('/', (req, res) => {
  res.status(200).json({
    name: 'Buffer API Gateway',
    version: '1.0.0',
    description: 'Intelligent Workflow Orchestrator API'
  });
});

export default app;
