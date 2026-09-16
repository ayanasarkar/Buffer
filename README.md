# 🚀 Project BUFFER - Intelligent Workflow Orchestrator & Smart Queuing System

**Team**: ThreadRippers  
**Hackathon**: HackSpire '26  
**Theme**: Smart Automation & Digital Transformation  

---

## 📌 Project Overview
Buffer is a remote queue management system and multi-counter workflow orchestrator designed to eliminate physical waiting lines in colleges, hospitals, banks, and government offices.

### Core Features
- **Multi-Counter Workflow Pipeline**: Connects sequential stages (`Registration` -> `Fee Payment` -> `Verification` -> `Approval`).
- **QR-Based Virtual Queue**: Users scan QR codes to enter workflows, receive digital tokens, and view required documents per stage.
- **Buffer Queue Scheduling Algorithm**:
  - **Buffer Group (First 60 Mins)**: Priority calculated using a weighted **Fairness Score** (Age & Convenience factors).
  - **FCFS Group**: Standard First-Come, First-Served queue for late registrants.
  - **Cutoff Check**: Registrations within 2 hours of queue closing are marked `UNSCHEDULED`.
  - **Travel Feasibility & Swapping**: Verifies return-home travel constraints and swaps slots when necessary.
- **Real-Time WebSockets**: Pushes predicted waiting times (ETA) and stage progression to the user's phone live.

---

## 🏗️ Repository Architecture (Monorepo)

```
Buffer/
├── docker-compose.yml           # Runs Express, Go Engine, PostgreSQL, & RabbitMQ
├── README.md                    # Main project documentation
└── services/
    ├── express-gateway/         # Node.js + Express API Gateway (Souvik)
    └── go-scheduler/            # Go Scheduling Engine (Ayana)
```

---

## 🚀 Quick Start (Local Development)

### 1. Express API Gateway (`services/express-gateway`)
```bash
cd services/express-gateway
npm install
npm run dev
```

### 2. Go Scheduling Engine (`services/go-scheduler`)
```bash
cd services/go-scheduler
go run main.go
```
