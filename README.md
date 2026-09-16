<div align="center">
  
# 🚀 Project BUFFER
### Intelligent Workflow Orchestrator & Smart Queuing System

[![Hackathon](https://img.shields.io/badge/Hackathon-HackSpire_'26-blueviolet?style=for-the-badge)](https://hackspire.com)
[![Status](https://img.shields.io/badge/Status-Active_Development-brightgreen?style=for-the-badge)]()
[![Architecture](https://img.shields.io/badge/Architecture-Event--Driven_Microservices-blue?style=for-the-badge)]()

**Team ThreadRippers** • Theme: Smart Automation & Digital Transformation

</div>

---

## 📌 The Problem
Physical queues at colleges, hospitals, government offices, and banks are inefficient, cause overcrowding, and offer zero visibility into actual wait times. Traditional digital queues are rigid and cannot handle complex, multi-stage workflows 

## 💡 The Solution: Buffer
**Buffer** is an intelligent, remote queue management system and multi-stage workflow orchestrator. It allows institutions to define dynamic, complex workflows, and allows users to join virtual queues seamlessly via QR code—tracking their live progress, required documents, and ETA directly on their phones.

---

## 🔥 Key Features

### 🛠️ Dynamic Workflow Engine
- **Admin Customizable Orchestration**: Admins can construct custom multi-stage workflows on the fly.
- **Stage-Specific Requirements**: Define distinct required documents and rules for every individual counter/stage in the flow.
- **Staff Control Panel**: Staff members can advance users, skip no-shows, pause for breaks, or report technical faults causing automatic re-routing.

### 🧠 Smart Scheduling & Buffer Algorithm
- **The Buffer Window (First 60 Mins)**: The system aggregates early registrants into a "Buffer Pool" and calculates a **Fairness Priority Score** based on age, accessibility needs, and other factors.
- **FCFS Group**: Standard First-Come, First-Served queue logic for later registrants.
- **Travel Feasibility Engine**: Predicts return-home travel constraints and swaps slots dynamically if a user's assigned ETA risks exceeding safe travel hours.

### ⚡ Real-Time Architecture
- **Live Updates**: Utilizing WebSockets, the system pushes live ETA updates, queue position, and counter assignments to the user's phone with zero manual refreshing.
- **Event-Driven Broker**: High-throughput message queuing using RabbitMQ ensures seamless, non-blocking communication between the Express API Gateway and the Go Scheduling Engine.

---

## 🏗️ System Architecture (Monorepo)

Buffer utilizes an **Event-Driven Microservices Architecture** to separate high-throughput API ingestion from intensive queue-sorting mathematics.

```text
Buffer/
├── docker-compose.yml           # Local Infrastructure (PostgreSQL & RabbitMQ)
└── services/
    ├── express-gateway/         # Node.js + Express: Public API & WebSockets (Souvik)
    └── go-scheduler/            # Golang Engine: Heavy-lifting Queue Algorithms (Ayana)
```

### 💻 Technology Stack
* **API Gateway**: Node.js, Express.js
* **Scheduling Engine**: Golang
* **Database**: PostgreSQL (Prisma ORM)
* **Message Broker**: RabbitMQ
* **Real-Time Communication**: WebSockets (WS)
* **Infrastructure**: Docker & Docker Compose

---

## 🚀 Quick Start (Local Development)

### Prerequisites
- Docker & Docker Compose
- Node.js (v18+)
- Go (1.21+)

### 1. Start Infrastructure (Database & RabbitMQ)
```bash
docker-compose up -d
```

### 2. Run the Express API Gateway (`services/express-gateway`)
```bash
cd services/express-gateway
npm install
npm run dev
```

### 3. Run the Go Scheduling Engine (`services/go-scheduler`)
```bash
cd services/go-scheduler
go run main.go
```

---

<div align="center">
  <i>Built with ❤️ for HackSpire '26 by Team ThreadRippers</i>
</div>
