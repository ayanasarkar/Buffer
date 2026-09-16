# ⚡ Go Scheduling Engine (Buffer Core)

**Owner**: Ayana  
**Role**: High-performance backend engine that consumes registration events from RabbitMQ, runs the Buffer Queue Scheduling Algorithm, and saves assigned slots to PostgreSQL.

---

## 📌 Responsibilities
1. **Queue Consumer**: Listens to RabbitMQ `UserRegistrationEvent` payloads sent by Express.
2. **Buffer Batching**: Holds registrants from the first 60 minutes in memory.
3. **Fairness Calculator**: Calculates fairness scores using Age Factor + Convenience Factor.
4. **Slot Generator & Feasibility Swapper**: Generates 10-minute slots, checks return-home deadlines, and performs slot swaps when travel constraints fail.
5. **Postgres Writer**: Inserts assigned slots, status, and reason codes into the shared database.
