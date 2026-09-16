# 📚 Buffer Backend Developer Glossary

This is your handy reference guide for core web development, server, and architecture terms used throughout the **Buffer** project.

---

## 🌐 1. The HTTP & Request/Response World

* **Request (`req`)**: The package of information sent from the user's phone/browser to our server.
* **Response (`res`)**: The package of information our server sends back to the user (e.g. "Registration successful!").
* **Payload / Request Body**: The actual JSON data sent inside a request.
  ```json
  {
    "userId": "U123",
    "age": 25,
    "travelTimeOut": 30
  }
  ```
* **Frontend**: The mobile app (React Native) or website running on the user's phone/laptop. **The frontend creates and sends the payload to Express.**
* **HTTP Methods (The Verbs)**:
  * **`GET`**: Asking the server to *fetch* data (e.g., view workflow details).
  * **`POST`**: Sending *new data* to the server to create something (e.g., register for a queue).
  * **`PUT` / `PATCH`**: Updating existing data.
  * **`DELETE`**: Removing data.
* **Status Codes**: 3-digit numbers the server returns to tell the client what happened:
  * **`200 OK`**: Everything went great!
  * **`201 Created`**: New data/token created successfully!
  * **`400 Bad Request`**: User sent invalid data.
  * **`401 Unauthorized`**: User is not logged in.
  * **`404 Not Found`**: The requested URL/page doesn't exist.
  * **`500 Internal Server Error`**: Backend code crashed or ran into a bug.

---

## 🏗️ 2. Express & Server Concepts

* **Server**: A computer program running 24/7 that listens for incoming messages from the internet and sends responses back.
* **Node.js**: The software environment that allows us to run JavaScript on a backend server (outside of a browser).
* **Express.js**: A popular JavaScript framework that makes building APIs, routes, and servers super simple.
* **Routing (Routes)**: Directing different web addresses (URLs) to different blocks of code inside your server.
* **Middleware**: Helper functions that run **in the middle** before a request reaches your main route handler.
  * *Example*: A middleware that checks if the user is an Admin *before* allowing them to create a workflow.
* **API Gateway**: A single server (our Express server) that handles all internet traffic and distributes tasks to internal microservices.
* **Data Ingestion**: Receiving raw incoming data from outside users, validating it, and importing it into the queue.

---

## 💾 3. Data & Storage

* **JSON (JavaScript Object Notation)**: The standard key-value text format for transferring data between frontend and backend.
* **PostgreSQL (Postgres)**: A powerful relational database that permanently stores our tables (users, workflows, counters, assigned slots).
* **Environment Variables (`.env`)**: Secret configuration settings (database passwords, API keys, port numbers) stored safely in a private file.

---

## ⚡ 4. Asynchronous & Messaging Concepts

* **Asynchronous (Async)**: Code that runs in the background without freezing the rest of your server.
* **Producer & Consumer (Message Queues)**:
  * **Producer**: Express pushes user registration messages onto RabbitMQ.
  * **Consumer**: Ayana's Go service pulls registration messages from RabbitMQ.
* **RabbitMQ / Redis**: Temporary holding queue between Express and Go.
* **WebSockets**: A persistent, 2-way live connection between browser/phone and Express for pushing instant notifications.

---

## 🔐 5. Unified Roles & Contextual Permission Model in Buffer

In Buffer, there are **no separate user account types** (no rigid "Admin account" vs "Staff account"). Every person registers as a single normal **User Account**. A user's role depends entirely on their context in a workflow:

1. **Workflow Creator (Admin)**: When a user creates a new workflow, they automatically become the **Owner / Admin** of that specific workflow (can edit stages, required documents, queue rules, and view analytics).
2. **Stage Operator (Staff)**: A user can join/be assigned to operate a specific counter/stage (e.g. Counter 2: Fee Payment) to call next tokens, verify documents, and complete stages.
3. **Queue Participant (User)**: A user can scan a QR code to join an active queue, receiving digital tokens, required document lists, and live position/ETA updates via WebSocket.
