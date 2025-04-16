# 🔄 Bidirectional ClickHouse & Flat File Data Ingestion Tool

A web-based application for **bidirectional data ingestion** between a ClickHouse database and Flat File (CSV) data. The tool supports JWT-based authentication for ClickHouse, schema discovery, column selection, and reports the total number of records processed. Optional features include data preview, multi-table joins, and a progress bar.

---

## 📚 Table of Contents

- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [UI Walkthrough](#ui-walkthrough)
- [Test Cases](#test-cases)
- [Screenshots](#screenshots)
- [Error Handling](#error-handling)
- [Bonus Features](#bonus-features)
- [Resources](#resources)
- [Contributors](#contributors)
- [License](#license)

---

## ✨ Features

- 🔁 **Bidirectional Data Flow**: 
  - ClickHouse ➡️ Flat File (CSV)
  - Flat File ➡️ ClickHouse
- 🔒 **JWT Authentication** for ClickHouse
- ⚠️ **Error Handling with Friendly UI Feedback**

---

## 🧰 Tech Stack

| Layer       | Tech Used               |
|-------------|--------------------------|
| Backend     | Java (Spring Boot) or Go |
| Frontend    | HTML, CSS, JS(bootstrap) |
| Database    | ClickHouse (Docker)      |
| Auth        | JWT Token                |
| File Format | CSV                      |

---

## 🗂️ Project Structure

```plaintext
Clickhouse-Flatfile-Ingestion/
├── backend/                 # Java or Go backend logic
├── frontend/                # HTML/JS or React UI
├── test_data/               # Sample Flat Files (CSV)
├── images/                  # Screenshots or UI outputs
├── README.md
```

---

## 🚀 Getting Started

### Clone the Repo

```bash
git clone https://github.com/fidelis-coraro/Clickhouse-Flatfile-ingestion
cd Clickhouse-Flatfile-Ingestion
```

### Set Up ClickHouse (Docker)

```bash
docker run -d --name clickhouse-server -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server
```


### Backend Setup (Java Example)

```bash
cd backend
./mvnw clean install
java -jar target/ingestion-tool.jar
```

### Frontend (HTML or React)

- 🔘 Choose Source: `ClickHouse` or `Flat File`
- 🔐 Enter Credentials: Host, Port, DB, User, JWT Token
- 📋 List Tables/Headers and select columns
- ▶️ Click Start Ingestion
- ✅ Result: Total records processed or error shown

---

## ✅ Test Cases

| # | Scenario | Description |
|---|----------|-------------|
| 1 | ClickHouse ➡️ Flat File | Export selected columns |
| 2 | Flat File ➡️ ClickHouse | CSV upload to new table |
| 3 | Multi-table Join        | Join and export from ClickHouse |
| 4 | Auth Failure            | Test invalid token or user |
| 5 | Data Preview (Optional) | Show first 100 records |

---

## ⚠️ Error Handling

- Connection errors (bad host/user)
- Authentication errors (JWT issues)
- Data issues (schema mismatch, empty files)
- UI shows human-readable messages

---

## 🎁 Bonus Features

- 🧩 Multi-table JOIN support
- 🔍 Data Preview (first 100 records)
- 📈 Progress Bar (optional)

---

## 📚 Resources

- [ClickHouse Docs](https://clickhouse.com/docs)
- [Example Datasets](https://clickhouse.com/docs/en/getting-started/example-datasets)
- [ClickHouse Clients](https://github.com/ClickHouse)

---

## 👥 Contributors

- 👤 Fidelis Coraro


---


