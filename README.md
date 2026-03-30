# MiniSQL — Lightweight Relational Database Engine in C++17

A fully functional mini relational database built from scratch in C++. It implements core concepts used in real-world systems like **PostgreSQL**, **MySQL**, and **SQLite**.

**Project Highlights**
- Dual storage engines: **Heap** + **B+ Tree**
- Primary + Secondary indexing
- SQL Parser with support for DDL & DML
- Rule-Based Query Optimizer (RBO)
- Interactive CLI 

Built as a deep learning project to understand database internals.

## Demo
MiniSQL Demo
<img src="/docs/demo.gif" width="800" alt="MiniSQL Demo">

##  Features

### Storage Layer
- **Heap Storage Engine**: Slotted-page file format optimized for fast full-table scans and sequential inserts.
- **B+ Tree Storage Engine**: Balanced tree with page splitting, merging, and borrowing. Leaf nodes contain full records (**clustered index**). Supports efficient point lookups and range scans.

### Indexing
- **Primary Index**: Clustered B+ Tree on the primary key (unique).
- **Secondary Indexes**: Non-clustered B+ Trees that store primary key references (RID) to the actual records.
- Automatic index selection for `WHERE` conditions.

### SQL Parser
- Hand-written **recursive-descent parser** supporting:
  - `CREATE TABLE` (with `PRIMARY KEY`)
  - `INSERT INTO` (single & multi-row)
  - `SELECT` with `WHERE` clause
  - `UPDATE` with `WHERE` clause
  - `DELETE` with `WHERE` clause
- Data types: `INT`, `TEXT`, `BOOL`

### Query Optimizer (Rule-Based)
- Index selection (chooses the best index when available)
- Predicate pushdown
- Projection pushdown

### CLI Interface
- Interactive shell with multi-line input support
- Pretty formatted table output
- Built-in commands: `SHOW TABLE_NAME`, `SHOW DATABASE`, `QUIT`


##  Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/nafeelxhussain-sys/miniSQL.git
cd miniSQL

# 2. Create and enter build directory
mkdir build && cd build

# 3. Configure with CMake
cmake .. -DCMAKE_BUILD_TYPE=Release

# 4. Build the project
cmake --build . --config Release

# 5. Run MiniSQL
./bin/minisql
```

##  Demo Queries
```bash

-- 1. Create a table with Primary Key
CREATE TABLE students (
    id INT PRIMARY KEY,
    name TEXT,
    age INT,
    active BOOL
);

-- 2. Insert multiple rows
INSERT INTO students VALUES 
    (1, 'Alice', 20, true),
    (2, 'Bob', 22, true),
    (3, 'Charlie', 19, false);

-- 3. Show all tables
SHOW database;

-- 4. Show students table
SHOW students;

-- 5. Query using Primary Key 
SELECT * FROM students WHERE id = 2;

-- 6. Query using Secondary Index
SELECT * FROM students WHERE age > 18;

-- 7. Update operation
UPDATE students SET active = false WHERE id = 3;

-- 8. Delete operation
DELETE FROM students WHERE id = 3;

-- 9. Final check
SELECT * FROM students;
```
## 📁 Project Structure
```markdown

```bash
## 📁 Project Structure

```bash
miniSQL/
├── src/                          # All source code
│   ├── main.cpp
│   ├── storage/                  # (*.h + *.cpp)
│   ├── index/                    # (*.h + *.cpp)
│   ├── parser/                   # (*.h + *.cpp)
│   ├── optimizer/                # (*.h + *.cpp)
│   ├── executor/                 # (*.h + *.cpp)
│   └── utility/                  # (*.h + *.cpp)
├── test/
│   ├── queries/                  # All test SQL files
│   │   ├── 01_create_table.sql
│   │   ├── 02_insert.sql
│   │   ├── 03_select_basic.sql
│   │   ├── 04_where_clause.sql
│   │   ├── 05_create_index.sql
│   │   ├── 06_update.sql
│   │   ├── 07_delete.sql
│   │   └── 08_show_commands.sql
│   ├── run_tests.py              # Cross-platform test runner
│   └── README.md
├── docs/                         # Architecture and design documents
│   ├── architecture.md
│   ├── storage_engine.md
│   ├── indexing.md
│   ├── query_optimizer.md
│   └── demo.gif
├── data/                         # database files
├── CMakeLists.txt
└── README.md


