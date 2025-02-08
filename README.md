# 🐘 TuskORM

## A Powerful, Async-First ORM for PostgreSQL
TuskORM is a **modern, general-purpose Object Relational Mapper (ORM)** built for **PostgreSQL**. Designed with **performance, scalability, and ease of use in mind**, TuskORM provides an **async-first** API while planning future support for synchronous execution.

🚀 **Fast, lightweight, and Pythonic.**
🛠 **Automatic migrations.**
🔄 **Async-first with future sync support.**
⚡ **Efficient query builder with chaining.**

---

## 🚀 Why TuskORM?
### ✅ **Async-First Performance**
Leverages `asyncpg` for high-performance, non-blocking database operations.

### ✅ **Automatic Migrations**
Schema changes? No problem. TuskORM handles migrations automatically.

### ✅ **Powerful Query API**
A Django-like query system with powerful filtering, ordering, and aggregation support.

### ✅ **Future Sync Support**
Initially designed for async execution, but sync mode (`psycopg2`) will be added soon.

---

## 📦 Installation
TuskORM uses **Poetry** for package management.

```sh
pip install poetry  # If you haven't installed Poetry yet
poetry add tuskorm
```

Or, if you're cloning from source:
```sh
git clone https://github.com/ihavemadefire/tuskorm.git
cd tuskorm
poetry install
```

---

## ⚡ Quick Start
### Define a Model
```python
from tuskorm import Model, IntegerField, CharField

class User(Model):
    id = IntegerField(primary_key=True)
    name = CharField(max_length=100)
```

### Perform Queries
```python
users = await User.filter(name="Alice").order_by("-id").all()
```

### Create a Record
```python
await User.create(name="Bob")
```

### Automatic Migrations
```sh
poetry run tuskorm migrate
```

---

## 🔧 Roadmap
✅ **Async query execution with `asyncpg`**  
✅ **Model system with Django-like syntax**  
✅ **Automatic migrations**  
🛠 **Sync support using `psycopg2`**  
🛠 **Relationships (`ForeignKey`, `ManyToMany`, `OneToOne`)**  
🛠 **Transactions & connection pooling**  
🛠 **Admin interface (optional)**  

---

## 🤝 Contributing
Want to make TuskORM even better? Contributions are welcome!

```sh
git clone https://github.com/ihavemadefire/tuskorm.git
cd tuskorm
poetry install
```

1. Fork the repository
2. Create a new branch (`git checkout -b feature-branch`)
3. Commit your changes (`git commit -m "Add new feature"`)
4. Push to the branch (`git push origin feature-branch`)
5. Open a Pull Request 🎉

---

## 📝 License
TuskORM is open-source and available under the **MIT License**.

---

## 🌟 Stay Connected
- **GitHub**: [github.com/yourusername/tuskorm](https://github.com/ihavemadefire/tuskorm)
- **Issues**: [Report a Bug](https://github.com/ihavemadefire/tuskorm/issues)
- **Contribute**: [Submit a PR](https://github.com/ihavemadefire/tuskorm/pulls)

🔥 **TuskORM – The Future of PostgreSQL ORMs Starts Here.**

