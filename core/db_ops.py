import json
import asyncpg

def configure_db():
    """
    Prompts the user for database connection details and stores them in a .DBConfig file.
    """
    print("=== Database Configuration ===")

    # Prompt for required connection fields
    host = input("Enter database host (e.g., localhost): ").strip()
    port = input("Enter database port (e.g., 5432): ").strip()
    username = input("Enter username: ").strip()
    password = input("Enter password: ").strip()
    database = input("Enter database name: ").strip()

    # Construct configuration dictionary
    config = {
        "host": host,
        "port": port,
        "user": username,
        "password": password,
        "database": database,
    }

    # Optional: confirm before saving
    print("\nConfiguration:")
    for key, value in config.items():
        print(f"{key}: {value}")
    confirm = input("\nSave this configuration? (y/n): ").lower()
    if confirm != "y":
        print("Configuration not saved.")
        return

    # Write config to .DBConfig file in JSON format
    try:
        with open(".DBConfig", "w") as config_file:
            json.dump(config, config_file, indent=4)
        print("Configuration saved successfully to .DBConfig")
    except Exception as e:
        print(f"Failed to write configuration file: {e}")

async def get_db_config():
    """
    Reads the database connection details from the .DBConfig file.
    """
    try:
        with open(".DBConfig", "r") as config_file:
            config = json.load(config_file)
            return config
    except FileNotFoundError:
        print("❌ Configuration file not found.")
    except Exception as e:
        print(f"❌ Error reading configuration file: {e}")
    return None

async def test_db():
    """
    Tests the database connection using the configuration stored in .DBConfig.
    """
    try:
        with open(".DBConfig", "r") as config_file:
            config = json.load(config_file)
    except FileNotFoundError:
        print("❌ Configuration file not found.")
        return
    except Exception as e:
        print(f"❌ Error reading configuration file: {e}")
        return

    try:
        conn = await asyncpg.connect(**config)
        print("✅ Database connection successful.")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")