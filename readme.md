# awsl-pic-pipeline

Migrate pics to Telegram storage via awsl-telegram-storage service.

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_URL` | MySQL connection string | required |
| `MIGRATION_LIMIT` | Max pics per run | 100 |
| `AWSL_STORAGE_URL` | awsl-telegram-storage URL | required |
| `AWSL_STORAGE_API_TOKEN` | API token | required |
| `ENABLE_DELETE` | Delete pics with broken URLs | false |

## Usage

```bash
python start.py
```
