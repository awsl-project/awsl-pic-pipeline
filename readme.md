# awsl-pic-pipeline

Migrate pics to Telegram storage via awsl-telegram-storage service.

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_URL` | MySQL connection string | required |
| `MIGRATION_LIMIT` | Max awsl_ids per run | 50 |
| `AWSL_STORAGE_URL` | awsl-telegram-storage URL | required |
| `AWSL_STORAGE_API_TOKEN` | API token | required |
| `AWSL_STORAGE_CHAT_ID` | Target Telegram chat ID (optional) | - |
| `ENABLE_DELETE` | Delete invalid pics | false |

## Usage

```bash
python start.py
```
