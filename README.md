# Telegram ↔ Bale Bridge Bot

A public, multi-tenant bridge bot for forwarding messages between Telegram and Bale groups, channels, and DMs.

## Features

- Users can link their own Telegram and Bale groups/channels via a verification code flow.
- Each user can register multiple groups/channels and pair them independently.
- Full isolation: messages only flow within a user's own pairs.
- Two-way bridging for DMs (optional), groups, and channels.
- Supports text, photo, document, and video forwarding.
- SQLite persistence via `aiosqlite`.
- Logging: console + rotating file.

## Requirements

- Python 3.10+
- Telegram bot token from [BotFather](https://t.me/BotFather)
- Bale bot token from Bale's bot creator

Install dependencies:

```sh
pip install aiogram==3.* Balethon aiosqlite pyyaml python-dotenv
```

## Setup

1. Fill out the `.env` file with your bot tokens and options:

    ```
    TELEGRAM_TOKEN=your-telegram-bot-token
    BALE_TOKEN=your-bale-bot-token
    MIRROR_DMS_TO_OWNER=false
    OWNER_TG_CHAT_ID=0
    OWNER_BALE_CHAT_ID=0
    DB_PATH=bridge_public.db
    BALE_POLL_INTERVAL=1.0
    ```

2. Run the bot:

    ```sh
    python bridge_public.py
    ```

3. Talk to the bot in Telegram or Bale DMs. Use `/help` for commands.

## Usage

- Use the bot's menu to link your groups/channels and pair them for two-way forwarding.
- Verification codes are single-use, 10 min TTL, and bound to (platform, user_id).
- Loops are prevented: messages sent by the bot itself are ignored.

## Security

- Verification codes are single-use and expire after 10 minutes.
- Messages are only forwarded between chats linked and paired by the same user.

## License

MIT

---

See [bridge_public.py](bridge_public.py) for implementation