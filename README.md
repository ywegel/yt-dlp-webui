# yt-dlp-webui

A minimal web UI for [yt-dlp](https://github.com/yt-dlp/yt-dlp). Paste a URL, pick video or audio, and download it directly to your device.

## Legal

Downloading copyrighted content without permission may be illegal in your country. Only use this tool for content you have the right to download.

If you run this as a service, make sure it is only accessible to people you trust. Do not expose it to the public internet. Run it in a private network or put it behind a login.

## Requirements

- [yt-dlp](https://github.com/yt-dlp/yt-dlp) installed and available on `PATH`

## Usage

```
cargo run --release
```

The server listens on `http://0.0.0.0:8080` by default.

## Deployment

For instructions on how to deploy this as a systemd service on Ubuntu, see [deploy/Deploy.md](deploy/Deploy.md).

## Configuration

Copy `config.example.toml` to `config.toml` and adjust as needed.

## License

Copyright (C) 2026 Yannick Wegel

Licensed under the [GNU Affero General Public License v3.0](LICENSE.md).

This project is free to use, modify, and share. If you build something with it, I would love for your changes to stay
open too. I am curious what people find lacking, what they change, and what they come up with to make it better. The
AGPL ensures that, even if you run a modified version as a web service without distributing it directly.
