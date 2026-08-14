# yt-dlp-webui

A minimal web UI for [yt-dlp](https://github.com/yt-dlp/yt-dlp). Paste a URL, pick video or audio, and download it directly to your device.

## Requirements

- [yt-dlp](https://github.com/yt-dlp/yt-dlp) installed and available on `PATH`

## Usage

```
cargo run --release
```

The server listens on `http://0.0.0.0:8080` by default.

## Configuration

Copy `config.example.toml` to `config.toml` and adjust as needed.

## License

Copyright (C) 2026 Yannick Wegel

Licensed under the [GNU Affero General Public License v3.0](LICENSE.md).

This project is free to use, modify, and share. If you build something with it, I would love for your changes to stay
open too. I am curious what people find lacking, what they change, and what they come up with to make it better. The
AGPL ensures that, even if you run a modified version as a web service without distributing it directly.
