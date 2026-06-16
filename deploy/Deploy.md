1. Copy binary release webserver to `/usr/local/bin/yt-dlp-webui`
2. Create `/opt/yt-dlp-webui/` and place the `serve/` folder there
3. Copy the `/deploy/yt-dlp-webui.service` file to `/etc/systemd/system/`
4. Start the service with 
```bash 
sudo systemctl enable --now yt-dlp-webui
```