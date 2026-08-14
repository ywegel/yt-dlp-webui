# Deploy
Written for ubuntu deployments, other OS can differ

## Requirements

### yt-dlp
Install yt-dlp with pipx, to allow impersonation with python `curl-cffi`:
```bash
sudo apt install pipx

# Older Ubuntu versions ship an obsolete pipx that does not support --global argument to install yt-dlp for all users. 
# Update to newest version and remove the obsolete apt package. Run the following commands in a single shell session 
# without switching terminals.
pipx ensurepath                     # adds ~/.local/bin to path
pipx install pipx                   # installs latest pipx to ~/.local/bin
sudo ~/.local/bin/pipx install pipx --global  # installs latest pipx to /usr/local/bin
sudo pipx ensurepath --global       # add latest pipx global path
sudo apt purge --autoremove pipx    # remove apt package


# Continue with yt-dlp install
sudo pipx install "yt-dlp[default,curl-cffi]" --global # Install yt-dlp to /usr/local/bin
```

### ffmpeg
Optional, but recommended for yt-dlp to handle audio and video streams correctly
```bash
sudo apt install ffmpeg
```

## Install webui and enable service:
1. Copy binary release webserver to `/usr/local/bin/yt-dlp-webui`
2. Create `/opt/yt-dlp-webui/` and place the `serve/` folder from this repository there
3. Optionally create a `config.toml` in `/opt/yt-dlp-webui/` to configure your server
4. Create a dedicated system user to run the service as, and give it ownership of `/opt/yt-dlp-webui/` (the service writes `jobs.db` there):

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin yt-dlp-webui
sudo chown -R yt-dlp-webui:yt-dlp-webui /opt/yt-dlp-webui
```
5. Copy the `deploy/yt-dlp-webui.service` file to `/etc/systemd/system/`
6. Reload systemd so it picks up the new unit file:

```bash
sudo systemctl daemon-reload
```
7. Start the service with

```bash
sudo systemctl enable --now yt-dlp-webui
```
