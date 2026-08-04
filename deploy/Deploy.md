# Deploy
Written for ubuntu deployments, other OS can differ

## Requirements

### yt-dlp
Install yt-dlp with pipx, to allow impersonation with python `curl-cffi`:
```bash
sudo apt install pipx

# Older Ubuntu version ship a obsolete pipx version, which does not support the --global argument to install yt-dlp for all users. Update to newest version and remove the obsolete apt package
pipx ensurepath                     # adds ~/.local/bin to path
pipx install pipx                   # installs latest pipx to ~/.local/bin
sudo ~/.local/bin/pipx install pipx --global  # installs latest pipx to /usr/local/bin
sudo pipx ensurepath --global       # add latest pipx global path
sudo apt purge --autoremove pipx    # remove apt package


# Continue with yt-dlp install
sudo pipx install "yt-dlp[default,curl-cffi]" --global # Install yt-dlp to /usr/local/bin
```

## Install webui and enable service:
1. Copy binary release webserver to `/usr/local/bin/yt-dlp-webui`
2. Create `/opt/yt-dlp-webui/` and place the `serve/` folder there
3. Optionally create a config.toml in the `/opt/yt-dlp-webui/` to configure your server
4. Copy the `/deploy/yt-dlp-webui.service` file to `/etc/systemd/system/`
5. Start the service with

```bash 
sudo systemctl enable --now yt-dlp-webui
```