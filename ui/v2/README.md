# Plakar UI

Plakar UI is available at the private repository [plakar-ui](https://github.com/plakarkorp/plakar-ui). This repository only stores the compiled build version of the UI.

To synchronize the build assets with this repository, use the provided `sync.sh` script.

## Development

To sync the Plakar UI build from your local filesystem, use the `--local` flag with `sync.sh`:

```bash
$ cd ~/dev/plakar/plakar-ui
$ npm run build
$ ./sync.sh --local ~/dev/plakar/plakar-ui
$ cat ./frontend/VERSION
version: local
commit: 4e5613ea839beba564d57cdad97664bf24d9b582
date: 2024-11-12T14:41:26+0100
```

## Production build

For syncing the UI build from the latest Plakar UI release on GitHub, use the `--release` flag. Optionally, specify a release version if needed:

```bash
$ ./sync.sh --release
$ cat ./frontend/VERSION
version: 0.1.0
commit: 4e5613ea839beba564d57cdad97664bf24d9b582
date: 2024-11-12T14:41:26+0100
```