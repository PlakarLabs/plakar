#!/bin/sh

set -e

cd `dirname $0`

usage() {
    echo "Usage: $0 --release [RELEASE_NUMBER] | --local PLAKAR_UI_PATH"
    exit 1
}

if [ "$#" -lt 1 ]; then
    usage
fi

# Initialize variables
mode=""
path=""
release_number="latest"

while [ "$#" -gt 0 ]; do
    case "$1" in
        --release)
            if [ -n "$mode" ]; then
                echo "Error: Only one of --release or --local can be specified."
                usage
            fi
            mode="release"
            if [ -n "$2" ] && [ "${2#-}" = "$2" ]; then
                release_number="$2"
                shift
            fi
            ;;
        --local)
            if [ -n "$mode" ]; then
                echo "Error: Only one of --release or --local can be specified."
                usage
            fi
            mode="local"
            if [ -z "$2" ] || [ "${2#-}" != "$2" ]; then
                echo "Error: --local requires a path argument."
                usage
            fi
            path="$2"
            shift
            ;;
        *)
            echo "Error: Invalid argument '$1'"
            usage
            ;;
    esac
    shift
done

if [ "$mode" = "release" ]; then
    echo "Downloading release: $release_number"
    # TODO

elif [ "$mode" = "local" ]; then
    if [ ! -d "$path" ]; then
        echo "Error: Path '$path' does not exist."
        exit 1
    fi

    if [ ! -d "$path/dist" ]; then
        echo "Error: before syncing, please build the UI using 'npm run build' in the UI directory."
        exit 1
    fi

	rm -rf -- ./frontend/
	cp -r ${path}/dist ./frontend

    cat<<EOF > ./frontend/VERSION
version: local
commit: $(git -C ${path} log -1 --format="%H")
date: $(date +"%Y-%m-%dT%H:%M:%S%z")
EOF

else
    usage
fi