#!/bin/bash
# Start the container
docker-compose up -d

# Detect OS and open a new terminal window
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS: Open in a new Terminal.app window
    osascript -e 'tell app "Terminal" to do script "docker attach minisql"'
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux: Try common terminal emulators
    if command -v gnome-terminal >/dev/null; then
        gnome-terminal -- docker attach minisql
    elif command -v xterm >/dev/null; then
        xterm -e "docker attach minisql"
    else
        echo "Could not find a terminal emulator. Attaching here instead..."
        docker attach minisql
    fi
fi