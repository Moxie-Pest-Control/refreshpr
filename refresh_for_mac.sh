#!/bin/bash
# Open a new Terminal window and run the Python script
osascript -e 'tell application "Terminal" to do script "python3 main.py; exec $SHELL"'
