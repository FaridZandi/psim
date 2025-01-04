#!/bin/bash

# Maximum allowed memory usage in GB
MAX_MEMORY=10
PYTHON_NAME="python3"
WHAT_TO_KILL="python3"


if [ $# -eq 1 ]; then
  MAX_MEMORY=$(($1))
fi

if [ $# -eq 2 ]; then
  MAX_MEMORY=$(($1))
  WHAT_TO_KILL=$2
fi

while true; do
  # Get total memory usage in GB
  memory_usage=$(free -g | awk '/^Mem:/ {print $3}')

  # Check if memory usage exceeds the limit
  if (( memory_usage > MAX_MEMORY )); then
    echo "Memory usage is $memory_usage GB, exceeding the limit of $MAX_MEMORY GB."
    echo "Killing all $WHAT_TO_KILL processes..."

    # Kill all python3 processes
    killall $WHAT_TO_KILL
    killall $PYTHON_NAME

    echo "All $WHAT_TO_KILL processes killed."
    exit 0
  # else
    # echo "Memory usage is $memory_usage GB, within limit of $MAX_MEMORY GB."
  fi

  # Wait for 10 seconds before checking again
  sleep 1
done

