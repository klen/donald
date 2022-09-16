#!/bin/bash

set -m

# Start the scheduler
python -m donald -M example scheduler &
  
# Start the first worker
python -m donald -M example worker &
  
# Start the second worker
python -m donald -M example worker &
  
# Wait for all process to exit
wait
  
# Exit with status of process that exited first
exit $?
