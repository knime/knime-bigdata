#
# Wait until job server becomes available
#
TIMEOUT=$(expr $(date +'%s') + 120) # wait 120s
while [ "$(curl -k -s $JOB_SERVER_URL/healthz)" != "OK" ]; do
  log "Waiting for job server startup..."
  sleep 2

  if [ $(date +'%s') -gt $TIMEOUT ]; then
    log "Timeout waiting for job server."
    exit 1
  fi
done
