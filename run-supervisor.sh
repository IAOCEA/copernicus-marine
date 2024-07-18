#!/usr/bin/env bash

echo "running the supervisor as: $(whoami)"
/usr/bin/supervisord -c /etc/supervisor/supervisord.conf
