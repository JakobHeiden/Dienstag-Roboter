#!/bin/bash
set -e

mvn clean package -q
scp -q target/Dienstag-1.0-SNAPSHOT.jar server:~/dienstag/
scp -q dienstag.service server:/etc/systemd/system/
ssh server 'systemctl daemon-reload && systemctl restart dienstag'

echo "Deployed successfully"