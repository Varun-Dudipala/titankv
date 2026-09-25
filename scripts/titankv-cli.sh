#!/bin/bash
# Command-line client: ./scripts/titankv-cli.sh localhost:9001 [command args...]
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_FILE=$(ls -t "$SCRIPT_DIR"/../target/titankv-*.jar 2>/dev/null | grep -v original | head -1)
if [ -z "$JAR_FILE" ]; then
    echo "Build first: mvn package -DskipTests" >&2
    exit 1
fi
exec java -cp "$JAR_FILE" com.titankv.cli.TitanKVCli "${@:-localhost:9001}"
