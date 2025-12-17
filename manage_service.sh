#!/bin/bash

SERVICE_NAME="copilot_prod.service"
APP_DIR="/home/vivaservicesai/htdocs/co-pilot/prod"
VENV_PYTHON="$APP_DIR/venv/bin/python"

case "$1" in
    start)
        echo "Initializing application..."
        cd $APP_DIR && $VENV_PYTHON initialize_app.py
        echo "Starting $SERVICE_NAME..."
        sudo systemctl start $SERVICE_NAME
        ;;
    stop)
        echo "Stopping $SERVICE_NAME..."
        sudo systemctl stop $SERVICE_NAME
        ;;
    restart)
        echo "Stopping $SERVICE_NAME..."
        sudo systemctl stop $SERVICE_NAME
        echo "Initializing application..."
        cd $APP_DIR && $VENV_PYTHON initialize_app.py
        echo "Starting $SERVICE_NAME..."
        sudo systemctl start $SERVICE_NAME
        ;;
    status)
        echo "Checking status of $SERVICE_NAME..."
        sudo systemctl status $SERVICE_NAME
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac

exit 0