#!/bin/bash

SERVICE_NAME="app_consultor_vendas.service"

case "$1" in
    start)
        echo "Starting $SERVICE_NAME..."
        sudo systemctl start $SERVICE_NAME
        ;;
    stop)
        echo "Stopping $SERVICE_NAME..."
        sudo systemctl stop $SERVICE_NAME
        ;;
    restart)
        echo "Restarting $SERVICE_NAME..."
        sudo systemctl restart $SERVICE_NAME
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