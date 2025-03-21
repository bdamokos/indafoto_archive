#!/bin/bash

# Function to display usage
usage() {
    echo "Usage: $0 {all|crawler|submitter|web}"
    echo "  all       - Monitor all logs"
    echo "  crawler   - Monitor crawler logs"
    echo "  submitter - Monitor archive submitter logs"
    echo "  web       - Monitor web interface logs"
    exit 1
}

# Check if tmux is installed
if ! command -v tmux &> /dev/null
then
    echo "tmux is required but not installed. Please install it first."
    exit 1
fi

# Create a new tmux session for monitoring
create_monitoring_session() {
    local session_name="indafoto_monitor"
    
    # Kill existing session if it exists
    tmux kill-session -t $session_name 2>/dev/null
    
    # Create new session
    tmux new-session -d -s $session_name
    
    # Split window for different logs
    tmux split-window -h
    tmux split-window -v
    
    # Configure panes for different logs
    tmux select-pane -t 0
    tmux send-keys "docker-compose logs -f crawler" C-m
    
    tmux select-pane -t 1
    tmux send-keys "docker-compose logs -f archive_submitter" C-m
    
    tmux select-pane -t 2
    tmux send-keys "docker-compose logs -f web" C-m
    
    # Attach to the session
    tmux attach-session -t $session_name
}

# Monitor specific service
monitor_service() {
    local service=$1
    docker-compose logs -f $service
}

# Main execution
case "$1" in
    "all")
        create_monitoring_session
        ;;
    "crawler")
        monitor_service crawler
        ;;
    "submitter")
        monitor_service archive_submitter
        ;;
    "web")
        monitor_service web
        ;;
    *)
        usage
        ;;
esac 