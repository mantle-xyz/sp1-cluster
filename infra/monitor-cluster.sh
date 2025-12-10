#!/bin/bash
# SP1 Cluster Monitoring Script - monitor which worker (CPU/GPU) is processing tasks

echo "=== SP1 Cluster Monitor ==="
echo ""

# Function to extract and format coordinator stats
function show_stats() {
    echo "📊 Cluster Statistics:"
    docker compose -f docker-compose.test.yml logs coordinator --tail 100 2>/dev/null | \
    grep "GetStatsResponse" | tail -1 | \
    sed 's/.*GetStatsResponse {//' | \
    sed 's/}//' | \
    tr ',' '\n' | \
    grep -E "tasks|workers|utilization|queue|proofs" | \
    sed 's/^[ \t]*/  /' | \
    sed 's/: /: \t/'
    echo ""
}

# Function to show worker details
function show_workers() {
    echo "👷 Registered Workers:"
    docker compose -f docker-compose.test.yml logs coordinator --tail 100 2>/dev/null | \
    grep "worker unknown_" | tail -2 | \
    while read line; do
        if [[ $line == *"Cpu"* ]]; then
            worker_id=$(echo "$line" | grep -o "unknown_[a-f0-9]*")
            tasks=$(echo "$line" | grep -o " [0-9]* \[\]" | awk '{print $1}')
            echo "  🖥️  CPU Worker: $worker_id (Active Tasks: $tasks)"
        elif [[ $line == *"Gpu"* ]]; then
            worker_id=$(echo "$line" | grep -o "unknown_[a-f0-9]*")
            tasks=$(echo "$line" | grep -o " [0-9]* \[\]" | awk '{print $1}')
            echo "  🎮 GPU Worker: $worker_id (Active Tasks: $tasks)"
        fi
    done
    echo ""
}

# Function to show GPU usage
function show_gpu() {
    echo "🎮 GPU Status:"
    if command -v nvidia-smi &> /dev/null; then
        nvidia-smi --query-gpu=index,name,utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits | \
        awk -F', ' '{printf "  GPU %s: %s | Utilization: %s%% | Memory: %s/%s MB\n", $1, $2, $3, $4, $5}'
    else
        echo "  nvidia-smi not available"
    fi
    echo ""
}

# Function to monitor task assignment in real-time
function watch_tasks() {
    echo "📝 Watching for task assignments (press Ctrl+C to stop)..."
    echo "   Looking for: Task dispatched, Task completed, etc."
    echo ""
    docker compose -f docker-compose.test.yml logs -f coordinator cpu-node gpu0 2>/dev/null | \
    grep --line-buffered -E "Task|Executing|Completed|dispatched|assigned|worker" | \
    while read line; do
        timestamp=$(date '+%H:%M:%S')
        if [[ $line == *"Cpu"* ]] || [[ $line == *"cpu-node"* ]]; then
            echo "[$timestamp] 🖥️  CPU: $line"
        elif [[ $line == *"Gpu"* ]] || [[ $line == *"gpu0"* ]]; then
            echo "[$timestamp] 🎮 GPU: $line"
        else
            echo "[$timestamp] ℹ️  : $line"
        fi
    done
}

# Main menu
case "${1:-stats}" in
    stats)
        show_stats
        show_workers
        show_gpu
        ;;
    watch)
        watch_tasks
        ;;
    full)
        while true; do
            clear
            echo "=== SP1 Cluster Live Monitor (refreshing every 5s) ==="
            echo ""
            show_stats
            show_workers
            show_gpu
            echo "Press Ctrl+C to stop"
            sleep 5
        done
        ;;
    *)
        echo "Usage: $0 [stats|watch|full]"
        echo ""
        echo "  stats  - Show current cluster statistics (default)"
        echo "  watch  - Watch task assignments in real-time"
        echo "  full   - Full dashboard with auto-refresh"
        echo ""
        exit 1
        ;;
esac

