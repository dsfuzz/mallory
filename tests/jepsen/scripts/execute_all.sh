#!/bin/bash

# ./execute_all.sh 2 35 /host/tests/scripts/logs

# Get input parameters
exec_num=$1         # The number of executions
run_time=$2         # The time limit for each execution(in seconds)
timeout=$3          # The timeout for mediator (in seconds) 
                    # (in case the jepsen test fails to contact with mediator, and the mediator will be stuck)
log_dir=$4          # The full log directory to analyze after multiple runs (without the last slash)
analysis_type=$5    # The type of analysis to perform

# If the number of input parameters is not correct, print the usage
if [ "$#" -ne 5 ]; then
    echo "Usage: ./execute_all.sh <exec_num> <run_time> <timeout> <log_dir> <analysis_type>"
    echo "analysis_type = event | branch"
    exit 1
fi

fuzzers=("Jepsen")
schedules=("noop")
feedbacks=("event_history")

subjects=("dqlite" "braft" "redisraft" "tikv" "scylladb" "mongodb")

# Create the log directory if it does not exist
rm -rf "$log_dir"
mkdir -p "$log_dir"

# Build the mediator
cd /host/mediator &&
    apt-get install musl-tools -y &&
    rustup target add x86_64-unknown-linux-musl &&
    RUSTFLAGS="-C target-cpu=native" cargo build --release --target=x86_64-unknown-linux-musl

# Traverse the subjects
for ((subject_idx = 0; subject_idx < "${#subjects[@]}"; subject_idx++)); do
    subject="${subjects[$subject_idx]}"
    subject_log_dir="$log_dir"/"$subject"
    rm -rf "$subject_log_dir"
    mkdir -p "$subject_log_dir"

    # Traverse the fuzzers
    for ((i = 1; i <= "$exec_num"; i++)); do
        for ((fuzzer_idx = 0; fuzzer_idx < "${#fuzzers[@]}"; fuzzer_idx++)); do
            fuzzer="${fuzzers[$fuzzer_idx]}"

            # Start the mediator
            schedule="${schedules[$fuzzer_idx]}"
            feedback="${feedbacks[$fuzzer_idx]}"
            
            cd /host/mediator && timeout "$timeout" ./target/x86_64-unknown-linux-musl/release/mediator "$schedule" "$feedback" 0.7 &
            # Wait for the mediator to start
            sleep 20

            # Run subjects
            # If the subject is dqlite
            if [ "$subject" = "dqlite" ]; then
                cd /host/tests/jepsen/"$subject" && lein run test --workload append --nemesis all --time-limit "$run_time" --test-count 1
            # If the subject is braft
            elif [ "$subject" = "braft" ]; then
                cd /host/tests/jepsen/"$subject" && lein run test --workload wr-register --time-limit "$run_time" --test-count 1
            # If the subject is redisraft
            elif [ "$subject" = "redisraft" ]; then
                cd /host/tests/jepsen/"$subject" && lein run test --workload append --nemesis all --follower-proxy --time-limit "$run_time" --test-count 1
            # If the subject is scylladb
            elif [ "$subject" = "scylladb" ]; then
                cd /host/tests/jepsen/"$subject" && lein run test --workload list-append --nemesis all --time-limit "$run_time" --test-count 1
            # If the subject is mongodb
            elif [ "$subject" = "mongodb" ]; then
                cd /host/tests/jepsen/"$subject" && lein run test --workload list-append --nemesis all  --nodes-file ~/nodes --time-limit "$run_time" --test-count 1 --sharded
            # If the subject is tikv
            elif [ "$subject" = "tikv" ]; then
                cd /host/tests/jepsen/"$subject" && apt-get install pkg-config -qy && make build-client-rust-server && lein run test --workload list-append --nemesis all --time-limit "$run_time" --test-count 1
            fi

            # Wait for the mediator to stop
            wait

            # Move the log file to the log directory
            mv /tmp/events.log "$subject_log_dir"/"$subject"_"$fuzzer"_events_"$i".log

            # Sleep for half hour to recover service
            sleep 1800
        done
    done

    # Generate bug reports after all execution is done
    for exec_dir in /host/tests/jepsen/"$subject"/store/*/*; do
        # For files beginning with "n", generate bug reports
        for node_dir in "$exec_dir"/n*; do
            cov_server_log="$node_dir"/cov-server.log
            bug_report="$node_dir"/bug-report.log

            # Checking if cov-server.log exists
            if [ ! -f "$cov_server_log" ]; then
                continue
            fi

            ag "BUG DETECTION" "$cov_server_log" >"$bug_report"
        done
    done

    # Analyze the log files and generate the figures after one subject is done
    # pass subject, fuzzers, run times and log_dir to analysis.sh
    if [ "$analysis_type" = "event" ]; then
        /host/tests/scripts/jepsen/analysis_events.sh "$subject" "${fuzzers[*]}" "$exec_num" "$log_dir"
    elif [ "$analysis_type" = "branch" ]; then
        /host/tests/scripts/jepsen/analysis_branch.sh "$subject" "${fuzzers[*]}" "$exec_num" "$log_dir"
    fi
done
