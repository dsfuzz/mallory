#!/bin/bash

# Get input parameters
subject=$1 # The subject to analyze
fuzzers=$2 # The fuzzers to analyze
runs=$3    # Total number of runs
logdir=$4  # The full log directory to analyze after multiple runs

# Set global variables
cutoff=2147483647                              # default: the maximum 32-bit integer
datacsv=$logdir/$subject/$subject"_events.csv" # Collect and store the events from all runs
outcsv=$logdir/$subject"_data.csv"             # Visualized data
outfig=$logdir/$subject"_data.png"             # Visualized figure

extract_events_from_logs() {
    isubject=$1
    iruns=$2
    ilogdir=$3
    ocsvfile=$4

    subject_log_dir="$ilogdir"/"$isubject"
    # Extract the data from the event file
    echo "Fuzzer,Time,Runs,EventType,EventNum" >>"$ocsvfile"

    # Traverse the log directory and extract the events from the log files
    for fuzzer in ${fuzzers[*]}; do
        for ((run = 1; run <= "$iruns"; run++)); do

            logfile="$subject_log_dir"/"$isubject"_"$fuzzer"_events_"$run".log
            tempfile="$subject_log_dir"/"$isubject"_"$fuzzer"_temp_"$run".txt

            echo "Processing $logfile ..."

            # Extract the events from the log file
            ag "Unified: " "$logfile" | cut -d ":" -f3 | awk 'NR % 3 == 0' >"$tempfile"

            # Calculate the line number
            event_line=0
            while read -r line; do
                # Get the line number
                cum_unique=$(echo "$line" | cut -d "/" -f1 | cut -d " " -f1)
                cum_pairs=$(echo "$line" | cut -d "/" -f2 | cut -d " " -f2)
                {
                    echo "$fuzzer",$event_line,$run,"Unique,$cum_unique"
                    echo "$fuzzer",$event_line,$run,"Pairs,$cum_pairs"
                } >>"$ocsvfile"
                event_line=$((event_line + 1))
            done <"$tempfile"

            # Extract the state number from the log file
            ag "Most hit states" "$logfile" | grep -o "[0-9]\+ unique" | cut -d " " -f1 | awk 'NR % 3 == 0' >"$tempfile"

            # Calculate the line number
            state_line=0
            while read -r line; do
                echo "$fuzzer",$state_line,$run,"States,$line" >> "$ocsvfile"
                state_line=$((state_line + 1))
            done <"$tempfile"

            # Update the cutoff
            # assert event_line == state_line
            if [ "$event_line" -lt "$cutoff" ]; then
                cutoff="$event_line"
            fi

            # Remove the event file
            rm -rf "$tempfile"
        done
    done

    echo "Data extracted from $subject done."
}

visualize_events() {
    isubject=$1
    idatafile=$2
    icutoff=$3
    iruns=$4
    odatafile=$5
    odatafig=$6

    python3 /host/tests/jepsen/scripts/generate_event_plot.py --subject "$isubject" --tools "${fuzzers[*]}" --data_csv "$idatafile" --cut_off "$icutoff" --runs "$iruns" --output_csv "$odatafile" --output_fig "$odatafig"
}

# Extract the events from the log file and output the mean value into the data file
echo "Extracting data of $subject ..."
rm -rf "$datacsv"
extract_events_from_logs "$subject" "$runs" "$logdir" "$datacsv"

# Visualize the final data
echo "Visualizing data for $subject ..."
rm -rf "$outcsv" "$outfig"
visualize_events "$subject" "$datacsv" "$cutoff" "$runs" "$outcsv" "$outfig"
