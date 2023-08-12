#! /usr/bin/env python3

import argparse
from matplotlib import pyplot as plt
import pandas as pd

# Define global variables
schedule_size = 12    # The number of steps in one schedule
step_time = 2.5       # The time(in seconds) for one step
event_types = ['Unique', 'Pairs', 'States']


def main(subject, fuzzers, data_csv, cut_off, runs, output_csv, output_fig):
    # Read the results from the data csv file
    # Format: Fuzzer, Time, Runs, EventType, EventNum
    df = pd.read_csv(data_csv)

    # Calculate the mean value for events
    # Store in a list temporarily for efficiency
    mean_list = []

    # Traverse fuzzers which is one array passed from the shell script
    fuzzer_list = fuzzers.split()
    fuzzer_list.sort()
    for fuzzer in fuzzer_list:
        for event_type in event_types:
            df1 = df[(df['Fuzzer'] == fuzzer) & (
                df['EventType'] == event_type)]

            # Format: Subject, Fuzzer, EventType, EventNum, Time
            for step in range(0, cut_off + 1, schedule_size):
                run_count = 0
                event_total = 0

                for run in range(1, runs + 1):
                    # get run-specific data frame
                    df2 = df1[df1['Runs'] == run]

                    # Collect all rows given a cutoff time
                    df3 = df2[df2['Time'] <= step]

                    # Update the total event number and run count
                    event_total += df3.tail(1)['EventNum'].values[0]
                    run_count += 1

                # Add a new row to the mean list
                mean_list.append((subject, fuzzer, event_type,
                                 event_total / run_count, step * step_time))

    # Convert the mean list to a data frame
    mean_df = pd.DataFrame(mean_list, columns=[
                           'Subject', 'Fuzzer', 'EventType', 'EventNum', 'Time'])

    # Save the mean data to the csv file
    print("Saving mean logs into file: ", output_csv)
    mean_df.to_csv(output_csv, index=False)

    fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(20, 10))
    fig.suptitle('Event number analysis of ' + subject)

    for key, grp in mean_df.groupby(['Fuzzer', 'EventType']):
        if key[1] == 'Unique':
            axes[0].plot(grp['Time'], grp['EventNum'], label=key[0])
            axes[0].set_xlabel('#Times (seconds)')
            axes[0].set_ylabel('#Unique events')
        elif key[1] == 'Pairs':
            axes[1].plot(grp['Time'], grp['EventNum'], label=key[0])
            axes[1].set_xlabel('#Times (seconds)')
            axes[1].set_ylabel('#Event pairs')
        elif key[1] == 'States':
            axes[2].plot(grp['Time'], grp['EventNum'], label=key[0])
            axes[2].set_xlabel('#Times (seconds)')
            axes[2].set_ylabel('#Unique states')

    for ax in axes:
        ax.legend(fuzzer_list, loc='upper left')
        ax.grid(linestyle='--', linewidth=0.5)

    # plt.show()
    plt.savefig(output_fig)
    print("Saving figure into file: ", output_fig)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--subject', type=str,
                        required=True, help="Subject name")
    parser.add_argument('-t', '--tools', type=str,
                        required=True, help="Tools to be tested")
    parser.add_argument('-d', '--data_csv', type=str,
                        required=True, help="Data csv file")
    parser.add_argument('-o', '--cut_off', type=int, required=True,
                        help="Cut-off time in schedule (x0.5 seconds))")
    parser.add_argument('-r', '--runs', type=int, required=True,
                        help="Number of runs in the experiment")
    parser.add_argument('-c', '--output_csv', type=str,
                        required=True, help="Output csv file")
    parser.add_argument('-f', '--output_fig', type=str,
                        required=True, help="Output figure file")
    args = parser.parse_args()
    main(args.subject, args.tools, args.data_csv,
         args.cut_off, args.runs, args.output_csv, args.output_fig)
