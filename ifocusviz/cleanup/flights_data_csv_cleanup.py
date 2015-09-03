"""
Simple clean up script for data downloaded from
Bureau of Transportation Statistics website.
This script assumes there's a header and the only two columns are:
"ORIGIN_STATE_ABR","DEP_DELAY"
"""

import csv

if __name__ == "__main__":
    dirty_file = open("../data/states_dep_delay_apr.csv")
    # skip header
    dirty_file.readline()

    csv_file = open("../data/cleaned_states_dep_delay_apr.csv", "w")

    csv_writer = csv.writer(csv_file, delimiter=',')
    for line in dirty_file:
        line_data = line.split(',')
        if not (line_data[0] and line_data[1]):
            continue
        csv_writer.writerow([line_data[0].strip('"'),
                             int(float(line_data[1]))])

    csv_file.close()
    dirty_file.close()

