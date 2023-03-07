import os

import numpy as np

from common import config


def write_tsv(dirs, result):
    query_runs = ""
    query_logs = ""
    query_error = ""
    keys = list(result.keys())
    keys.sort()

    for i, sql_name in enumerate(keys):
        query_index = i + 1
        # query_display_names += '{}\t{}\t{}\t{}\n'.format(config.COMMIT_ID, config.BATCH, query_index, sql_name)

        for v in result[sql_name]:
            exception = v["exception"]

            if exception is not None:
                query_error += '{}\n'.format(exception)
                continue

            query_id = v["query_id"]
            start_time = v["start_time"]
            profile = v["profile"]
            client_time = profile["client_time"]
            query_runs += '{}\t{}\t{}\t{}\n'.format(config.BATCH, query_index, query_id,
                                                    client_time)
            query_logs += '{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(config.BATCH, query_id, sql_name,
                                                                query_index,
                                                                client_time, to_profile_string(profile),
                                                                profile["memory_usage"])

    with open(dirs + os.sep + "query-logs.tsv", "w") as query_logs_tsv:
        query_logs_tsv.write(query_logs)

    with open(dirs + os.sep + "query-runs.tsv", "w") as query_runs_tsv:
        query_runs_tsv.write(query_runs)

    with open(dirs + os.sep + "run-errors.tsv", "w") as query_error_txt:
        query_error_txt.write(query_error)


def to_profile_string(profile):
    r = "{"
    for key in profile.keys():
        if len(r) != 1:
            r += ","
        r += "'{}':{}".format(key, profile[key])

    r += "}"
    return r


def write_csv(dirs, result):
    query_details = ""
    keys = list(result.keys())
    keys.sort()
    aggregated_format = '{},{},{},{},{}\n'
    query_aggregated = aggregated_format.format("name", "avg_response_time", "median_response_time",
                                                "min_response_time",
                                                "max_response_time")

    for i, sql_name in enumerate(keys):
        response_times = []
        for response in result[sql_name]:
            if response["profile"]["client_time"] > 0:
                response_times.append(response["profile"]["client_time"])

        max_response_time = np.amax(response_times, axis=0)
        min_response_time = np.amin(response_times, axis=0)
        median_response_time = np.median(response_times, axis=0)
        avg_response_time = np.average(response_times, axis=0)

        query_aggregated += aggregated_format.format(sql_name, avg_response_time, median_response_time,
                                                     min_response_time, max_response_time)

        query_details += sql_name + "," + ",".join([str(x) for x in response_times]) + "\n"

    with open(dirs + os.sep + "detail.csv", "w") as detail:
        detail.write(query_details)

    with open(dirs + os.sep + "aggregated.csv", "w") as aggregated:
        aggregated.write(query_aggregated)
