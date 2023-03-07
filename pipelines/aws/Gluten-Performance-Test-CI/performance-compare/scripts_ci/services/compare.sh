#!/bin/bash
set -exu
set -o pipefail
trap "exit" INT TERM
# The watchdog is in the separate process group, so we have to kill it separately
# if the script terminates earlier.
trap 'kill $(jobs -pr) ${watchdog_pid:-} ||:' EXIT

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $script_dir
echo "script_dir" $script_dir


HOST=localhost
PORT=9000
DATABASE=performance_comparison

client=(clickhouse-client --host ${HOST} --port ${PORT} --database ${DATABASE} --date_time_input_format=best_effort)


# Build and analyze randomization distribution for all queries.
function analyze_queries
{

mkdir -p analyze/tmp ||:
mkdir -p report ||:
rm analyze/tmp/* analyze/*.log analyze/*.tsv analyze/*.txt report/* ||:



# for each query run, prepare array of metrics from query log
clickhouse-local --query "
create view query_runs as select * from file('analyze/make/query-runs.tsv', TSV,
    'test text, query_index int, query_id text, version UInt8, time float');

-- Separately process 'partial' queries which we could only run on the new server
-- because they use new functions. We can't make normal stats for them, but still
-- have to show some stats so that the PR author can tweak them.
create view partial_queries as select test, query_index
    from file('analyze/make/partial-queries.tsv', TSV,
        'test text, query_index int, servers Array(int)');

create table partial_query_times engine File(TSVWithNamesAndTypes,
        'analyze/partial-query-times.tsv')
    as select test, query_index, stddevPop(time) time_stddev, median(time) time_median
    from query_runs
    where (test, query_index) in partial_queries
    group by test, query_index
    ;

-- Process queries that were run normally, on both servers.
  create view query_logs as select *
    from file('analyze/make/query-logs.tsv', TSV,
    'version UInt8,
    query_id String,
    query_duration_ms UInt64,
    ProfileEvents Map(String, UInt64),
    memory_usage UInt64');

-- This is a single source of truth on all metrics we have for query runs. The
-- metrics include ProfileEvents from system.query_log, and query run times
-- reported by the perf.py test runner.
create table query_run_metric_arrays engine File(TSV, 'analyze/query-run-metric-arrays.tsv')
    as
    with (
        -- sumMapState with the list of all keys with '-0.' values. Negative zero is because
        -- sumMap removes keys with positive zeros.
        with (select groupUniqArrayArray(mapKeys(ProfileEvents)) from query_logs) as all_names
            select arrayReduce('sumMapState', [(all_names, arrayMap(x->-0., all_names))])
        ) as all_metrics
    select test, query_index, version, query_id,
        (finalizeAggregation(
            arrayReduce('sumMapMergeState',
                [
                    all_metrics,
                    arrayReduce('sumMapState',
                        [(mapKeys(ProfileEvents),
                            arrayMap(x->toFloat64(x), mapValues(ProfileEvents)))]
                    ),
                    arrayReduce('sumMapState', [(
                        ['client_time', 'server_time', 'memory_usage'],
                        arrayMap(x->if(x != 0., x, -0.), [
                            toFloat64(query_runs.time / 1000.),
                            toFloat64(query_duration_ms / 1000.),
                            toFloat64(memory_usage / 1000.)]))])
                ]
            )) as metrics_tuple).1 metric_names,
        metrics_tuple.2 metric_values
    from query_logs
    right join query_runs
        on query_logs.query_id = query_runs.query_id
            and query_logs.version = query_runs.version
    where (test, query_index) not in partial_queries
    ;

-- This is just for convenience -- human-readable + easy to make plots.
create table query_run_metrics_denorm engine File(TSV, 'analyze/query-run-metrics-denorm.tsv')
    as select test, query_index, metric_names, version, query_id, metric_values
    from query_run_metric_arrays
    array join metric_names, metric_values
    order by test, query_index, metric_names, version, query_id
    ;

-- Filter out tests that don't have an even number of runs, to avoid breaking
-- the further calculations. This may happen if there was an error during the
-- test runs, e.g. the server died. It will be reported in test errors, so we
-- don't have to report it again.
create view broken_queries as
    select test, query_index
    from query_runs
    group by test, query_index
    having count(*) % 2 != 0
    ;

-- This is for statistical processing with eqmed.sql
create table query_run_metrics_for_stats engine File(
        TSV, -- do not add header -- will parse with grep
        'analyze/query-run-metrics-for-stats.tsv')
    as select test, query_index, 0 run, version,
        -- For debugging, add a filter for a particular metric like this:
        -- arrayFilter(m, n -> n = 'client_time', metric_values, metric_names)
        --     metric_values
        -- Note that further reporting may break, because the metric names are
        -- not filtered.
        metric_values
    from query_run_metric_arrays
    where (test, query_index) not in broken_queries
    order by test, query_index, run, version
    ;

-- This is the list of metric names, so that we can join them back after
-- statistical processing.
create table query_run_metric_names engine File(TSV, 'analyze/query-run-metric-names.tsv')
    as select metric_names from query_run_metric_arrays limit 1
    ;
" 2> >(tee -a analyze/errors.log 1>&2)

# This is a lateral join in bash... please forgive me.
# We don't have arrayPermute(), so I have to make random permutations with
# `order by rand`, and it becomes really slow if I do it for more than one
# query. We also don't have lateral joins. So I just put all runs of each
# query into a separate file, and then compute randomization distribution
# for each file. I do this in parallel using GNU parallel.
( set +x # do not bloat the log
IFS=$'\n'
for prefix in $(cut -f1,2 "analyze/query-run-metrics-for-stats.tsv" | sort | uniq)
do
    file="analyze/tmp/${prefix//	/_}.tsv"
    grep "^$prefix	" "analyze/query-run-metrics-for-stats.tsv" > "$file" &
    printf "%s\0\n" \
        "clickhouse-local \
            --file \"$file\" \
            --structure 'test text, query text, run int, version UInt8, metrics Array(float)' \
            --query \"$(cat "$script_dir/analyze/make/eqmed.sql")\" \
            >> \"analyze/query-metric-stats.tsv\"" \
            2>> analyze/errors.log \
        >> analyze/commands.txt
done
wait
unset IFS
)

# The comparison script might be bound to one NUMA node for better test
# stability, and the calculation runs out of memory because of this. Use
# all nodes.
numactl --show
numactl --cpunodebind=all --membind=all numactl --show
# Use less jobs to avoid OOM. Some queries can consume 8+ GB of memory.
#jobs_count=$(($(grep -c ^processor /proc/cpuinfo) / 3))
jobs_count=8
numactl --cpunodebind=all --membind=all parallel --jobs  $jobs_count --joblog analyze/parallel-log.txt --null < analyze/commands.txt 2>> analyze/errors.log

clickhouse-local --query "
-- Join the metric names back to the metric statistics we've calculated, and make
-- a denormalized table of them -- statistics for all metrics for all queries.
-- The WITH, ARRAY JOIN and CROSS JOIN do not like each other:
--  https://github.com/ClickHouse/ClickHouse/issues/11868
--  https://github.com/ClickHouse/ClickHouse/issues/11757
-- Because of this, we make a view with arrays first, and then apply all the
-- array joins.
create view query_metric_stat_arrays as
    with (select * from file('analyze/query-run-metric-names.tsv',
        TSV, 'n Array(String)')) as metric_name
    select test, query_index, metric_name, left, right, diff, stat_threshold
    from file('analyze/query-metric-stats.tsv', TSV, 'left Array(float),
        right Array(float), diff Array(float), stat_threshold Array(float),
        test text, query_index int') reports
    order by test, query_index, metric_name
    ;

create table query_metric_stats_denorm engine File(TSVWithNamesAndTypes,
        'analyze/query-metric-stats-denorm.tsv')
    as select test, query_index, metric_name, left, right, diff, stat_threshold
    from query_metric_stat_arrays
    left array join metric_name, left, right, diff, stat_threshold
    order by test, query_index, metric_name
    ;
" 2> >(tee -a analyze/errors.log 1>&2)

# Fetch historical query variability thresholds from the CI database

set +x # Don't show password in the log

# Precision is going to be 1.5 times worse for PRs, because we run the queries
# less times. How do I know it? I ran this:
# SELECT quantilesExact(0., 0.1, 0.5, 0.75, 0.95, 1.)(p / m)
# FROM
# (
#     SELECT
#         quantileIf(0.95)(stat_threshold, pr_number = 0) AS m,
#         quantileIf(0.95)(stat_threshold, (pr_number != 0) AND (abs(diff) < stat_threshold)) AS p
#     FROM query_metrics_v2
#     WHERE (event_date > (today() - toIntervalMonth(1))) AND (metric = 'client_time')
#     GROUP BY
#         test,
#         query_index,
#         query_display_name
#     HAVING count(*) > 100
# )
#
# The file can be empty if the server is inaccessible, so we can't use
# TSVWithNamesAndTypes.

"${client[@]}" --query "
        select test, query_index,
            quantileExact(0.99)(abs(diff)) * 1.5 AS max_diff,
            quantileExactIf(0.99)(stat_threshold, abs(diff) < stat_threshold) * 1.5 AS max_stat_threshold,
            query_display_name
        from query_metrics_v2
        -- We use results at least one week in the past, so that the current
        -- changes do not immediately influence the statistics, and we have
        -- some time to notice that something is wrong.
        where event_date between now() - interval 1 month - interval 1 week
                and now() - interval 1 week
            and metric = 'client_time'
            and pr_number = 0
        group by test, query_index, query_display_name
        having count(*) > 100
        " > analyze/historical-thresholds.tsv
set -x
}

# Analyze results
function report
{
rm -r report ||:
mkdir report report/tmp ||:

#rm ./*.{rep,svg} test-times.tsv test-dump.tsv unstable.tsv unstable-query-ids.tsv unstable-query-metrics.tsv changed-perf.tsv unstable-tests.tsv unstable-queries.tsv bad-tests.tsv slow-on-client.tsv all-queries.tsv run-errors.tsv ||:

#build_log_column_definitions

cat analyze/errors.log >> report/errors.log ||:
cat profile-errors.log >> report/errors.log ||:

clickhouse-local --query "
create view query_display_names as select * from
    file('analyze/make/query-display-names.tsv', TSV,
        'test text, query_index int, query_display_name text')
    ;

create view partial_query_times as select * from
    file('analyze/partial-query-times.tsv', TSVWithNamesAndTypes,
        'test text, query_index int, time_stddev float, time_median double')
    ;

-- Report for partial queries that we could only run on the new server (e.g.
-- queries with new functions added in the tested PR).
create table partial_queries_report engine File(TSV, 'report/partial-queries-report.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as select toDecimal64(time_median, 3) time,
        toDecimal64(time_stddev / time_median, 3) relative_time_stddev,
        test, query_index, query_display_name
    from partial_query_times
    join query_display_names using (test, query_index)
    order by test, query_index
    ;

create view query_metric_stats as
    select * from file('analyze/query-metric-stats-denorm.tsv',
        TSVWithNamesAndTypes,
        'test text, query_index int, metric_name text, left float, right float,
            diff float, stat_threshold float')
    ;

create table report_thresholds engine File(TSVWithNamesAndTypes, 'report/thresholds.tsv')
    as select
        query_display_names.test test, query_display_names.query_index query_index,
        ceil(greatest(0.1, historical_thresholds.max_diff,
            test_thresholds.report_threshold), 2) changed_threshold,
        ceil(greatest(0.2, historical_thresholds.max_stat_threshold,
            test_thresholds.report_threshold + 0.1), 2) unstable_threshold,
        query_display_names.query_display_name query_display_name
    from query_display_names
    left join file('analyze/historical-thresholds.tsv', TSV,
        'test text, query_index int, max_diff float, max_stat_threshold float,
            query_display_name text') historical_thresholds
    on query_display_names.test = historical_thresholds.test
        and query_display_names.query_index = historical_thresholds.query_index
        and query_display_names.query_display_name = historical_thresholds.query_display_name
    left join file('analyze/make/report-thresholds.tsv', TSV,
        'test text, report_threshold float') test_thresholds
    on query_display_names.test = test_thresholds.test
    ;

-- Main statistics for queries -- query time as reported in query log.
create table queries engine File(TSVWithNamesAndTypes, 'report/queries.tsv')
    as select
        -- It is important to have a non-strict inequality with stat_threshold
        -- here. The randomization distribution is actually discrete, and when
        -- the number of runs is small, the quantile we need (e.g. 0.99) turns
        -- out to be the maximum value of the distribution. We can also hit this
        -- maximum possible value with our test run, and this obviously means
        -- that we have observed the difference to the best precision possible
        -- for the given number of runs. If we use a strict equality here, we
        -- will miss such cases. This happened in the wild and lead to some
        -- uncaught regressions, because for the default 7 runs we do for PRs,
        -- the randomization distribution has only 16 values, so the max quantile
        -- is actually 0.9375.
        abs(diff) > changed_threshold        and abs(diff) >= stat_threshold as changed_fail,
        abs(diff) > changed_threshold - 0.05 and abs(diff) >= stat_threshold as changed_show,

        not changed_fail and stat_threshold > unstable_threshold as unstable_fail,
        not changed_show and stat_threshold > unstable_threshold - 0.05 as unstable_show,

        left, right, diff, stat_threshold,
        query_metric_stats.test test, query_metric_stats.query_index query_index,
        query_display_names.query_display_name query_display_name
    from query_metric_stats
    left join query_display_names
        on query_metric_stats.test = query_display_names.test
            and query_metric_stats.query_index = query_display_names.query_index
    left join report_thresholds
        on query_display_names.test = report_thresholds.test
            and query_display_names.query_index = report_thresholds.query_index
            and query_display_names.query_display_name = report_thresholds.query_display_name
    -- 'server_time' is rounded down to ms, which might be bad for very short queries.
    -- Use 'client_time' instead.
    where metric_name = 'client_time'
    order by test, query_index, metric_name
    ;

create table changed_perf_report engine File(TSV, 'report/changed-perf.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as with
        -- server_time is sometimes reported as zero (if it's less than 1 ms),
        -- so we have to work around this to not get an error about conversion
        -- of NaN to decimal.
        (left > right ? left / right : right / left) as times_change_float,
        isFinite(times_change_float) as times_change_finite,
        toDecimal64(times_change_finite ? times_change_float : 1., 3) as times_change_decimal,
        times_change_finite
            ? (left > right ? '-' : '+') || toString(times_change_decimal) || 'x'
            : '--' as times_change_str
    select
        toDecimal64(left, 3), toDecimal64(right, 3), times_change_str,
        toDecimal64(diff, 3), toDecimal64(stat_threshold, 3),
        changed_fail, test, query_index, query_display_name
    from queries where changed_show order by query_display_name asc;

create table unstable_queries_report engine File(TSV, 'report/unstable-queries.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as select
        toDecimal64(left, 3), toDecimal64(right, 3), toDecimal64(diff, 3),
        toDecimal64(stat_threshold, 3), unstable_fail, test, query_index, query_display_name
    from queries where unstable_show order by query_display_name asc;



create table queries_metrics engine File(TSVWithNamesAndTypes, 'report/queries-metrics.tsv')
as select
        abs(diff) > changed_threshold        and abs(diff) >= stat_threshold as changed_fail,
        abs(diff) > changed_threshold - 0.05 and abs(diff) >= stat_threshold as changed_show,

        not changed_fail and stat_threshold > unstable_threshold as unstable_fail,
        not changed_show and stat_threshold > unstable_threshold - 0.05 as unstable_show,

        left, right, diff, stat_threshold,
        query_metric_stats.metric_name metric_name, query_metric_stats.query_index query_index,
        query_display_names.query_display_name query_display_name
    from query_metric_stats
    left join query_display_names
        on query_metric_stats.test = query_display_names.test
            and query_metric_stats.query_index = query_display_names.query_index
    left join report_thresholds
        on query_display_names.test = report_thresholds.test
            and query_display_names.query_index = report_thresholds.query_index
            and query_display_names.query_display_name = report_thresholds.query_display_name
    order by query_display_name, metric_name;

create view test_speedup as
    select
        metric_name,
        query_display_name,
        exp2(avg(log2(left / right))) times_speedup,
        count(*) queries,
        unstable + changed bad,
        sum(changed_show) changed,
        sum(unstable_show) unstable
    from queries_metrics
    group by metric_name,query_display_name
    order by times_speedup desc
    ;

create view total_speedup as
    select metric_names metric_name,query_display_name,times_speedup,queries,bad,changed,unstable
    from (
    select
        'Total' metric_names,
        a.metric_name query_display_name,
        exp2(avg(log2(times_speedup))) times_speedup,
        sum(queries) queries,
        unstable + changed bad,
        sum(changed) changed,
        sum(unstable) unstable
    from test_speedup a
    group by metric_names, query_display_name
    ) t
    ;

create table test_perf_changes_report engine File(TSV, 'report/test-perf-changes.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as with
        (times_speedup >= 1
            ? '-' || toString(toDecimal64(times_speedup, 3)) || 'x'
            : '+' || toString(toDecimal64(1 / times_speedup, 3)) || 'x')
        as times_speedup_str
    select metric_name,query_display_name, times_speedup_str, queries, bad, changed, unstable
    -- Not sure what's the precedence of UNION ALL vs WHERE & ORDER BY, hence all
    -- the braces.
    from (
        (
            select * from total_speedup
        ) union all (
            select * from test_speedup
            where
                (times_speedup >= 1 ? times_speedup : (1 / times_speedup)) >= 1.005
                or bad
        )
    )
    order by metric_name = 'Total' desc, query_display_name asc
    ;


--create view total_client_time_per_query as select *
--    from file('analyze/client-times.tsv', TSV,
--        'test text, query_index int, client float, server float');

--create table slow_on_client_report engine File(TSV, 'report/slow-on-client.tsv')
--    settings output_format_decimal_trailing_zeros = 1
--    as select client, server, toDecimal64(client/server, 3) p,
--        test, query_display_name
--    from total_client_time_per_query left join query_display_names using (test, query_index)
--    where p > toDecimal64(1.02, 3) order by p desc;

--create table wall_clock_time_per_test engine Memory as select *
--    from file('wall-clock-times.tsv', TSV, 'test text, real float, user float, system float');

--create table test_time engine Memory as
--    select test, sum(client) total_client_time,
--        max(client) query_max,
--        min(client) query_min,
--        count(*) queries
--    from total_client_time_per_query full join queries using (test, query_index)
--    group by test;

create view query_runs as select * from file('analyze/make/query-runs.tsv', TSV,
    'test text, query_index int, query_id text, version UInt8, time float');

--
-- Guess the number of query runs used for this test. The number is required to
-- calculate and check the average query run time in the report.
-- We have to be careful, because we will encounter:
--  1) partial queries which run only on one server
--  2) short queries which run for a much higher number of times
--  3) some errors that make query run for a different number of times on a
--     particular server.
--


--create view test_times_view as
--    select
--        wall_clock_time_per_test.test test,
--        real,
--        total_client_time,
--        queries,
--        query_max,
--       real / if(queries > 0, queries, 1) avg_real_per_query,
--        query_min,
--        runs
--    from test_time
--        -- wall clock times are also measured for skipped tests, so don't
--        -- do full join
--        left join wall_clock_time_per_test
--            on wall_clock_time_per_test.test = test_time.test
--        full join test_runs
--            on test_runs.test = test_time.test
    ;


-- report for all queries page, only main metric
create table all_tests_report engine File(TSV, 'report/all-queries.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as with
        -- server_time is sometimes reported as zero (if it's less than 1 ms),
        -- so we have to work around this to not get an error about conversion
        -- of NaN to decimal.
        (left > right ? left / right : right / left) as times_change_float,
        isFinite(times_change_float) as times_change_finite,
        toDecimal64(times_change_finite ? times_change_float : 1., 3) as times_change_decimal,
        times_change_finite
            ? (left > right ? '-' : '+') || toString(times_change_decimal) || 'x'
            : '--' as times_change_str
    select changed_fail, unstable_fail,
        toDecimal64(left, 3), toDecimal64(right, 3), times_change_str,
        toDecimal64(isFinite(diff) ? diff : 0, 3),
        toDecimal64(isFinite(stat_threshold) ? stat_threshold : 0, 3),
        test, query_index, query_display_name
    from queries order by test, query_index;


-- Report of queries that have inconsistent 'short' markings:
-- 1) have short duration, but are not marked as 'short'
-- 2) the reverse -- marked 'short' but take too long.
-- The threshold for 2) is significantly larger than the threshold for 1), to
-- avoid jitter.



--------------------------------------------------------------------------------
-- various compatibility data formats follow, not related to the main report

-- keep the table in old format so that we can analyze new and old data together
create table queries_old_format engine File(TSVWithNamesAndTypes, 'queries.rep')
    as select 0 short, changed_fail, unstable_fail, left, right, diff,
        stat_threshold, test, query_display_name query
    from queries
    ;

-- new report for all queries with all metrics (no page yet)
create table all_query_metrics_tsv engine File(TSV, 'report/all-query-metrics.tsv') as
    select metric_name, left, right, diff,
        floor(left > right ? left / right : right / left, 3),
        stat_threshold, test, query_index, query_display_name
    from query_metric_stats
    left join query_display_names
        on query_metric_stats.test = query_display_names.test
            and query_metric_stats.query_index = query_display_names.query_index
    order by test, query_index;
" 2> >(tee -a report/errors.log 1>&2)

# Prepare source data for metrics and flamegraphs for queries that were profiled
# by perf.py.
#for version in {right,left}
#do
#    rm -rf data
#    clickhouse-local --query "
#create view query_profiles as
#    with 0 as left, 1 as right
#    select * from file('analyze/query-profiles.tsv', TSV,
#        'test text, query_index int, query_id text, version UInt8, time float')
#    where version = $version
#    ;
#
#create view query_display_names as select * from
#    file('analyze/make/query-display-names.tsv', TSV,
#        'test text, query_index int, query_display_name text')
#    ;
#
#create table unstable_query_runs engine File(TSVWithNamesAndTypes,
#        'unstable-query-runs.$version.rep') as
#    select query_profiles.test test, query_profiles.query_index query_index,
#        query_display_name, query_id
#    from query_profiles
#    left join query_display_names on
#        query_profiles.test = query_display_names.test
#        and query_profiles.query_index = query_display_names.query_index
#    ;
#
#create view query_log as select *
#    from file('$version-query-log.tsv', TSVWithNamesAndTypes,
#        '$(cat "$version-query-log.tsv.columns")');
#
#create table unstable_run_metrics engine File(TSVWithNamesAndTypes,
#        'unstable-run-metrics.$version.rep') as
#    select test, query_index, query_id, value, metric
#    from query_log
#    array join
#        mapValues(ProfileEvents) as value,
#        mapKeys(ProfileEvents) as metric
#    join unstable_query_runs using (query_id)
#    ;
#
#create table unstable_run_metrics_2 engine File(TSVWithNamesAndTypes,
#        'unstable-run-metrics-2.$version.rep') as
#    select
#        test, query_index, query_id,
#        v, n
#    from (
#        select
#            test, query_index, query_id,
#            ['memory_usage', 'read_bytes', 'written_bytes', 'query_duration_ms'] n,
#            [memory_usage, read_bytes, written_bytes, query_duration_ms] v
#        from query_log
#        join unstable_query_runs using (query_id)
#    )
#    array join v, n;
#
#create view trace_log as select *
#    from file('$version-trace-log.tsv', TSVWithNamesAndTypes,
#        '$(cat "$version-trace-log.tsv.columns")');
#
#create view addresses_src as select addr,
#        -- Some functions change name between builds, e.g. '__clone' or 'clone' or
#        -- even '__GI__clone@@GLIBC_2.32'. This breaks differential flame graphs, so
#        -- filter them out here.
#        [name, 'clone.S (filtered by script)', 'pthread_cond_timedwait (filtered by script)']
#            -- this line is a subscript operator of the above array
#            [1 + multiSearchFirstIndex(name, ['clone.S', 'pthread_cond_timedwait'])] name
#    from file('$version-addresses.tsv', TSVWithNamesAndTypes,
#        '$(cat "$version-addresses.tsv.columns")');
#
#create table addresses_join_$version engine Join(any, left, address) as
#    select addr address, name from addresses_src;
#
#create table unstable_run_traces engine File(TSVWithNamesAndTypes,
#        'unstable-run-traces.$version.rep') as
#    select
#        test, query_index, query_id,
#        count() value,
#        joinGet(addresses_join_$version, 'name', arrayJoin(trace))
#            || '(' || toString(trace_type) || ')' metric
#    from trace_log
#    join unstable_query_runs using query_id
#    group by test, query_index, query_id, metric
#    order by count() desc
#    ;
#
#create table metric_devation engine File(TSVWithNamesAndTypes,
#        'report/metric-deviation.$version.tsv')
#    settings output_format_decimal_trailing_zeros = 1
#    -- first goes the key used to split the file with grep
#    as select test, query_index, query_display_name,
#        toDecimal64(d, 3) d, q, metric
#    from (
#        select
#            test, query_index,
#            (q[3] - q[1])/q[2] d,
#            quantilesExact(0, 0.5, 1)(value) q, metric
#        from (select * from unstable_run_metrics
#            union all select * from unstable_run_traces
#            union all select * from unstable_run_metrics_2) mm
#        group by test, query_index, metric
#        having isFinite(d) and d > 0.5 and q[3] > 5
#    ) metrics
#    left join query_display_names using (test, query_index)
#    order by test, query_index, d desc
#    ;
#
#create table stacks engine File(TSV, 'report/stacks.$version.tsv') as
#    select
#        -- first goes the key used to split the file with grep
#        test, query_index, trace_type, any(query_display_name),
#        -- next go the stacks in flamegraph format: 'func1;...;funcN count'
#        arrayStringConcat(
#            arrayMap(
#                addr -> joinGet(addresses_join_$version, 'name', addr),
#                arrayReverse(trace)
#            ),
#            ';'
#        ) readable_trace,
#        count() c
#    from trace_log
#    join unstable_query_runs using query_id
#    group by test, query_index, trace_type, trace
#    order by test, query_index, trace_type, trace
#    ;
#" 2> >(tee -a report/errors.log 1>&2) &
#done
wait
#
## Create per-query flamegraphs
#touch report/query-files.txt
#IFS=$'\n'
#for version in {right,left}
#do
#    for query in $(cut -d'	' -f1-4 "report/stacks.$version.tsv" | sort | uniq)
#    do
#        query_file=$(echo "$query" | cut -c-120 | sed 's/[/	]/_/g')
#        echo "$query_file" >> report/query-files.txt
#
#        # Build separate .svg flamegraph for each query.
#        # -F is somewhat unsafe because it might match not the beginning of the
#        # string, but this is unlikely and escaping the query for grep is a pain.
#        grep -F "$query	" "report/stacks.$version.tsv" \
#            | cut -f 5- \
#            | sed 's/\t/ /g' \
#            | tee "report/tmp/$query_file.stacks.$version.tsv" \
#            | ~/fg/flamegraph.pl --hash > "$query_file.$version.svg" &
#    done
#done
#wait
#unset IFS
#
## Create differential flamegraphs.
#while IFS= read -r query_file
#do
#    ~/fg/difffolded.pl "report/tmp/$query_file.stacks.left.tsv" \
#            "report/tmp/$query_file.stacks.right.tsv" \
#        | tee "report/tmp/$query_file.stacks.diff.tsv" \
#        | ~/fg/flamegraph.pl > "$query_file.diff.svg" &
#done < report/query-files.txt
#wait
#
## Create per-query files with metrics. Note that the key is different from flamegraphs.
#IFS=$'\n'
#for version in {right,left}
#do
#    for query in $(cut -d'	' -f1-3 "report/metric-deviation.$version.tsv" | sort | uniq)
#    do
#        query_file=$(echo "$query" | cut -c-120 | sed 's/[/	]/_/g')
#
#        # Ditto the above comment about -F.
#        grep -F "$query	" "report/metric-deviation.$version.tsv" \
#            | cut -f4- > "$query_file.$version.metrics.rep" &
#    done
#done
#wait
#unset IFS
#
## Prefer to grep for clickhouse_driver exception messages, but if there are none,
## just show a couple of lines from the log.
#for log in *-err.log
#do
#    test=$(basename "$log" "-err.log")
#    {
#        # The second grep is a heuristic for error messages like
#        # "socket.timeout: timed out".
#        grep -h -m2 -i '\(Exception\|Error\):[^:]' "$log" \
#            || grep -h -m2 -i '^[^ ]\+: ' "$log" \
#            || head -2 "$log"
#    } | sed "s/^/$test\t/" >> run-errors.tsv ||:
#done
}

function report_metrics
{
rm -rf metrics ||:
mkdir metrics

clickhouse-local --query "
create table queries_metrics engine File(TSVWithNamesAndTypes, 'report/queries-metrics.tsv');

create table metrics engine File(TSV, 'metrics/metrics.tsv') as
    select concat(query_display_name,'-',metric_name) text, 1000, left,right
        from queries_metrics
    ;

create table changes engine File(TSV, 'metrics/changes.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as select metric_name, left, right,
        toDecimal64(diff, 3), toDecimal64(times_diff, 3)
    from (
        select metric_name, median(left) as left, median(right) as right,
            (right - left) / left diff,
            if(left > right, left / right, right / left) times_diff
        from queries_metrics
        group by metric_name
        having abs(diff) > 0.05 and isFinite(diff) and isFinite(times_diff)
    )
    order by diff desc
    ;
" 2> >(tee -a metrics/errors.log 1>&2)

IFS=$'\n'
for prefix in $(cut -f1 "metrics/metrics.tsv" | sort | uniq)
do
    file="metrics/$prefix.tsv"
    grep "^$prefix	" "metrics/metrics.tsv" | cut -f2- > "$file"

    gnuplot -e "
        set datafile separator '\t';
        set terminal png size 960,540;
        set xtics time format '%tH:%tM';
        set title '$prefix' noenhanced offset 0,-3;
        set key left top;
        plot
            '$file' using 1:2 with lines title 'Left'
            , '$file' using 1:3 with lines title 'Right'
            ;
    " \
        | convert - -filter point -resize "200%" "metrics/$prefix.png" &

done
wait
unset IFS
}

# Check that local and client are in PATH
clickhouse-local --version > /dev/null
clickhouse-client --version > /dev/null

analyze_queries
report

time "$script_dir/report.py" --report=all-queries > report/all-queries.html 2> >(tee -a report/errors.log 1>&2) ||:
time "$script_dir/report.py" > report/report.html

#report_metrics




