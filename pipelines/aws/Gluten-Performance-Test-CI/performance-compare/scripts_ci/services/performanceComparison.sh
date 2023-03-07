#!/bin/bash

BASELINE_TAG=$1
TARGET_TAG=$2
BATCH=$3
RESULT_DIR=$4

echo "BASELINE_TAG $BASELINE_TAG, TARGET_TAG $TARGET_TAG, BATCH $BATCH, RESULT_DIR $RESULT_DIR"

HOST=localhost
PORT=9000
DATABASE=performance_comparison

client=(clickhouse-client --host ${HOST} --port ${PORT} --database ${DATABASE} --date_time_input_format=best_effort -mn --input_format_tsv_use_best_effort_in_schema_inference true)

function init_env {
  clickhouse-client --host ${HOST} --port ${PORT} -mn --query "create database if not exists $DATABASE;"

  "${client[@]}" --query "
    create table if not exists query_runs (
      commit String,
      test String,
      query_index UInt32,
      query_id String,
      query_duration_ms float,
      update_time DateTime DEFAULT now()
    )Engine=MergeTree()
    partition by toYYYYMM(update_time)
    order by commit;

    create table if not exists query_logs (
      commit String ,
      test String,
      query_id String,
      query_display_name String,
      query_index UInt32,
      query_duration_ms UInt64,
      ProfileEvents Map(String, UInt64),
      memory_usage UInt64,
      update_time DateTime DEFAULT now()
    )Engine=MergeTree()
    partition by toYYYYMM(update_time)
    order by (commit,query_display_name);


    CREATE TABLE if not exists query_metrics_v2
    (
        event_date Date,
        event_time DateTime,
        pr_number UInt8,
        old_sha String,
        new_sha String,
        test String,
        query_index Int32,
        query_display_name String,
        metric String,
        old_value Float32,
        new_value Float32,
        diff Float32,
        stat_threshold Float32
    )
    ENGINE = MergeTree
    ORDER BY tuple()
  "
  echo "init compare env end."
}

function upload_to_ch {
  echo "begin upload to ch."

  "${client[@]}" --query "

    insert into query_logs
      select
        '$TARGET_TAG',
        test,
        query_id,
        query_display_name,
        query_index,
        query_duration_ms,
        events,
        memory_usage,
        now()
      from input
        ('test String,
          query_id String,
          query_display_name String,
          query_index UInt32,
          query_duration_ms UInt64,
          events Map(String, UInt64),
          memory_usage UInt64')
          settings input_format_tsv_use_best_effort_in_schema_inference ='true'
          FORMAT TSV

  " <${RESULT_DIR}/query-logs.tsv

  "${client[@]}" --query "

    insert into query_runs
      select
        '$TARGET_TAG',
        test,
        query_index,
        query_id,
        query_duration_ms,
        now()
      from input
        ('test String,
          query_index UInt32,
          query_id String,
          query_duration_ms float') FORMAT TSV

  " <${RESULT_DIR}/query-runs.tsv

  echo "end upload to ch."
}

function compare() {
  original_data_dir=$RESULT_DIR/compare/analyze/make

  rm -rf $RESULT_DIR/compare
  mkdir -p $original_data_dir

  "${client[@]}" --query "select 1,query_id,query_duration_ms,ProfileEvents,memory_usage from query_logs where commit='$BASELINE_TAG' and test='$BATCH'" \
    >${original_data_dir}/query-logs.tsv

  "${client[@]}" --query "select test,query_index,query_id,1,query_duration_ms from query_runs where commit='$BASELINE_TAG' and test='$BATCH'" \
    >${original_data_dir}/query-runs.tsv

  #newpr
  "${client[@]}" --query "select 2,query_id,query_duration_ms,ProfileEvents,memory_usage from query_logs where commit='$TARGET_TAG' and test='$BATCH'" \
    >>${original_data_dir}/query-logs.tsv

  "${client[@]}" --query "select test,query_index,query_id,2,query_duration_ms from query_runs where commit='$TARGET_TAG' and test='$BATCH'" \
    >>${original_data_dir}/query-runs.tsv

  "${client[@]}" --query "select distinct test,query_index,query_display_name from query_logs where commit='$TARGET_TAG' and test='$BATCH' " \
    >${original_data_dir}/query-display-names.tsv

  touch ${original_data_dir}/partial-queries.tsv
  #  echo "" > ${original_data_dir}/partial-queries.tsv #todo

  "${client[@]}" --query "select '$BATCH',0.05;">${original_data_dir}/report-thresholds.tsv

  cp ${RESULT_DIR}/run-errors.tsv $RESULT_DIR/compare/

  echo "tag:" $BASELINE_TAG > $RESULT_DIR/compare/left-commit.txt
  echo "tag:" $TARGET_TAG > $RESULT_DIR/compare/right-commit.txt

  echo "init compare data end."

  cp compare.sh report.py $RESULT_DIR/compare/
  cp eqmed.sql $RESULT_DIR/compare/analyze/make/

  $RESULT_DIR/compare/compare.sh
}

init_env
upload_to_ch
compare
