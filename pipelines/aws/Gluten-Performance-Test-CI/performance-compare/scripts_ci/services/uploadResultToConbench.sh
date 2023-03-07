#!/bin/ksh

sudo apt install -y curl

#GlutenWithCHStandard_2023_01_15_18_03_24/
result_dir=$(cat /tmp/result_dir)
if [ -z "${result_dir}" ];then
  echo "$(date '+%F %T'): result dir not found,conbench upload failed"
  exit 160
fi

# /tmp/trigger m t e
# /tmp/gluten_prno m e
# /tmp/gluten_commit_ids t
# /tmp/libch_tag m
# /tmp/chbackend_commit_ids t e
# /tmp/spark_session_conf  m
# /tmp/starrocks_session_conf  m
# /tmp/service m t e

raw_token=$(curl -i -X 'POST' \
  'http://ec2-161-189-50-52.cn-northwest-1.compute.amazonaws.com.cn:5000/api/login/' \
  -H 'accept: */*' \
  -H 'Content-Type: application/json' \
  -d '{
  "email": "liang.huang@kyligence.io",
  "password": "123456",
  "remember_me": true
}'|grep remember_token)


raw_token_sub1=$(echo ${raw_token#*: })

login_token=$(echo ${raw_token_sub1%%;*})


cd "${result_dir}"|| exit 160


year=$(date +%Y)
run_trend_id=${year}
timestamp=$(date "+%Y-%m-%d %H:%M:%S")
dt=$(date "+%Y-%m-%d")

run_id=$(date +%s%N |md5sum | cut -c 1-32) # use 32 characters like https://conbench.ursa.dev/
run_name=${run_id}
batch_id=$(date +%s%N |md5sum | cut -c 1-32)
batch_tag=""
gluten_prno=""
commit_ids=""
chbackend_cids=""
run_reason=""
benchmark_context=""

libch_tag=$(cat /tmp/libch_tag) # testGroup in fact,not only for libch,also for sr and vanilla spark
service_type=$(cat /tmp/service)
benchmark_display_for_trend=${timestamp}_${service_type}
source_type=$(cat /tmp/source_type)

base_tag=""

trigger_method=$(cat /tmp/trigger)
if [ "${trigger_method}" == "mannual" ];then
  run_name=${run_id}_$(cat /tmp/libch_tag)

  gluten_prno=$(cat /tmp/gluten_prno)
  if [ -z ${gluten_prno} ];then
    gluten_prno=-1
  fi

  batch_tag=${libch_tag}
  run_reason="${dt} mannual triggered ${service_type} test,${libch_tag}"

  if [ "${service_type}" == "GlutenWithCHStandard" ] || [ "${service_type}" == "VanillaSparkOptimized" ];then
    benchmark_context=$(cat /tmp/spark_session_conf)
  else
    benchmark_context=$(cat /tmp/starrocks_session_conf)
  fi
  benchmark_context=${benchmark_context//\"/} # rm all " in string ,or upload to conbench will fail

  base_tag=$(cat /tmp/base_tag)

elif [ "${trigger_method}" == "timer" ];then
  batch_tag=${dt}_timer_triggered
  gluten_prno=-1
  chbackend_cids=""
  cat chbackend_commit_ids | while read line
  do
    chbackend_cids=${chbackend_cids}${line}
  done

  gluten_cids=""
  cat gluten_commit_ids | while read line
  do
    gluten_cids=${gluten_cids}${line}
  done

  commit_ids=${gluten_cids}
  run_reason="${dt} timer triggered test"

  base_tag=$(clickhouse-client -d performance_comparison --query="select commit from query_logs where query_id=(select max(query_id) from query_logs where commit like '%timer_triggered%')")

else # event(PR)
  batch_tag=${run_id}_b_event_triggered_$(date "+%H:%M:%S")
  gluten_prno=$(cat /tmp/gluten_prno)
  if [ -z ${gluten_prno} ];then
    echo "$(date '+%F %T'): PRNO not found,something wrong,conbench upload failed"
    exit 160
  fi
  run_reason="${dt} pr triggered test"
fi


current_tag=${batch_tag}_${timestamp}
if [ -z ${base_tag} ];then
  base_tag=${current_tag}
fi

# replace " " with "_" in this two args
current_tag=${current_tag// /_}
base_tag=${base_tag// /_}


cd "${script_home}" # this is need by performanceComparison.sh
echo "$(date '+%F %T'): ${script_home}/performanceComparison.sh ${base_tag} ${current_tag}  tpch100 ${result_dir}"
${script_home}/performanceComparison.sh ${base_tag} ${current_tag}  tpch100 ${result_dir}
cd $(cat /tmp/result_dir) # cd back
pwd

echo "$(date '+%F %T'): copy these html reports to report home"
mkdir -p ${report_home}/${base_tag}____${current_tag} # baseline____contender
cp aggregated.csv compare/report/report.html compare/report/all-queries.html ${report_home}/${base_tag}____${current_tag}/
# ~/glutenTest/result/GlutenWithCHStandard_2023_02_07_14_58_19/compare/report
report_url=$(cat /tmp/driver_ip):5678/${base_tag}____${current_tag}/report.html
result_aggregated_csv_url=$(cat /tmp/driver_ip):5678/${base_tag}____${current_tag}/aggregated.csv

# load detail csv
declare -A detail_map
while read line
do
  detail_map[${line%%,*}]=${line#*,}
  # echo ${detail_map[${line%%,*}]}
done < detail.csv

# load agg csv
total_time=0
cat aggregated.csv | while read line
do
  query=${line%%,*}
  if [ ${query} == "name" ];then
    continue
  fi
  # echo "query:${query}"
  # echo "detail data:"${detail_map[${query}]}

  data=${line#*,}
  data_arr=(${data//,/ })
  avg=${data_arr[1]}  # use median to represent mean value which displays in conbench benchmark "mean" column
  median=${data_arr[1]}
  min=${data_arr[2]}
  max=${data_arr[3]}

  # todo: iqr q1 q3 stdev zscore times

  total_time=$((${total_time} + ${avg}))

  if [[ "${trigger_method}" == "timer" ]];then
  # gen single run benchmarks
  curl -X 'POST' \
    'http://ec2-161-189-50-52.cn-northwest-1.compute.amazonaws.com.cn:5000/api/benchmarks/' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -H "Cookie: ${login_token}" \
      -d "{
      \"batch_id\": \"${batch_id}\",
      \"cluster_info\": {
        \"info\": {},
        \"name\": \"\",
        \"optional_info\": {}
      },
      \"context\": {\"benchmark_language\":\"scala & c++\", \"session_conf\":\"${benchmark_context}\", \"baseline_contender_comparison_report\":\"${report_url}\", \"result_aggregated_csv\":\"${result_aggregated_csv_url}\", \"chbackend_commit\":\"https://github.com/Kyligence/ClickHouse/commit/${chbackend_cids}\"},
      \"github\": {
        \"branch\": \"main\",
        \"commit\": \"${commit_ids}\",
        \"pr_number\": ${gluten_prno},
        \"repository\": \"oap-project/gluten\"
      },
      \"info\": {},
      \"is_step_change\": true,
      \"optional_benchmark_info\": {},
      \"run_id\": \"${run_id}\",
      \"run_name\": \"${run_name}\",
      \"run_reason\": \"${run_reason}\",
      \"stats\": {
        \"data\": [
          ${detail_map[${query}]}
        ],
        \"iqr\": 0,
        \"iterations\": 7,
        \"max\": ${max},
        \"mean\": ${avg},
        \"median\": ${median},
        \"min\": ${min},
        \"q1\": 0,
        \"q3\": 0,
        \"stdev\": 0,
        \"time_unit\": \"ms\",
        \"times\": [
          1
        ],
        \"unit\": \"ms\"
      },
      \"tags\": {\"name\":\"${batch_tag}\", \"display\":\"${query}\"},
      \"timestamp\": \"${timestamp}\",
      \"validation\": {}
    }"

    # gen yearly query trend benchmarks for timer triggered test

      curl -X 'POST' \
          'http://ec2-161-189-50-52.cn-northwest-1.compute.amazonaws.com.cn:5000/api/benchmarks/' \
          -H 'accept: application/json' \
          -H 'Content-Type: application/json' \
          -H "Cookie: ${login_token}" \
            -d "{
            \"batch_id\": \"${run_trend_id}_trend_${query}\",
            \"cluster_info\": {
              \"info\": {},
              \"name\": \"\",
              \"optional_info\": {}
            },
            \"context\": {\"benchmark_language\":\"scala & c++\", \"session_conf\":\"${benchmark_context}\", \"baseline_contender_comparison_report\":\"${report_url}\",  \"result_aggregated_csv\":\"${result_aggregated_csv_url}\", \"chbackend_commit\":\"https://github.com/Kyligence/ClickHouse/commit/${chbackend_cids}\"},
            \"github\": {
              \"branch\": \"main\",
              \"commit\": \"${commit_ids}\",
              \"pr_number\": ${gluten_prno},
              \"repository\": \"oap-project/gluten\"
            },
            \"info\": {},
            \"is_step_change\": true,
            \"optional_benchmark_info\": {},
            \"run_id\": \"${run_trend_id}\",
            \"run_name\": \"${run_trend_id}\",
            \"run_reason\": \"${year} query response trend\",
            \"stats\": {
              \"data\": [
                ${detail_map[${query}]}
              ],
              \"iqr\": 0,
              \"iterations\": 7,
              \"max\": ${max},
              \"mean\": ${avg},
              \"median\": ${median},
              \"min\": ${min},
              \"q1\": 0,
              \"q3\": 0,
              \"stdev\": 0,
              \"time_unit\": \"ms\",
              \"times\": [
                1
              ],
              \"unit\": \"ms\"
            },
            \"tags\": {\"name\":\"${run_trend_id}_trend_${query}\", \"display\":\"${benchmark_display_for_trend}\"},
            \"timestamp\": \"${timestamp}\",
            \"validation\": {}
          }"
    fi

    if [[ "${trigger_method}" == "mannual" ]];then
      # mannual_triggered_GlutenWithCHStandard_baseline_q01
      # q01_LocalNullableMergetree
      # put all benchmarks with the same group() and query no in one batch batch_tag=mannual_triggered_${service_type}_${libch_tag}
      timestamp_noblank=${timestamp// /_}
      curl -X 'POST' \
                'http://ec2-161-189-50-52.cn-northwest-1.compute.amazonaws.com.cn:5000/api/benchmarks/' \
                -H 'accept: application/json' \
                -H 'Content-Type: application/json' \
                -H "Cookie: ${login_token}" \
                  -d "{
                  \"batch_id\": \"${batch_tag}_${query}\",
                  \"cluster_info\": {
                    \"info\": {},
                    \"name\": \"\",
                    \"optional_info\": {}
                  },
                  \"context\": {\"benchmark_language\":\"scala & c++\", \"session_conf\":\"${benchmark_context}\", \"baseline_contender_comparison_report\":\"${report_url}\", \"result_aggregated_csv\":\"${result_aggregated_csv_url}\"},
                  \"github\": {
                    \"branch\": \"main\",
                    \"commit\": \"${commit_ids}\",
                    \"pr_number\": ${gluten_prno},
                    \"repository\": \"oap-project/gluten\"
                  },
                  \"info\": {},
                  \"is_step_change\": true,
                  \"optional_benchmark_info\": {},
                  \"run_id\": \"${batch_tag}\",
                  \"run_name\": \"${run_name}\",
                  \"run_reason\": \"${batch_tag}\",
                  \"stats\": {
                    \"data\": [
                      ${detail_map[${query}]}
                    ],
                    \"iqr\": 0,
                    \"iterations\": 7,
                    \"max\": ${max},
                    \"mean\": ${avg},
                    \"median\": ${median},
                    \"min\": ${min},
                    \"q1\": 0,
                    \"q3\": 0,
                    \"stdev\": 0,
                    \"time_unit\": \"ms\",
                    \"times\": [
                      1
                    ],
                    \"unit\": \"ms\"
                  },
                  \"tags\": {\"name\":\"${batch_tag}_${query}\", \"display\":\"${service_type}_${source_type}_${timestamp_noblank}\"},
                  \"timestamp\": \"${timestamp}\",
                  \"validation\": {}
                }"

    fi

done

echo "total_time:"${total_time}

if [[ "${trigger_method}" == "timer" ]];then
  # get total time and put it into a special "total time batch" of this year
  curl -X 'POST' \
          'http://ec2-161-189-50-52.cn-northwest-1.compute.amazonaws.com.cn:5000/api/benchmarks/' \
          -H 'accept: application/json' \
          -H 'Content-Type: application/json' \
          -H "Cookie: ${login_token}" \
            -d "{
            \"batch_id\": \"${run_trend_id}_trend_total\",
            \"cluster_info\": {
              \"info\": {},
              \"name\": \"\",
              \"optional_info\": {}
            },
            \"context\": {\"benchmark_language\":\"scala & c++\", \"session_conf\":\"${benchmark_context}\"},
            \"github\": {
              \"branch\": \"main\",
              \"commit\": \"${commit_ids}\",
              \"pr_number\": ${gluten_prno},
              \"repository\": \"oap-project/gluten\"
            },
            \"info\": {},
            \"is_step_change\": true,
            \"optional_benchmark_info\": {},
            \"run_id\": \"${run_trend_id}\",
            \"run_name\": \"${run_trend_id}\",
            \"run_reason\": \"query response trend\",
            \"stats\": {
              \"data\": [
                ${total_time}
              ],
              \"iqr\": 0,
              \"iterations\": 1,
              \"max\": ${total_time},
              \"mean\": ${total_time},
              \"median\": ${total_time},
              \"min\": ${total_time},
              \"q1\": 0,
              \"q3\": 0,
              \"stdev\": 0,
              \"time_unit\": \"ms\",
              \"times\": [
                1
              ],
              \"unit\": \"ms\"
            },
            \"tags\": {\"name\":\"${run_trend_id}_trend_total\", \"display\":\"${benchmark_display_for_trend}\"},
            \"timestamp\": \"${timestamp}\",
            \"validation\": {}
          }"
fi

cp /tmp/result_dir /tmp/trigger /tmp/gluten_prno /tmp/gluten_commit_ids /tmp/libch_tag /tmp/chbackend_commit_ids \
   /tmp/spark_session_conf /tmp/starrocks_session_conf /tmp/service /tmp/source_type \
   /tmp/vanilla_spark_url /tmp/emr_cluster_id /tmp/*_ip \
   ${result_dir}/
# clean args tmp file to avoid interference between different test runs
> /tmp/result_dir
> /tmp/trigger
> /tmp/gluten_prno
> /tmp/gluten_commit_ids
> /tmp/libch_tag
> /tmp/chbackend_commit_ids
> /tmp/spark_session_conf
> /tmp/starrocks_session_conf
> /tmp/service
> /tmp/source_type
