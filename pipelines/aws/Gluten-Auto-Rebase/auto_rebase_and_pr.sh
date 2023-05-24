#!/bin/bash

CURRENT_DATE=$(date +%Y%m%d)
GITHUB_TOKEN=${GITHUB_TOKEN}
PR_BASE=${PR_BASE:-main}
MANUAL_REBASE=${MANUAL_REBASE:-false}
REBASE_CH_BRANCH_NAME=${REBASE_CH_BRANCH_NAME:-rebase_ch/${CURRENT_DATE}}
REBASE_CH_COMMIT=${REBASE_CH_COMMIT}

function get_current_gluten_ch_version() {
  CURRENT_GLUTEN_CH_VERSION=$(curl -s https://raw.githubusercontent.com/oap-project/gluten/${PR_BASE}/cpp-ch/clickhouse.version)
  CH_ORG=$(echo "${CURRENT_GLUTEN_CH_VERSION}" | grep -oP '(?<=^CH_ORG=).*')
  CH_BRANCH=$(echo "${CURRENT_GLUTEN_CH_VERSION}" | grep -oP '(?<=^CH_BRANCH=).*')
  CH_COMMIT=$(echo "${CURRENT_GLUTEN_CH_VERSION}" | grep -oP '(?<=^CH_COMMIT=).*')
  echo "CH_ORG=${CH_ORG}, CH_BRANCH=${CH_BRANCH}, CH_COMMIT=${CH_COMMIT}"

  if [ -z "${CH_ORG}" ] || [ -z "${CH_BRANCH}" ] || [ -z "${CH_COMMIT}" ]; then
    echo "[ERROR] One or more of CH_ORG, CH_BRANCH, CH_COMMIT is empty, please check the file cpp-ch/clickhouse.version in gluten"
    exit 1
  fi

  export CH_ORG=${CH_ORG} && export CH_BRANCH=${CH_BRANCH} && export CH_COMMIT=${CH_COMMIT}
}

function auto_rebase_clickhouse() {
  [ -d "ClickHouse/.git" ] && rm -rf ClickHouse
  git clone -b "${CH_BRANCH}" https://github.com/Kyligence/ClickHouse.git

  cd ClickHouse
  git checkout -b "$REBASE_CH_BRANCH_NAME"
  git reset --hard "${CH_COMMIT}"

  # git rebase Clickhouse/ClickHouse master
  git remote add clickhouse https://github.com/ClickHouse/ClickHouse.git
  git fetch clickhouse master
  git rebase clickhouse/master

  # shellcheck disable=SC2181
  if [ $? -ne 0 ]; then
    echo "[ERROR] git rebase clickhouse/master failed, please fix the conflict and run this script again"
    exit 1
  fi
  # delete origin branch if exists
  BRANCH_EXISTS=$(git ls-remote --heads origin | grep -c "refs/heads/${REBASE_CH_BRANCH_NAME}")
  [ "${BRANCH_EXISTS}" -gt 0 ] && git push origin --delete "$REBASE_CH_BRANCH_NAME"

  git push origin "$REBASE_CH_BRANCH_NAME"
  # export commit, use short commit id
  REBASE_CH_COMMIT=$(git rev-parse --short HEAD)
  export REBASE_CH_COMMIT=${REBASE_CH_COMMIT}
  echo "auto rebase successfully, REBASE_CH_COMMIT=${REBASE_CH_COMMIT}"
  # shellcheck disable=SC2103
  cd ..
}

function modify_gluten_ch_version() {
  [ -d "gluten/.git" ] && rm -rf gluten
  git clone -b "${PR_BASE}" https://github.com/oap-project/gluten.git
  cd gluten

  git checkout -b "$REBASE_CH_BRANCH_NAME"
  if [ -z "${REBASE_CH_COMMIT}" ]; then
    echo "[ERROR] REBASE_CH_COMMIT is empty, please manually set it or check ERROR in auto_rebase_clickhouse"
    exit 1
  fi

  # modify clickhouse.version
  sed -i "s|CH_BRANCH=.*|CH_BRANCH=${REBASE_CH_BRANCH_NAME}|g" cpp-ch/clickhouse.version
  sed -i "s|CH_COMMIT=.*|CH_COMMIT=${REBASE_CH_COMMIT}|g" cpp-ch/clickhouse.version

  git add .
  git commit -m "[GLUTEN-1632][CH]Daily Update Clickhouse Version (${CURRENT_DATE})"

  git remote add kyligence https://github.com/Kyligence/gluten.git

  # delete origin branch if exists
  BRANCH_EXISTS=$(git ls-remote --heads kyligence | grep -c "refs/heads/${REBASE_CH_BRANCH_NAME}")
  [ "${BRANCH_EXISTS}" -gt 0 ] && git push kyligence --delete "$REBASE_CH_BRANCH_NAME"

  git push kyligence "$REBASE_CH_BRANCH_NAME"
  # shellcheck disable=SC2103
  cd ..

}

function auto_create_pr() {
  PR_TITLE="[GLUTEN-1632][CH]Daily Update Clickhouse Version (${CURRENT_DATE})"
  PR_BODY="Auto commit by gluten daily build, please check the build status and merge it if it's green."
  PR_HEAD="Kyligence:${REBASE_CH_BRANCH_NAME}"

  curl -L \
    -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    https://api.github.com/repos/oap-project/gluten/pulls \
    -d '{"title":"'"${PR_TITLE}"'", "body":"'"${PR_BODY}"'", "head":"'"${PR_HEAD}"'", "base":"'"${PR_BASE}"'"}' \
    > curl_response.json

  # shellcheck disable=SC2002
  PR_NUMBER=$(cat curl_response.json | grep -oP '(?<="number": ).*(?=,)')
  # check if PR is created successfully
  if [ -z "${PR_NUMBER}" ]; then
    # shellcheck disable=SC2028
    echo "[ERROR] Failed to create pr, more details bellow"
    cat curl_response.json
    exit 1
  fi
  echo "PR Created, PR_NUMBER=${PR_NUMBER}"
  echo "https://github.com/oap-project/gluten/pull/${PR_NUMBER}" > pr_url.txt
}

echo "MANUAL_REBASE=${MANUAL_REBASE}"
get_current_gluten_ch_version
[ "${MANUAL_REBASE}" = "false" ] && auto_rebase_clickhouse
modify_gluten_ch_version
auto_create_pr
