#!/bin/bash
set -e
#set -x

#
# Environment settings and script settings
#
#TEMP_DIR=/data2/spark/tmp
#export TMP=$TEMP_DIR
#export TEMP=$TEMP_DIR
BASE_DIR=$(dirname $(readlink -m $(dirname $0i)))
CONF_DIR=$BASE_DIR/conf
LOG_DIR=$BASE_DIR/logs
JAR=$BASE_DIR/lib/dcc-release-client.jar
CONF_FILE=$CONF_DIR/application.yml

# File which stores information about release name of the last run
LASTRUN=$CONF_DIR/.lastrun

# Jobs configuration 
JOBS_CONF=$CONF_DIR/jobs.properties

DRIVER_LIB="/usr/lib/hadoop/lib/native"
DRIVER_MEM=30g

# Read projects
. $CONF_DIR/workflow_settings.sh

#
# Constants
#
# List of all available jobs
ALL_JOBS=(STAGE MASK ID IMAGE ANNOTATE JOIN FATHMM FI IMPORT SUMMARIZE DOCUMENT INDEX EXPORT)

#
# Global variables
#
# Name of release to run
RELEASE_NAME=
# Work directory
WORK_DIR=
# Submission data directory
DATA_DIR=
PRINT_HELP=NO
PROJECTS=
RUN_JOBS=
CORES=


#
# Functions
#
print_usage() {
  echo "Usage: `basename $0` [OPTIONS] RELEASE"
  echo "  or: `basename $0` -h"
  echo "Run DCC RELEASE."
  echo "For example: `basename $0` ICGC21"
  echo ""
  echo "Options:"
  echo "  -c    Nubmer of cores to use. Defaults to maximum allowed"
  echo "  -j    Jobs to run. Could be a comma-separated list or a jobs range"
  echo "  -p    A comma-separated list of projects to run the release for"
  echo "  -h    Print this message"
}


die() {
  message=$@
  echo $message
  exit 1
}

advise_help() {
  echo -n "Try '"
  echo -n "`basename $0` -h"
  echo "' for more information."
}

# Verifies the Perl like regex.
ensure_regex() {
  if [[ $# -ne 3 ]]; then
    die "Invalid 'ensure_regex' metod arguments $@"
  fi
  value=$1
  regex=$2
  error_message=$3

  # Disable error checking as grep produces errors when the regex doesn't match
  set +e
  $(echo "$value" | grep -q -P $regex)
  result=$?
  [[ $result -eq 0 ]] || die $error_message
  # Reset errors checking
  set -e
}

ensure_decimal() {
  ensure_regex $1 '^[0-9]+$' "$1 is not a decimal number"
}

verify_core() {
  if [[ -n $CORES ]]; then 
    ensure_decimal $CORES
  fi
}

verify_release_name() {
  re='^ICGC\d+$'
  ensure_regex $RELEASE_NAME $re "Release name '$RELEASE_NAME' doesn't match pattern '$re'"
  
}

resolve_arguments() {
  verify_core
  verify_release_name
}

parse_arguments() {
  args_num=$#
  # Verify at least release name is provided
  if [[ $# -lt 1 ]]; then
    echo "Missing release to run"
    advise_help
    exit 1
  fi

  # Parse args
  while [[ $# -gt 0 ]]; do
    key=$1
    case $key in
      -h)
        PRINT_HELP=YES
        shift
        ;;
      -j)
        shift
        [[ -n $1 ]] || die "Jobs list is not provided"
        RUN_JOBS=$1
        shift
        ;;
      -c)
        shift
        [[ -n $1 ]] || die "Number of cores is undefined"
        CORES=$1
        shift
        ;;
      -p)
        shift
        [[ -n $1 ]] || die "Projects are undefined"
        PROJECTS=$1
        shift
        ;;
      *)
       [[ -z $RELEASE_NAME ]] || die "Unrecognized argument '$key'"
       RELEASE_NAME=$1
       shift
       ;;
    esac
  done

  # Check if nothing more than print help requested
  if [[ "$PRINT_HELP" == "YES" ]]; then
    if [[ $args_num -gt 1 ]]; then
      echo "Unrecognized arguments combination."
      advise_help
      exit 1
    else
      print_usage
    fi
    exit 0
  fi

  # Verify that the provided argumens are correct
  resolve_arguments
}

resolve_data_dir() {
  DATA_DIR="/icgc/submission/${RELEASE_NAME}"
}

resolve_work_dir() {
  WORK_DIR="/icgc/release/${RELEASE_NAME}/0"
}

create_workdir() {
  hdfs dfs -test -e $WORK_DIR
  if [[ "$?" != "0" ]]; then
    echo Working directory $WORK_DIR does not exist. Creating...
    hdfs dfs -mkdir $WORK_DIR
  fi
}

is_valid_job() {
  job=$1
  for i in ${ALL_JOBS[@]}; do
    if [[ $i == $job ]]; then
      echo 0
      return
    fi
  done
  echo 1
}

append_jobs() {
  total=$1
  jobs=$2
  if [[ -z $jobs ]]; then
    echo $total
  else
    echo "${total},$jobs"
  fi
}

index_of_job() {
  job=$1
  index=0
  for i in ${ALL_JOBS[@]}; do
    if [[ $i == $job ]]; then
      echo $index
      return
    fi
    index=$(expr $index + 1)
  done
  die "Invalid job $job"
}

ensure_job_range_correctness() {
  start_job=$1
  end_job=$2
  start_index=$(index_of_job $start_job)
  end_index=$(index_of_job $end_job)
  [[ "$start_index" -lt "$end_index" ]] || die "Incorrect jobs range order"
}

resolve_job_range() {
  range=$1
  # Ensure range definition is correct
  $(echo $range | grep -q -P '^[A-Z]*-[A-Z]*$')
  [[ $? -eq 0 ]] || die "Failed to resolve jobs from job range $1"

  IFS='-' read -ra JOBS <<< "$range"
  start_job=${JOBS[0]}
  end_job=${JOBS[1]}
  if [[ -z $start_job ]]; then
    start_job=${ALL_JOBS[0]}
  else
    [[ $(is_valid_job $start_job) -eq 0 ]] || die "Invalid job $start_job"
  fi

  if [[ -z $end_job ]]; then
    end_job=${ALL_JOBS[-1]}
  else
    [[ $(is_valid_job $end_job) -eq 0 ]] || die "Invalid job $end_job"
  fi

  ensure_job_range_correctness $start_job $end_job

  jobs_index_range=$(seq $(index_of_job $start_job) $(index_of_job $end_job))
  jobs=
  for i in $jobs_index_range; do
    job=${ALL_JOBS[$i]}
    jobs=$(append_jobs $jobs $job)
  done
  echo $jobs
}

resolve_jobs() {
  if [[ -z $RUN_JOBS ]]; then
    RUN_JOBS=$(echo ${ALL_JOBS[@]} | tr ' ' ,)
  else
    jobs=
    # Loop through all the requested jobs
    for job in $(echo $RUN_JOBS | tr , ' '); do
      # If it's a valid job add it to the final jobs list
      if [[ $(is_valid_job $job) -eq 0 ]]; then
        jobs=$(append_jobs $jobs $job)
      # If it's a range of jobs resolve the range and add it to the final jobs list
      elif [[ $job == *"-"* ]]; then
        range=$(resolve_job_range $job)
        jobs=$(append_jobs $jobs $range)
      else
        die "Invalid job $job"
      fi
    done
    RUN_JOBS=$jobs
  fi
  echo "Resolved jobs to $RUN_JOBS"
}

get_major_version() {
  version=$(echo $1 | sed 's/ICGC//')
  echo $version | cut -d '-' -f 1
}

get_minor_version() {
  version=$(echo $1 | sed 's/ICGC//')
  echo $version | cut -d '-' -f 2
}

resolve_release_name() {
  last_release=
  [[ -e $LASTRUN ]] && last_release=$(head $LASTRUN)
  if [[ -z $last_release ]]; then
    RELEASE_NAME=${RELEASE_NAME}-0
  else 
    ensure_regex $last_release '^ICGC\d+-\d+$' "Failed to resolve next release name from last run release $last_release"
    last_major_version=$(get_major_version $last_release)
    major_version=$(get_major_version $RELEASE_NAME)
    if [[ $major_version -eq $last_major_version ]]; then
      last_minor_version=$(get_minor_version $last_release)
      next_minor_version=$(expr $last_minor_version + 1)
      RELEASE_NAME=${RELEASE_NAME}-$next_minor_version
    else
      RELEASE_NAME=${RELEASE_NAME}-0
    fi
  fi
  echo $RELEASE_NAME > $LASTRUN
}

run() {
  args=""
  if [[ -n $CORES ]]; then
    args="$args --spark.properties.spark.cores.max=${CORES}"
  fi

  args="$args --release-dir ${DATA_DIR}"
  args="$args --staging-dir ${WORK_DIR}"
  args="$args --release ${RELEASE_NAME}"

  if [[ -n $PROJECTS ]]; then
    args="$args --project-names ${PROJECTS}"
  else
    args="$args --project-names ${PROJ}"
  fi

  args="$args --jobs ${RUN_JOBS}"

  java \
    -Djava.library.path=${DRIVER_LIB} \
    -Dlogging.config=$CONF_DIR/logback.xml \
    -Xmx${DRIVER_MEM} -Xms${DRIVER_MEM} \
    -jar $JAR \
    --spring.config.location=${CONF_FILE} \
    $args
}

run_jobs() {
  if [[ ! -f $JOBS_CONF ]] || [[ -n $CORES ]]; then
    echo Running jobs...
  else
    echo "Running jobs according to the jobs configuration..."
    # Associative arrays work with Bash version > 3
    declare -A jobs_conf
    # Read jobs configuration in map
    while IFS='=' read -r key value; do
      jobs_conf[$key]=$value
    done < $JOBS_CONF

    all_jobs=$(echo $RUN_JOBS | tr , ' ')
    current_cores=
    prev_cores=-1
    jobs_to_run=""
    for j in $all_jobs; do
      current_cores=${jobs_conf[$j]}

      # Very first job
      if [[ $prev_cores -eq -1 ]]; then
        prev_cores=$current_cores
        jobs_to_run=$(append_jobs $jobs_to_run $j)
      elif [[ $current_cores -eq $prev_cores ]]; then
        jobs_to_run=$(append_jobs $jobs_to_run $j)
      else
        # Need to run the jobs as jobs configuration changed
        if [[ -z $prev_cores ]]; then
          unset CORES
        else
          CORES=$prev_cores
        fi
        echo "Running jobs $jobs_to_run on '$CORES' cores"
        RUN_JOBS=$jobs_to_run
        run
        jobs_to_run=$j
        prev_cores=$current_cores
      fi
    done

    # Run the jobs left after iterating over the configuration file
    if [[ -z $prev_cores ]]; then
      unset CORES
    else
      CORES=$prev_cores
    fi
    echo "Running jobs $jobs_to_run on '$CORES' cores"
    RUN_JOBS=$jobs_to_run
    run
  fi
}

#
# Main
#

# This seems is not required as the release client creates directory itself
#create_workdir

# Parse arguments and set configuration variables
parse_arguments $@
resolve_data_dir
resolve_work_dir
resolve_jobs
resolve_release_name
run_jobs

# Useful settings
#    -Djoinjob.tasks=sgv \
#    -Djoinjob.clean=false \
#    -Djoinjob.sequential=true \
