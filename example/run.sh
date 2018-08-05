#!/usr/bin/env bash

ROOT=$(cd $(dirname $0)/..; pwd)
EXAMPLE_ROOT=$ROOT/example
LOCAL_MAVEN_REPO=$ROOT/build/repo

(
  cd $EXAMPLE_ROOT

  ## to remove cache
  rm -rf .digdag

  ## run
  digdag run example.dig -p repos=${LOCAL_MAVEN_REPO} --no-save
)
