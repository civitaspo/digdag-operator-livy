# digdag-operator-livy
[![Jitpack](https://jitpack.io/v/pro.civitaspo/digdag-operator-livy.svg)](https://jitpack.io/#pro.civitaspo/digdag-operator-livy) [![CircleCI](https://circleci.com/gh/civitaspo/digdag-operator-livy.svg?style=shield)](https://circleci.com/gh/civitaspo/digdag-operator-livy) [![Digdag](https://img.shields.io/badge/digdag-v0.9.27-brightgreen.svg)](https://github.com/treasure-data/digdag/releases/tag/v0.9.27)

This operator is for operating a job by Livy REST API.

# Overview

- Plugin type: operator

# Usage

```yaml
_export:
  plugin:
    repositories:
      - https://jitpack.io
    dependencies:
      - pro.civitaspo:digdag-operator-livy:0.0.5
  livy:
    host: mylivy.internal
    port: 8998
    scheme: http

        
+step1:
  livy.submit_job>:
  job:
    name: livy-test
    class_name: 'pro.civitaspo.livy_test.Launcher'
    file: s3://mybucket/path/to/livy-test.jar
    args: ['run', '-e', 'development']
    driver_memory: 3G
    driver_cores: 1
    executor_memory: 30G
    executor_cores: 4
    num_executors: 25
  wait_until_finished: true
  wait_timeout_duration: 40m

+step2:
  echo>: ${livy.last_job.id}

```

# Configuration

## Remarks

- type `DurationParam` is strings matched `\s*(?:(?<days>\d+)\s*d)?\s*(?:(?<hours>\d+)\s*h)?\s*(?:(?<minutes>\d+)\s*m)?\s*(?:(?<seconds>\d+)\s*s)?\s*`.
  - The strings is used as `java.time.Duration`.

## Common Configuration

### Options

- **host**: Livy API host name. (string, required)
- **port**: Livy API port. (integer, default: `8998`)
- **scheme**: `"https"` or `"http"` (default: `"http"`)
- **header**: Header for HTTP Requests. (string to string map, optional)
  - Note: Force to include `Content-type: application/json` as header.
- **connection_timeout_duration**: Connection timeout duration for HTTP Requests. (`DurationParam`, default: `"5m"`)
- **read_timeout_duration**: Read timeout duration for HTTP Requests. (`DurationParam`, default: `"5m"`)

## Configuration for `livy.submit_job>` operator

### Options

- **job**: Specify a job settings. See [doc (POST /batches)](http://livy.incubator.apache.org./docs/latest/rest-api.html) (map, required)
  - **file**: File containing the application to execute. (string, required)
  - **proxy_user**: User to impersonate when running the job. (string, optional)
  - **class_name**: Application Java/Spark main class. (string, optional)
  - **args**: Command line arguments for the application. (array of string, optional)
  - **jars**: jars to be used in this session. (array of string, optional)
  - **py_files**: Python files to be used in this session. (array of string, optional)
  - **files**: files to be used in this session. (array of string, optional)
  - **driver_memory**: Amount of memory to use for the driver process. (string, optional)
  - **driver_cores**: Number of cores to use for the driver process. (integer, optional)
  - **executor_memory**: Amount of memory to use per executor process. (string, optional)
  - **executor_cores**: Number of cores to use for each executor. (integer, optional)
  - **num_executors**: Number of executors to launch for this session. (integer, optional)
  - **archives**: Archives to be used in this session. (array of string, optional)
  - **queue**: The name of the YARN queue to which submitted. (string, optional)
  - **name**: The name of this session. (string, default: `"digdag-${session_uuid}"`)
  - **conf**: Spark configuration properties. (string to string map, optional)
- **wait_until_finished**: Specify whether to wait until the job is finished or not. (boolean, default: `true`)
- **wait_timeout_duration**: Specify timeout period. (`DurationParam`, default: `"45m"`)
  
### Output Parameters

- **livy.last_job.session_id**: The session id. (integer)
- **livy.last_job.application_id**: The application id of this session. (string)
- **livy.last_job.application_info**: The detailed application info. (string to string map)
- **livy.last_job.state**: The batch state. (string)

## Configuration for `livy.wait_job>` operator

### Options

- **livy.wait_job>**: The session id. (integer, required)
- **success_states**: The session states breaks polling the session. Valid values are `"not_started"`, `"starting"`, `"recovering"`, `"idle"`, `"running"`, `"busy"`, `"shutting_down"`, `"error"`, `"dead"`, `"killed"` and `"success"`. (array of string, required)
- **error_states**: The session states breaks polling the session with errors. Valid values are `"not_started"`, `"starting"`, `"recovering"`, `"idle"`, `"running"`, `"busy"`, `"shutting_down"`, `"error"`, `"dead"`, `"killed"` and `"success"`. (array of string, optional)
- **polling_interval**: Specify polling interval. (`DurationParam`, default: `"5s"`)
- **timeout_duration**: Specify timeout period. (`DurationParam`, default: `"45m"`)

### Output Parameters

- **livy.last_job.session_id**: The session id. (integer)
- **livy.last_job.application_id**: The application id of this session. (string)
- **livy.last_job.application_info**: The detailed application info. (string to string map)
- **livy.last_job.state**: The batch state. (string)

# Development

## Run an Example

### 1) build

```sh
./gradlew publish
```

Artifacts are build on local repos: `./build/repo`.

### 2) get your aws profile

```sh
aws configure
```

### 3) run an example

```sh
./example/run.sh
```

## (TODO) Run Tests

```sh
./gradlew test
```

# ChangeLog

[CHANGELOG.md](./CHANGELOG.md)

# License

[Apache License 2.0](./LICENSE.txt)

# Author

@civitaspo
