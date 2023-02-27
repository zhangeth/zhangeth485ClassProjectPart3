# Class Project

This repo is the base codes of the ClassProject of 2023Spring CSCI485.

## Getting Started
You need to migrate the code of Stage1 to this project. 
Your codes need to be put under `src/CSCI485ClassProject/tableSchemaManagement`. 
You may need to also adjust the `import` path inside the source codes. Once the migration
is finished, you can verify it by running the unit test of Part1.

## Project Structure Overview

- `lib`: the dependencies of the project
- `src`: the source codes of the project
  - `CSCI485ClassProject`: root package of the project
    - `tableSchemaManagement`: package for Part1
    - `dataRecordManagement`: package for Part2
    - `test`: package for unit tests



## Unit Tests Specification
There are 5 parts in this project. Each part has the corresponding unit tests 
for the purpose of correctness check and grading. 

All unit tests are put in directory `src/CSCI485ClassProject/test`. 
Unit tests for Part `X` will be named as `PartXTest.java` 
## How to run unit tests

For `macOS/Linux`, you can use `make` to run the corresponding test. 

For example, to run the Part2's test, you can execute
```shell
make part2Test
```

For `Windows`, you may need to have additional configurations in order to make the corresponding unit tests run. 

The grading environment is macOS/Linux.

