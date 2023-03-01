# Class Project: Stage2

This repo is the base codes of the ClassProject of 2023Spring CSCI485.

## Getting Started
You need to migrate the code of Stage1 to this project. 
Your codes need to be put under `src/CSCI485ClassProject/` and its subdirectories. 
You may need to also adjust the `import` path inside the source codes. Once the migration
is finished, you can verify it by running the unit test of Part1.

## Project Structure Overview

- `lib`: the dependencies of the project
- `src`: the source codes of the project
  - `CSCI485ClassProject`: root package of the project
    - `models`: package storing some defined representations of models in the database.
    - `test`: package for unit tests
    
## Codes to implement
Under `src/CSCI485ClassProject`, there are three classes to finish:
- `CursorImpl`: implementation of an abstract class `Cursor`
- `IndexesImpl`: implementation of an interface class `Indexes`
- `RecordsImpl`: implementation of an interface class `Records`
