package CSCI485ClassProject;

import CSCI485ClassProject.models.Record;

public class CursorImpl extends Cursor {
  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public Record next() {
    return null;
  }

  @Override
  StatusCode close() {
    return StatusCode.SUCCESS;
  }
}
