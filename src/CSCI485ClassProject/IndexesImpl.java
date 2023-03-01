package CSCI485ClassProject;

import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;

public class IndexesImpl implements Indexes{
  @Override
  public StatusCode createIndex(String tableName, IndexType indexType, String attrName) {
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode insertIndexRecord(String tableName, String attrName, Object attrValue, IndexType indexType) {
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode removeIndex(String tableName, String attrName, IndexType indexType) {
    return StatusCode.SUCCESS;
  }

  @Override
  public Record getIndexRecord(String tableName, String attrName, Object attrValue, IndexType indexType) {
    return null;
  }

  @Override
  public StatusCode deleteIndexRecord(String tableName, String attrName, Object attrValue, IndexType indexType) {
    return StatusCode.SUCCESS;
  }
}
