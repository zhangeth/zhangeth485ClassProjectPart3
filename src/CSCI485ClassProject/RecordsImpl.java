package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;

public class RecordsImpl implements Records{
  @Override
  public StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues) {
    return StatusCode.SUCCESS;
  }

  @Override
  public Cursor openReadOnlyCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, IndexType indexType) {
    return null;
  }

  @Override
  public Cursor openReadOnlyCursor(String tableName) {
    return null;
  }

  @Override
  public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode) {
    return null;
  }

  @Override
  public Cursor openCursor(String tableName) {
    return null;
  }

  @Override
  public Record getFirst(Cursor cursor) {
    return null;
  }

  @Override
  public Record getLast(Cursor cursor) {
    return null;
  }

  @Override
  public Record getNext(Cursor cursor) {
    return null;
  }

  @Override
  public Record getPrevious(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteRecord(Cursor cursor) {
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode closeCursor(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues) {
    return null;
  }
}
