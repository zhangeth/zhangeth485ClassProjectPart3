package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;

/**
 * DataRecordManager manages the data stored by the certain table. It provides interfaces to
 * get/create/update/delete certain data record(s) in a table
 */
public interface Records {

  /**
   * Insert a new row into the table.
   *
   * Given primary keys and values must match the specification in TableMetadata.
   *
   * If given attributes does not exist in the schema, attributes will be added to the schema.
   *
   * @param tableName the target tableName
   * @param primaryKeys primary keys
   * @param primaryKeysValues corresponding values of primary keys
   * @param attrNames attribute names(doesn't contain primary keys)
   * @param attrValues attribute values(doesn't contain primary key values)
   * @return StatusCode
   */
  StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues);

  /**
   * Open a cursor that hooks to certain attributes in a table.
   *
   * Cursor can be used to read/update/delete records of a table
   *
   * @param tableName the target table name
   * @param attrName the attribute name
   * @param attrValue the attribute value
   * @param operator the operator imposed on the target attribute and value. See {ComparisonOperator} for supported operator types.
   * @return Cursor
   */
  Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator);

  /**
   * Open a cursor that hooks to the certain attribute with certain index in a table.
   *
   * Given type of the index structure should exist on the given attribute.
   *
   * After open a cursor, either {getFirst} or {getLast} should be called in order to perform certain operations using cursor,
   * @param tableName the target table name
   * @param attrName the attribute name
   * @param attrValue the attribute value
   * @param operator the operator imposed on the target attribute and value. See {ComparisonOperator} for supported operator types.
   * @param indexType the target type of index
   * @return Cursor
   */
  Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, IndexType indexType);

  /**
   * Seek the cursor to the first qualified record.
   * @param cursor the target cursor
   * @return the first qualified record
   */
  Record getFirst(Cursor cursor);

  /**
   * Seek the cursor to the last qualified record.
   * @param cursor the target cursor
   * @return the last qualified record
   */
  Record getLast(Cursor cursor);

  // if last record -> EOF
  /**
   * Move the cursor to the next valid record and return. If it is already at the last record, return the EOF
   * @param cursor the cursor to move next
   * @return Record if the next record exists, otherwise return null
   */
  Record getNext(Cursor cursor);

  /**
   * Move the cursor to the previous valid record and return. If it is already at the first record, return the EOF
   * @param cursor the cursor to move previous
   * @return Record if the previous record exists, otherwise return null
   */
  Record getPrevious(Cursor cursor);

  /**
   * Update the record that the cursor is pointing at, with new attribute values.
   *
   * If the given attribute(s) do not exist, the attribute should be added to the table schema.
   * If index type is specified when opening the cursor, the corresponding index record should also be updated
   * TODO: what if there are multiple indexes on this record?
   * @param cursor the target cursor
   * @param attrNames the attribute names
   * @param attrValues the corresponding attribute values
   * @return StatusCode
   */
  StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues);

  /**
   * Delete the record that the cursor is pointing at.
   *
   * If index type is specified when opening the cursor, the corresponding index record should also be deleted
   * TODO: what if there are multiple indexes on this record?
   * @param cursor
   * @return
   */
  StatusCode deleteRecord(Cursor cursor);

  /**
   * Close the cursor
   * @param cursor the target cursor
   * @return StatusCode
   */
  StatusCode closeCursor(Cursor cursor);

  /**
   * Delete the record with given attribute names and values in a table.
   *
   * If indexes are built on the table, index records should also be modified accordingly.
   *
   * @param tableName the target table name
   * @param attrNames the attribute names
   * @param attrValues the corresponding attribute values
   * @return StatusCode
   */
  StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues);
}
