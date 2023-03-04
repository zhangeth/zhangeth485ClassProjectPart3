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
   * Open a READ_ONLY cursor that hooks to the certain attribute with predicate.
   *
   * @param tableName the target table name
   * @param attrName the attribute name
   * @param attrValue the attribute value
   * @param operator the operator imposed on the target attribute and value. See {ComparisonOperator} for supported operator types.
   * @param indexType NO_INDEX if not using any index, otherwise use the given index type when doing operations
   * @return Cursor
   */
  Cursor openReadOnlyCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, IndexType indexType);

  /**
   * Open a READ_ONLY cursor without any predicate or index.
   *
   * @param tableName the target table name
   * @return Cursor
   */
  Cursor openReadOnlyCursor(String tableName);

  /**
   * Open a cursor in READ_WRITE/WRITE_ONLY mode that hooks to the certain attribute in a table.
   *
   * @param tableName the target tableName
   * @param attrName the target attribute Name
   * @param attrValue the attribute value for the predicate
   * @param operator the operator used by the predicate
   * @param mode the mode of the cursor
   * @return Cursor
   */
  Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode);

  /**
   * Open a cursor in READ_WRITE/WRITE_ONLY mode without any predicate
   *
   * @param tableName
   * @return
   */
  Cursor openCursor(String tableName);

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
   * If index structures are built on some attributes, they should also be updated
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
