package CSCI485ClassProject;

import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;

public interface Indexes {

  /**
   * Create index structures for an existing attribute in a table.
   *
   * The table and the target attribute should exist.
   * @param tableName the target table name
   * @param indexType the index type
   * @param attrName the target attribute name
   * @return StatusCode
   */
  StatusCode createIndex(String tableName, IndexType indexType, String attrName);

  /**
   * Insert a new index record to an existing index structure of an attribute in a table.
   *
   * The index structure should exist.
   * @param tableName the target table name
   * @param attrName the attribute name
   * @param attrValue the corresponding attribute value
   * @param indexType the type of index
   * @return StatusCode
   */
  StatusCode insertIndexRecord(String tableName, String attrName, Object attrValue, IndexType indexType);

  /**
   * Remove the index structure from an attribute in a table.
   *
   * The table, attribute and index structure should exist.
   * @param tableName the target table name
   * @param attrName the target attribute name
   * @param indexType the type of index
   * @return StatusCode
   */
  StatusCode removeIndex(String tableName, String attrName, IndexType indexType);


  /**
   * Get an index record of a given attribute and its value in a table
   *
   * The attribute value, table and the index structure should exist.
   * @param tableName the target table name
   * @param attrName the target attribute name
   * @param attrValue the target attribute value
   * @param indexType the type of index
   * @return Index Record found on this attribute value; return null if there is no such index record.
   */
  Record getIndexRecord(String tableName, String attrName, Object attrValue, IndexType indexType);

  /**
   * Remove a certain type of index record of a given attribute and its value in a table.
   *
   * The table, attribute and the type of index should exist
   * @param tableName
   * @param attrName
   * @param attrValue
   * @return
   */
  StatusCode deleteIndexRecord(String tableName, String attrName, Object attrValue, IndexType indexType);

}
