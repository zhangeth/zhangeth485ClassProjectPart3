package CSCI485ClassProject;

import CSCI485ClassProject.models.IndexType;

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
   * Remove the index structure from an attribute in a table.
   *
   * The table, attribute and index structure should exist.
   * @param tableName the target table name
   * @param attrName the target attribute name
   * @param indexType the type of index
   * @return StatusCode
   */
  StatusCode removeIndex(String tableName, String attrName, IndexType indexType);
}
