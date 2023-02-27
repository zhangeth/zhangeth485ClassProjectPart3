package CSCI485ClassProject.dataRecordManagement;


import CSCI485ClassProject.StatusCode;
import CSCI485ClassProject.models.DataRecord;

import java.util.List;

/**
 * DataRecordManager manages the data stored by the certain table. It provides interfaces to
 * get/create/update/delete certain data record(s) in a table
 */
public interface DataRecordManager {

  /**
   * Create a new DataRecord in a table.
   *
   * For the attributeNames that are specified in the TableMetadata
   * but not in here, set their corresponding value to be NULL.
   *
   * ERROR Checking and the StatusCode that should return:
   * - table does not exist --> TABLE_NOT_FOUND
   * - no attribute is provided --> DATA_RECORD_CREATION_ATTRIBUTES_VALUES_INVALID
   * - attributeNames/values is null or does not have equal length --> DATA_RECORD_CREATION_ATTRIBUTES_VALUES_INVALID
   * - attributeNames are not the subset of attributes specified in TableMetadata --> DATA_RECORD_CREATION_ATTRIBUTES_INVALID
   * - one or more values' type are not supported --> ATTRIBUTE_TYPE_NOT_SUPPORTED
   * - values' type do not match the attributeType specified in TableMetadata --> DATA_RECORD_CREATION_VALUES_INVALID
   * - one or more primary key attribute(s) do not have corresponding non-null values --> DATA_RECORD_CREATION_PRIMARY_KEYS_NOT_SET
   * - Internal FDB Failure --> INTERNAL_STORAGE_FAILURE
   *
   * @param tableName the target table name
   * @param attributeNames the attribute names.
   * @param values the attribute values corresponding to attribute names
   * @return StatusCode
   */
  StatusCode createDataRecord(String tableName, String[] attributeNames, Object[] values);

  /**
   * Get one DataRecord in a Table. The data record is identified by primary key values.
   *
   * ERROR Checking(return null if falls into these cases):
   * - table does not exist
   * - pkAttributes or pkValues is null/empty or does not have equal size
   * - pkAttributes does not equal to the primary key specification in the TableMetadata
   * - pkValues does not match the types specified in the TableMetadata
   * - Internal FDB Failure --> INTERNAL_STORAGE_FAILURE
   *
   * @param tableName the target table name
   * @param pkAttributes the primary key attributes
   * @param pkValues the values corresponding to the primary key attributes
   * @return the DataRecord if found, otherwise return null
   */
  DataRecord getDataRecord(String tableName, String[] pkAttributes, Object[] pkValues);

  /**
   * Get all data records in a Table
   *
   * ERROR Checking(return emptyList if falls into these cases):
   * - table does not exist
   * - Internal FDB Failure
   *
   * @param tableName the target tableName
   * @return the list of DataRecord
   */
  List<DataRecord> getAllDataRecord(String tableName);

  /**
   * Update an existing data record in a table. The data record is identified by primary key values.
   *
   * ERROR Checking and the StatusCode that should return:
   * - table does not exist --> TABLE_NOT_FOUND
   * - pkAttributes or pkValues is null/empty/not equal in length --> DATA_RECORD_UPDATE_PK_ATTRIBUTES_VALUES_INVALID
   * - attributesToUpdate or newValues is null/empty/not equal in length --> DATA_RECORD_UPDATE_ATTRIBUTES_VALUES_INVALID
   * - pkAttributes does not equal to the primary keys specified in the TableMetadata --> DATA_RECORD_UPDATE_PK_ATTRIBUTES_INVALID
   * - pkValues does not match the types specified in the TableMetadata --> DATA_RECORD_UPDATE_PK_VALUES_INVALID
   * - attributesToUpdate are not the subset of attributes specified in the TableMetadata --> DATA_RECORD_UPDATE_ATTRIBUTES_INVALID
   * - newValues does not match the types specified in the TableMetadata --> DATA_RECORD_UPDATE_VALUES_INVALID
   * - one or more values' type are not supported --> ATTRIBUTE_TYPE_NOT_SUPPORTED
   * - Internal FDB Failure --> INTERNAL_STORAGE_FAILURE
   *
   * @param tableName the target table name
   * @param pkAttributes the primary key attributes
   * @param pkValues the values corresponding to the primary key attributes
   * @param attributesToUpdate the attribute names to be updated
   * @param newValues the new values corresponding to the attributes
   * @return StatusCode
   */
  StatusCode updateDataRecord(String tableName, String[] pkAttributes, Object[] pkValues, String[] attributesToUpdate, Object[] newValues);

  /**
   * Delete an existing data record in a table. The data record is identified by primary key values.
   *
   * ERROR Checking and the StatusCode that should return:
   * - table does not exist --> TABLE_NOT_FOUND
   * - pkAttributes or pkValues is null/empty/not equal in length --> DATA_RECORD_DELETION_PK_ATTRIBUTES_VALUES_INVALID
   * - pkAttributes does not equal to the primary keys specified in the TableMetadata --> DATA_RECORD_DELETION_PK_ATTRIBUTES_INVALID
   * - pkValues does not match the types specified in the TableMetadata --> DATA_RECORD_DELETION_PK_VALUES_INVALID
   * - Internal FDB Failure --> INTERNAL_STORAGE_FAILURE
   *
   * @param tableName the target table name
   * @param pkAttributes the primary key attributes
   * @param pkValues the values corresponding to the primary key attributes
   * @return StatusCode
   */
  StatusCode deleteDataRecord(String tableName, String[] pkAttributes, Object[] pkValues);
}
