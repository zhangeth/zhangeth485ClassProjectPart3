package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RecordsImpl implements Records{

  private final Database db;

  public RecordsImpl() {
    db = FDBHelper.initialization();
  }

  private TableMetadata getTableMetadataByTableName(Transaction tx, String tableName) {
    TableMetadataTransformer tblMetadataTransformer = new TableMetadataTransformer(tableName);
    List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx,
        tblMetadataTransformer.getTableAttributeStorePath());
    TableMetadata tblMetadata = tblMetadataTransformer.convertBackToTableMetadata(kvPairs);
    return tblMetadata;
  }
  @Override
  public StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues) {
    // open the transaction
    Transaction tx = FDBHelper.openTransaction(db);
    // check if the table exists
    if (!FDBHelper.doesSubdirectoryExists(tx, Collections.singletonList(tableName))) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.TABLE_NOT_FOUND;
    }
    // check the validity of the input parameters
    if (primaryKeys == null || primaryKeysValues == null || attrNames == null || attrValues == null) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;
    }
    if (primaryKeys.length != primaryKeysValues.length || attrValues.length != attrNames.length) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;
    }

    // get the tableMetadata
    TableMetadata tblMetadata = getTableMetadataByTableName(tx, tableName);

    List<String> pks = Arrays.asList(primaryKeys);
    List<String> schemaPks = tblMetadata.getPrimaryKeys();

    // check if the given primary keys are identical to primary keys stated in the table schema
    if (!pks.containsAll(schemaPks) || !schemaPks.containsAll(pks)) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
    }

    Record record = new Record();
    // do the input values' type checking
    for (int i = 0; i<primaryKeys.length; i++) {
      StatusCode status = record.setAttrNameAndValue(primaryKeys[i], primaryKeysValues[i]);
      if (status != StatusCode.SUCCESS) {
        FDBHelper.abortTransaction(tx);
        return status;
      }
    }

    // update the table schema if there are new columns
    HashMap<String, AttributeType> existingTblAttributes = tblMetadata.getAttributes();
    Set<String> existingTblAttributeNames = existingTblAttributes.keySet();

    List<FDBKVPair> tblSchemaUpdatePairs = new ArrayList<>();
    TableMetadataTransformer tblMetadataTransformer = new TableMetadataTransformer(tableName);

    for (int i = 0; i<attrNames.length; i++) {
      String attrName = attrNames[i];
      StatusCode status = record.setAttrNameAndValue(attrName, attrValues[i]);
      if (status != StatusCode.SUCCESS) {
        FDBHelper.abortTransaction(tx);
        return status;
      }

      AttributeType attrType = record.getTypeForGivenAttrName(attrName);
      if (!existingTblAttributeNames.contains(attrName)) {
        tblSchemaUpdatePairs.add(tblMetadataTransformer.getAttributeKVPair(attrName, attrType));
      } else if (!attrType.equals(existingTblAttributes.get(attrName))) {
        FDBHelper.abortTransaction(tx);
        return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
      }
    }

    // serialize the Record and persist to FDB
    // persist the data pairs
    RecordsTransformer recordsTransformer = new RecordsTransformer(tableName, tblMetadata);

    // check if records already exists
    Tuple primKeyTuple = new Tuple();
    for (int i = 0; i<primaryKeysValues.length; i++) {
      primKeyTuple = primKeyTuple.addObject(primaryKeysValues[i]);
    }

    if (recordsTransformer.doesPrimaryKeyExist(tx, primKeyTuple)) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.DATA_RECORD_CREATION_RECORD_ALREADY_EXISTS;
    }

    List<FDBKVPair> fdbkvPairs = recordsTransformer.convertToFDBKVPairs(record);

    DirectorySubspace dataRecordsSubspace = FDBHelper.createOrOpenSubspace(tx, recordsTransformer.getTableRecordPath());
    for (FDBKVPair kv : fdbkvPairs) {
      FDBHelper.setFDBKVPair(dataRecordsSubspace, tx, kv);
    }

    // persist the schema changing pairs
    DirectorySubspace tableSchemaDirectory = FDBHelper.openSubspace(tx, tblMetadataTransformer.getTableAttributeStorePath());
    for (FDBKVPair kv : tblSchemaUpdatePairs) {
      FDBHelper.setFDBKVPair(tableSchemaDirectory, tx, kv);
    }

    FDBHelper.commitTransaction(tx);
    return StatusCode.SUCCESS;
  }

  @Override
  public Cursor openCursor(String tableName, Cursor.Mode mode) {
    Transaction tx = FDBHelper.openTransaction(db);

    if (!FDBHelper.doesSubdirectoryExists(tx, Collections.singletonList(tableName))) {
      // check if the table exists
      FDBHelper.abortTransaction(tx);
      return null;
    }

    TableMetadata tblMetadata = getTableMetadataByTableName(tx, tableName);
    return new Cursor(mode, tableName, tblMetadata, tx);
  }

  @Override
  public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
    Transaction tx = FDBHelper.openTransaction(db);

    if (!FDBHelper.doesSubdirectoryExists(tx, Collections.singletonList(tableName))) {
      // check if the table exists
      FDBHelper.abortTransaction(tx);
      return null;
    }

    TableMetadata tblMetadata = getTableMetadataByTableName(tx, tableName);

    // check if the given attribute exists
    if (!tblMetadata.doesAttributeExist(attrName)) {
      FDBHelper.abortTransaction(tx);
      return null;
    }

    Cursor cursor = new Cursor(mode, tableName, tblMetadata, tx);
    Record.Value attrVal = new Record.Value();
    StatusCode initVal = attrVal.setValue(attrValue);
    if (initVal != StatusCode.SUCCESS) {
      // check if the new value's type matches the table schema
      FDBHelper.abortTransaction(tx);
      return null;
    }
    cursor.enablePredicate(attrName, attrVal, operator);
    return cursor;
  }

  @Override
  public Record getFirst(Cursor cursor) {
    return cursor.getFirst();
  }

  @Override
  public Record getLast(Cursor cursor) {
    return cursor.getLast();
  }

  @Override
  public Record getNext(Cursor cursor) {
    return cursor.next(false);
  }

  @Override
  public Record getPrevious(Cursor cursor) {
    return cursor.next(true);
  }

  @Override
  public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
    return cursor.updateCurrentRecord(attrNames, attrValues);
  }

  @Override
  public StatusCode deleteRecord(Cursor cursor) {
    if (cursor == null || cursor.getTx() == null) {
      return StatusCode.CURSOR_INVALID;
    }
    if (!cursor.isInitialized()) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }
    if (cursor.getCurrentRecord() == null) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }
    Record recordToDelete = cursor.getCurrentRecord();
    Set<String> attrDiffSet = new HashSet<>();
    Transaction tx = cursor.getTx();

    // Open another cursor and scan the table, see if the table schema needs to change because of the deletion
    Cursor scanCursor =
        new Cursor(Cursor.Mode.READ, cursor.getTableName(), cursor.getTableMetadata(), tx);
    boolean isScanCursorInit = false;
    while (true) {
      Record record;
      if (!isScanCursorInit) {
        isScanCursorInit = true;
        record = getFirst(scanCursor);
      } else {
        record = getNext(scanCursor);
      }
      if (record == null) {
        break;
      }
      Set<String> attrSet = record.getMapAttrNameToValue().keySet();
      Set<String> attrSetToDelete = new HashSet<>(recordToDelete.getMapAttrNameToValue().keySet());
      attrSetToDelete.removeAll(attrSet);
      attrDiffSet.addAll(attrSetToDelete);
    }
    if (!attrDiffSet.isEmpty()) {
      // drop the attributes of the table
      TableMetadataTransformer transformer = new TableMetadataTransformer(scanCursor.getTableName());
      List<String> tblAttributeDirPath = transformer.getTableAttributeStorePath();
      DirectorySubspace tableAttrDir = FDBHelper.openSubspace(tx, tblAttributeDirPath);

      for (String attrNameToDrop : attrDiffSet) {
        Tuple attrKeyTuple = TableMetadataTransformer.getTableAttributeKeyTuple(attrNameToDrop);
        FDBHelper.removeKeyValuePair(tableAttrDir, tx, attrKeyTuple);
      }
    }
    return cursor.deleteCurrentRecord();
  }

  @Override
  public StatusCode commitCursor(Cursor cursor) {
    if (cursor == null) {
      return StatusCode.CURSOR_INVALID;
    }
    cursor.commit();
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode abortCursor(Cursor cursor) {
    if (cursor == null) {
      return StatusCode.CURSOR_INVALID;
    }
    cursor.abort();
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues) {
    return null;
  }
}
