package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.*;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.*;

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

  public static void insertIndex(Database db, Transaction tx, String tableName, String targetAttrName, Object targetAttrVal, Tuple pkVal)
  {
    // read indexType
    DirectorySubspace indexSubspace = FDBHelper.getIndexSubspace(tx, tableName, targetAttrName);
    AsyncIterable<KeyValue> indexIterable  = FDBHelper.getKVPairIterableOfDirectory(indexSubspace, tx, false);
    AsyncIterator<KeyValue> indexIterator = indexIterable.iterator();
    FDBKVPair kvPair = FDBHelper.convertKeyValueToFDBKVPair(tx, FDBHelper.getIndexPath(tx, tableName, targetAttrName), indexIterator.next());
    Tuple keyTuple = kvPair.getKey();

    IndexType idxType;
    // read typing, index type stored in second element in keyTuple
    int typeCode = (int)keyTuple.getLong(1);
    if (typeCode == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX.ordinal())
    {
      idxType = IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX;
    }
    else {
      idxType = IndexType.NON_CLUSTERED_HASH_INDEX;
    }
    // otherwise, don't change because set to hash by default

    NonClusteredIndexRecord rec = new NonClusteredIndexRecord(tableName, targetAttrName, targetAttrVal, pkVal, idxType);
    rec.setRecord(tx);
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

    // update index if there are any
    // loop through attributes, and check if any Indices exist, update them if they do
    for (Map.Entry e :  tblMetadata.getAttributes().entrySet())
    {
      List<String> potentialIndexPath = new ArrayList<>();
      String attrName = e.getKey().toString();
      potentialIndexPath.add(tableName); potentialIndexPath.add(attrName + "Index");
      if (FDBHelper.doesSubdirectoryExists(tx, potentialIndexPath))
      {
       // System.out.println("entered insertion for: " + attrName);
        Tuple pkTuple = new Tuple();
        for (String s : tblMetadata.getPrimaryKeys())
        {
          pkTuple = pkTuple.addObject(record.getValueForGivenAttrName(s));
        }
        // update index with appropriate record
        insertIndex(db, tx, tableName, attrName,record.getValueForGivenAttrName(attrName), pkTuple);

      }

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

    Cursor cursor;
    //System.out.println("yee");
    // use index structure if exists
    if (isUsingIndex)
    {
      if (FDBHelper.doesIndexExist(tx, tableName, attrName))
      {
        /*System.out.println("entered");
        RecordsTransformer recordsTransformer = new RecordsTransformer(tableName, getTableMetadataByTableName(tx, tableName));
        List<String> recordStorePath = recordsTransformer.getTableRecordPath();*/

    /*  for (FDBKVPair kv : FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, recordStorePath))
        {
          System.out.println("kv key: " + kv.getKey().toString());
        }*/
/*        List<String> p = new ArrayList<>(); p.add(tableName); p.add("SalaryIndex");
        for (FDBKVPair pa : FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, p))
        {
          System.out.println("pa key: " + pa.getKey());
        }*/
        cursor = new Cursor(mode, tableName, tblMetadata, tx, true);

      }
      else {
        // index structure doesn't exist
        return null;
      }
    }
    else {
      cursor = new Cursor(mode, tableName, tblMetadata, tx);
    }

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
