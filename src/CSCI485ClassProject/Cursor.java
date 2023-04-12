package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.*;
import CSCI485ClassProject.utils.ComparisonUtils;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.*;

import static CSCI485ClassProject.RecordsTransformer.getPrimaryKeyValTuple;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE,
  }

  // used by predicate
  private boolean isPredicateEnabled = false;
  private String predicateAttributeName;
  private Record.Value predicateAttributeValue;
  private ComparisonOperator predicateOperator;

  // Table Schema Info
  private String tableName;
  private TableMetadata tableMetadata;

  private RecordsTransformer recordsTransformer;

  private boolean isInitialized = false;

  private boolean isInitializedToLast = false;

  private boolean isIndexTypeInitialized = false;

  private final Mode mode;

  private AsyncIterator<KeyValue> iterator = null;

  private AsyncIterator<KeyValue> indexIterator = null;

  // by default is hash
  private IndexType indexType = IndexType.NON_CLUSTERED_HASH_INDEX;

  private Record currentRecord = null;

  private Transaction tx;

  private DirectorySubspace directorySubspace;

  private DirectorySubspace indexSubspace;

  // initialized at start of moveToNext
  private List<String> recordStorePath;

  private boolean isMoved = false;
  private FDBKVPair currentKVPair = null;

  private boolean isUsingIndex;

  public Cursor(Mode mode, String tableName, TableMetadata tableMetadata, Transaction tx) {
    this.mode = mode;
    this.tableName = tableName;
    this.tableMetadata = tableMetadata;
    this.tx = tx;
  }

  // assumes index structure exists, checks exists in recordsImpl
  public Cursor(Mode mode, String tableName, TableMetadata tableMetadata, Transaction tx, boolean isUsingIndex)
  {
    this(mode, tableName, tableMetadata, tx);
    this.isUsingIndex = isUsingIndex;
  }


  public void setTx(Transaction tx) {
    this.tx = tx;
  }

  public Transaction getTx() {
    return tx;
  }

  public void abort() {
    if (iterator != null) {
      iterator.cancel();
    }

    if (tx != null) {
      FDBHelper.abortTransaction(tx);
    }

    tx = null;
  }

  public void commit() {
    if (iterator != null) {
      iterator.cancel();
    }
    if (tx != null) {
      FDBHelper.commitTransaction(tx);
    }

    tx = null;
  }

  public final Mode getMode() {
    return mode;
  }

  public boolean isInitialized() {
    return isInitialized;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  public void setTableMetadata(TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public void enablePredicate(String attrName, Record.Value value, ComparisonOperator operator) {
    this.predicateAttributeName = attrName;
    this.predicateAttributeValue = value;
    this.predicateOperator = operator;
    this.isPredicateEnabled = true;

    // if using index subspace
    if (isUsingIndex)
    {
      indexSubspace = FDBHelper.getIndexSubspace(tx, tableName, attrName);
    }

  }



  private Record moveToNextRecord(boolean isInitializing) {
    if (!isInitializing && !isInitialized) {
      return null;
    }

    if (isInitializing) {
      // initialize the subspace and the iterator
      recordsTransformer = new RecordsTransformer(getTableName(), getTableMetadata());
      directorySubspace = FDBHelper.openSubspace(tx, recordsTransformer.getTableRecordPath());
      AsyncIterable<KeyValue> fdbIterable = FDBHelper.getKVPairIterableOfDirectory(directorySubspace, tx, isInitializedToLast);
      if (fdbIterable != null)
        iterator = fdbIterable.iterator();

      isInitialized = true;
    }
    // reset the currentRecord
    currentRecord = null;

    // no such directory, or no records under the directory
    if (directorySubspace == null || !hasNext()) {
      return null;
    }

    List<String> recordStorePath = recordsTransformer.getTableRecordPath();
    List<FDBKVPair> fdbkvPairs = new ArrayList<>();

    boolean isSavePK = false;
    Tuple pkValTuple = new Tuple();
    Tuple tempPkValTuple = null;

    if (isMoved && currentKVPair != null) {
      fdbkvPairs.add(currentKVPair);
      pkValTuple = getPrimaryKeyValTuple(currentKVPair.getKey());
      isSavePK = true;
    }

    isMoved = true;
    boolean nextExists = false;

    while (iterator.hasNext()) {
      KeyValue kv = iterator.next();
      Tuple keyTuple = directorySubspace.unpack(kv.getKey());
      Tuple valTuple = Tuple.fromBytes(kv.getValue());
      FDBKVPair kvPair = new FDBKVPair(recordStorePath, keyTuple, valTuple);
      tempPkValTuple = getPrimaryKeyValTuple(keyTuple);
      if (!isSavePK) {
        pkValTuple = tempPkValTuple;
        isSavePK = true;
      } else if (!pkValTuple.equals(tempPkValTuple)){
        // when pkVal change, stop there
        currentKVPair = kvPair;
        nextExists = true;
        break;
      }
      fdbkvPairs.add(kvPair);
    }
    if (!fdbkvPairs.isEmpty()) {
      currentRecord = recordsTransformer.convertBackToRecord(fdbkvPairs);
    }

    if (!nextExists) {
      currentKVPair = null;
    }
    return currentRecord;
  }

  // TODO
  public Record moveToNextUsingIndex(boolean isInitializing)
  {
    if (isInitializing) {
      recordsTransformer = new RecordsTransformer(getTableName(), getTableMetadata());
      directorySubspace = FDBHelper.openSubspace(tx, recordsTransformer.getTableRecordPath());
      recordStorePath = recordsTransformer.getTableRecordPath();
      AsyncIterable<KeyValue> fdbIterable = FDBHelper.getKVPairIterableOfDirectory(directorySubspace, tx, isInitializedToLast);
      if (fdbIterable != null)
        iterator = fdbIterable.iterator();

      // initialize indexIterator
      AsyncIterable<KeyValue> indexIterable  = FDBHelper.getKVPairIterableOfDirectory(indexSubspace, tx, false);
      if (indexIterable != null)
      {
        indexIterator = indexIterable.iterator();
      }
      else
      {
        System.out.println("Index Iterable is null");
        return null;
      }
      isInitialized = true;
    }
    // get pk Value
    if (indexIterator.hasNext())
    {
      KeyValue kv = indexIterator.next();
      FDBKVPair kvPair = FDBHelper.convertKeyValueToFDBKVPair(tx, FDBHelper.getIndexPath(tx, tableName, predicateAttributeName), kv);

      Tuple keyTuple = kvPair.getKey();

      if (!isIndexTypeInitialized)
      {
        // read typing
        int typeCode = (int)keyTuple.getLong(1);
        if (typeCode == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX.ordinal())
        {
          System.out.println("B_PLUS entered");
          indexType = IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX;
        }
        // otherwise, don't change because set to hash by default
        System.out.println("typeCode : " + typeCode + ", ordinalVal: " + IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX.ordinal());
        isIndexTypeInitialized = true;
      }

      Tuple insidePrimaryTuple = keyTuple.getNestedTuple(keyTuple.size() - 1);

      System.out.println(insidePrimaryTuple + " : queried primaryTuple");

      // make record in main data, starting from ssn key
      AsyncIterable<KeyValue> mainDataIterable = FDBHelper.getKVPairIterableWithPrefixInDirectory(directorySubspace, tx, insidePrimaryTuple, false);
      AsyncIterator<KeyValue> mainDataIterator = mainDataIterable.iterator();

      List<FDBKVPair> pairsToBeRecord = new ArrayList<>();

      while (mainDataIterator.hasNext())
      {
        //System.out.println("making record");
        KeyValue itKV = mainDataIterator.next();
        //System.out.println(itKV.toString());
        //Tuple valueTuple = Tuple.fromBytes(itKV.getValue());
        FDBKVPair itPair = FDBHelper.convertKeyValueToFDBKVPair(tx, recordStorePath, itKV);
        //System.out.print(itPair.getKey().toString() + " k, v: " + valueTuple.toString());
        pairsToBeRecord.add(itPair);
      }

      Record res = recordsTransformer.convertBackToRecord(pairsToBeRecord);
      System.out.println("record: ");
      System.out.println("ssn: " + res.getValueForGivenAttrName("SSN"));
      System.out.println("name: " + res.getValueForGivenAttrName("Name"));
      System.out.println("salary: " + res.getValueForGivenAttrName("Salary"));
      System.out.println("address: " + res.getValueForGivenAttrName("Address"));
      System.out.println("email: " + res.getValueForGivenAttrName("Email"));

      System.out.println("Printing Map");
      // convert
      for (Map.Entry e : res.getMapAttrNameToValue().entrySet())
      {
        System.out.println("key: " + e.getKey().toString() + ", Val: " + e.getValue().toString());
      }
      return res;

    }
    // get keyQuery using predicate value

    Tuple queryTuple = new Tuple();
    queryTuple = queryTuple.add(tableName);
    //queryTuple = queryTuple.add()
    return null;
  }

  public Record getFirst() {
    if (isInitialized) {
      return null;
    }
    isInitializedToLast = false;

    if (isUsingIndex)
    {
      return moveToNextUsingIndex(true);
    }

    Record record = moveToNextRecord(true);
    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        record = moveToNextRecord(false);
      }
    }
    return record;
  }

  private boolean doesRecordMatchPredicate(Record record) {
    Object recVal = record.getValueForGivenAttrName(predicateAttributeName);
    AttributeType recType = record.getTypeForGivenAttrName(predicateAttributeName);
    if (recVal == null || recType == null) {
      // attribute not exists in this record
      return false;
    }

    if (recType == AttributeType.INT) {
      return ComparisonUtils.compareTwoINT(recVal, predicateAttributeValue.getValue(), predicateOperator);
    } else if (recType == AttributeType.DOUBLE){
      return ComparisonUtils.compareTwoDOUBLE(recVal, predicateAttributeValue.getValue(), predicateOperator);
    } else if (recType == AttributeType.VARCHAR) {
      return ComparisonUtils.compareTwoVARCHAR(recVal, predicateAttributeValue.getValue(), predicateOperator);
    }

    return false;
  }

  public Record getLast() {
    if (isInitialized) {
      return null;
    }
    isInitializedToLast = true;

    Record record = moveToNextRecord(true);
    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        record = moveToNextRecord(false);
      }
    }
    return record;
  }

  public boolean hasNext() {
    return isInitialized && iterator != null && (iterator.hasNext() || currentKVPair != null);
  }

  // need method to traverse index Structure
  public void readTypeOfIndex()
  {

  }


  // need method to convert obtained pkValue from indexStructure to use in obtaining mainData to make record

  public Record next(boolean isGetPrevious) {
    if (!isInitialized) {
      return null;
    }
    if (isGetPrevious != isInitializedToLast) {
      return null;
    }
    Record record = null;
    if (isUsingIndex)
    {
      record = moveToNextUsingIndex(false);
    }

    record = moveToNextRecord(false);

    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        record = moveToNextRecord(false);
      }
    }
    return record;
  }

  public Record getCurrentRecord() {
    return currentRecord;
  }

  public StatusCode updateCurrentRecord(String[] attrNames, Object[] attrValues) {
    if (tx == null) {
      return StatusCode.CURSOR_INVALID;
    }

    if (!isInitialized) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }

    if (currentRecord == null) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }

    Set<String> currentAttrNames = currentRecord.getMapAttrNameToValue().keySet();
    Set<String> primaryKeys = new HashSet<>(tableMetadata.getPrimaryKeys());

    boolean isUpdatingPK = false;
    for (int i = 0; i<attrNames.length; i++) {
      String attrNameToUpdate = attrNames[i];
      Object attrValToUpdate = attrValues[i];

      if (!currentAttrNames.contains(attrNameToUpdate)) {
        return StatusCode.CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND;
      }

      if (!Record.Value.isTypeSupported(attrValToUpdate)) {
        return StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
      }

      if (!isUpdatingPK && primaryKeys.contains(attrNameToUpdate)) {
        isUpdatingPK = true;
      }
    }

    if (isUpdatingPK) {
      // delete the old record first
      StatusCode deleteStatus = deleteCurrentRecord();
      if (deleteStatus != StatusCode.SUCCESS) {
        return deleteStatus;
      }
    }

    for (int i = 0; i<attrNames.length; i++) {
      String attrNameToUpdate = attrNames[i];
      Object attrValToUpdate = attrValues[i];
      currentRecord.setAttrNameAndValue(attrNameToUpdate, attrValToUpdate);
    }

    List<FDBKVPair> kvPairsToUpdate = recordsTransformer.convertToFDBKVPairs(currentRecord);
    for (FDBKVPair kv : kvPairsToUpdate) {
      FDBHelper.setFDBKVPair(directorySubspace, tx, kv);
    }
    return StatusCode.SUCCESS;
  }

  public StatusCode deleteCurrentRecord() {
    if (tx == null) {
      return StatusCode.CURSOR_INVALID;
    }

    if (!isInitialized) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }

    if (currentRecord == null) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }

    List<FDBKVPair> kvPairsToDelete = recordsTransformer.convertToFDBKVPairs(currentRecord);
    for (FDBKVPair kv : kvPairsToDelete) {
      FDBHelper.removeKeyValuePair(directorySubspace, tx, kv.getKey());
    }

    return StatusCode.SUCCESS;
  }
}
