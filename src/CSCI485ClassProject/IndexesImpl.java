package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.util.ArrayList;
import java.util.List;


public class IndexesImpl implements Indexes{

  private Database db;

  // default constructor for passing test case
  public IndexesImpl()
  {
    db = FDBHelper.initialization();
  }

  @Override
  public StatusCode createIndex(String tableName, String attrName, IndexType indexType) {
    // check if table exists
    List<String> tablePath = new ArrayList<>();
    tablePath.add(tableName);

    Transaction tx = FDBHelper.openTransaction(db);

    if (!FDBHelper.doesSubdirectoryExists(tx, tablePath))
      return StatusCode.TABLE_NOT_FOUND;

    // make subdirectory for index for the attrName
    DirectorySubspace tableSubspace = FDBHelper.createOrOpenSubspace(tx, tablePath);

    // loop through table subspace and make NonClusteredHashIndex
    NonClusteredHashIndex.buildNonClusteredHashIndex(db, tx, tableName, attrName);
    // for now, assume it's a non_clustered_hash_index, ignore second type
    // create index structure from existing data, so we want to translate records into in memory hashmap, in which
    // the order of the key (hash) is tableName, targetAttrName, attrValue (hashValue), corresponding primaryKey
    tablePath.add("EmailIndex");
    // check subspace
    List<FDBKVPair> list =  FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, tablePath);

    for (FDBKVPair p : list)
    {
      System.out.println(p.getKey() + " : key" + p.getValue() + ": val");
    }

    System.out.println("reached end");
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropIndex(String tableName, String attrName) {
    // your code
    return null;
  }
}
