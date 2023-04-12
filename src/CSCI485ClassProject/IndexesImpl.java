package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.IndexType;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
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

    List<String> indexPath = tablePath;
    indexPath.add(attrName + "Index");

    if (FDBHelper.doesSubdirectoryExists(tx, indexPath))
    {
      return StatusCode.INDEX_ALREADY_EXISTS_ON_ATTRIBUTE;
    }

    // loop through table subspace and make NonClusteredHashIndex
    NonClusteredIndex.buildNonClusteredIndex(db, tx, tableName, attrName, indexType);

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropIndex(String tableName, String attrName) {
    Transaction tx = FDBHelper.openTransaction(db);
    List<String> path =  FDBHelper.getIndexPath(tx, tableName, attrName);
    if (FDBHelper.doesIndexExist(tx, tableName, attrName))
    {
      FDBHelper.dropSubspace(tx, path);
      FDBHelper.commitTransaction(tx);
      return StatusCode.SUCCESS;
    }

    return StatusCode.INDEX_NOT_FOUND;
  }
}
