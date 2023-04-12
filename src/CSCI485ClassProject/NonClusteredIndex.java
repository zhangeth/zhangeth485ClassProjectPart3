package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class NonClusteredIndex {

    //public static HashMap<String, >
    private static RecordsImpl recordsImpl;

    public static void buildNonClusteredIndex(Database db, Transaction tx, String tableName, String targetAttrName, IndexType indexType)
    {

        recordsImpl = new RecordsImpl();
        // actually make cursor to iterate over all records

        Cursor cursor = recordsImpl.openCursor(tableName, Cursor.Mode.READ);
        Record rec = recordsImpl.getFirst(cursor);

        List<String> path = new ArrayList<>();
        path.add(tableName);

        TableMetadataTransformer tbmTransformer = new TableMetadataTransformer(tableName);

        TableMetadata tbm = tbmTransformer.convertBackToTableMetadata(FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, tbmTransformer.getTableAttributeStorePath()));

        List<String> pKeys = tbm.getPrimaryKeys();

        while (rec != null)
        {
            Tuple pkValue = new Tuple();
            // convert into NonClusteredHashIndexRecord, just one primaryKey
            for (String pKey : pKeys)
            {
                pkValue = pkValue.addObject(rec.getValueForGivenAttrName(pKey));
            }

            Long attrValue = 0L;
            // if hash type, hash it, otherwise store reg value for sorting purposes

            if (indexType == IndexType.NON_CLUSTERED_HASH_INDEX)
                attrValue = Long.valueOf(rec.getValueForGivenAttrName(targetAttrName).hashCode());
            else
                attrValue = Long.valueOf((long)rec.getValueForGivenAttrName(targetAttrName));

            NonClusteredIndexRecord nchRecord = new NonClusteredIndexRecord(tableName, targetAttrName, attrValue, pkValue, indexType);

            nchRecord.setRecord(tx);

            rec = recordsImpl.getNext(cursor);
        }

        FDBHelper.commitTransaction(tx);

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
            System.out.println("B_PLUS type");
        }
        else {
            idxType = IndexType.NON_CLUSTERED_HASH_INDEX;
        }
        // otherwise, don't change because set to hash by default
        System.out.println("typeCode : " + typeCode);

        NonClusteredIndexRecord rec = new NonClusteredIndexRecord(tableName, targetAttrName, (Long)targetAttrVal, pkVal, idxType);
        rec.setRecord(tx);

    }


    public static void commitIndex()
            // keyTuple: [tableName, attrName, hashAttrValue, pkValu], valueTuple; ""
            // instead of going through whole list, maybe do one by one commit in above
    {}

}
