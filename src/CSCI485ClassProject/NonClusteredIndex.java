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

}
