package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
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

            Long attrValue = Long.valueOf(rec.getValueForGivenAttrName(targetAttrName).hashCode());

            NonClusteredHashIndexRecord nchRecord = new NonClusteredHashIndexRecord(tableName, targetAttrName, attrValue, pkValue);

            nchRecord.setRecord(tx);

            rec = recordsImpl.getNext(cursor);
        }

        FDBHelper.commitTransaction(tx);

    }

    public static void commitIndex()
            // keyTuple: [tableName, attrName, hashAttrValue, pkValu], valueTuple; ""
            // instead of going through whole list, maybe do one by one commit in above
    {}

}
