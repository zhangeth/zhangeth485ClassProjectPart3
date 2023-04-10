package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.util.ArrayList;
import java.util.List;

public class NonClusteredHashIndex {

    //public static HashMap<String, >
    private static RecordsImpl recordsImpl;

    public static List<NonClusteredHashIndexRecord> buildNonClusteredHashIndex(Database db, Transaction tx, String tableName, String targetAttrName)
    {

        List<NonClusteredHashIndexRecord> res = new ArrayList<>();
        recordsImpl = new RecordsImpl();
        // actually make cursor to iterate over all records

        Cursor cursor = recordsImpl.openCursor(tableName, Cursor.Mode.READ);
        Record rec = recordsImpl.getFirst(cursor);

        List<String> path = new ArrayList<>();
        path.add(tableName);

        TableMetadataTransformer tbmTransformer = new TableMetadataTransformer(tableName);

        TableMetadata tbm = tbmTransformer.convertBackToTableMetadata(FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, path));

        List<String> pKeys = tbm.getPrimaryKeys();

        while (rec != null)
        {
            long pKValue = 0;
            // convert into NonClusteredHashIndexRecord, just one primaryKey
            for (String pKey : pKeys)
            {
                pKValue = (long)rec.getValueForGivenAttrName(pKey);
                System.out.print("pKey" + pKey + " value: " + rec.getValueForGivenAttrName(pKey) + " converted val: " + pKValue);
            }

            Long attrValue = Long.valueOf((long)rec.getValueForGivenAttrName(targetAttrName).hashCode());

            NonClusteredHashIndexRecord nchRecord = new NonClusteredHashIndexRecord(tableName, targetAttrName, attrValue, Long.valueOf(pKValue));
            // get pkValue
            res.add(nchRecord);

            rec = recordsImpl.getNext(cursor);
        }

        return res;
    }
}
