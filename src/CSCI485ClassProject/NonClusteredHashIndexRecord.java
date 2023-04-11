package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.IndexType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class NonClusteredHashIndexRecord {

    private String tableName;
    private static IndexType indexType = IndexType.NON_CLUSTERED_HASH_INDEX;
    private String hashAttrName;
    private Long hashValue;
    private Tuple pkValue;

    public NonClusteredHashIndexRecord(String tableName, String hashAttrName, Long hashValue, Tuple pkValue) {
        this.tableName = tableName;
        // index type is automatic non-clustered
        this.hashAttrName = hashAttrName;
        this.hashValue = hashValue;
        this.pkValue = pkValue;
    }

    public Tuple getKeyTuple() {
        Tuple keyTuple = new Tuple();
        keyTuple = keyTuple.add(tableName).add(indexType.hashCode()).add(hashAttrName).add(hashValue).add(pkValue);
        return keyTuple;
    }

    public Tuple getValueTuple() {
        Tuple valueTuple = new Tuple();
        return valueTuple;
    }

    public void setRecord(Transaction tx)
    {
        String indexSubstr = hashAttrName + "Index";

        List<String> indexSubspacePath = new ArrayList<>();
        indexSubspacePath.add(tableName); indexSubspacePath.add(indexSubstr);

        DirectorySubspace indexSubspace = FDBHelper.createOrOpenSubspace(tx, indexSubspacePath);
        // make key value pair
        FDBKVPair kvPair = new FDBKVPair(indexSubspacePath, getKeyTuple(), getValueTuple());

        FDBHelper.setFDBKVPair(indexSubspace, tx, kvPair);
    }




    public static Tuple getPrefixQueryTuple(String tableName, String hashAttrName, Long hashValue) {
        Tuple keyTuple = new Tuple();
        keyTuple = keyTuple.add(tableName).add(indexType.hashCode()).add(hashAttrName).add(hashValue);
        return keyTuple;
    }

    public String toString() {
        // loop through properties to print
        String s = "tableName: ";
        s += tableName;
        s += ", hashAttrName: " + hashAttrName;
        s += ", hashAttrVal: " + hashValue;
        s += ", pKey: " + pkValue;
        return s;
    }


}
