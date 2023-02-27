package CSCI485ClassProject.models;


import CSCI485ClassProject.StatusCode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static CSCI485ClassProject.StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
import static CSCI485ClassProject.StatusCode.DATA_RECORD_VALUE_TYPE_UNMATCHED;
import static CSCI485ClassProject.StatusCode.SUCCESS;

/**
 * DataRecord represents a row of a Table. It is identified by its primary key(s)
 */
public class DataRecord {

  public class Value {
    private AttributeType attributeType;

    private Object value;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Value value1 = (Value) o;
      return attributeType == value1.attributeType && Objects.equals(value, value1.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(attributeType, value);
    }

    public Value() {}

    public AttributeType getAttributeType() {
      return attributeType;
    }

    public void setAttributeType(AttributeType attributeType) {
      this.attributeType = attributeType;
    }

    public Object getValue() {
      return value;
    }

    public StatusCode setValue(AttributeType attrType, Object value) {
      if ((attrType == AttributeType.INT && !(value instanceof Integer))
          || (attrType == AttributeType.VARCHAR && !(value instanceof String))
          || (attrType == AttributeType.DOUBLE && !(value instanceof Double)))
        return DATA_RECORD_VALUE_TYPE_UNMATCHED;

      this.value = value;
      this.attributeType = attrType;
      return SUCCESS;
    }
  }

  private String tableName;

  private HashMap<String, Value> attributeName2Value;

  private List<String> primaryKeyAttributeNames;

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public HashMap<String, Value> getAttributeName2Value() {
    return attributeName2Value;
  }

  public void setAttributeName2Value(HashMap<String, Value> attributeName2Value) {
    this.attributeName2Value = attributeName2Value;
  }

  public List<String> getPrimaryKeyAttributeNames() {
    return primaryKeyAttributeNames;
  }

  public StatusCode setPrimaryKeyAttributeNames(List<String> primaryKeyAttributeNames) {
    Set<String> pkSet = new HashSet<>(primaryKeyAttributeNames);

    if (!attributeName2Value.keySet().containsAll(pkSet)) {
      return DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
    }
    this.primaryKeyAttributeNames = primaryKeyAttributeNames;
    return SUCCESS;
  }
}
