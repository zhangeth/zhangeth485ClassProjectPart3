package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;

/**
 * Cursor hooks to a certain attribute in a table.
 *
 * It will traverse the qualified records in this table based on its direction, comparison operator and attribute value.
 *
 * It is created by {#Records.openCursor}
 */
public abstract class Cursor {

  public enum Direction {
    ORDER,
    REVERSE_ORDER
  }

  private TableMetadata tableMetadata;

  private ComparisonOperator operator;

  /**
   * Direction is set when {#Records.getFirst} or {#Records.getLast} is called
   */
  private Direction direction;

  /**
   * The attributeName that the Cursor is hooked to.
   */
  private String attrName;

  /**
   * Check if the next record reaches to the EOF. Cursor itself will not move to the next position.
   * @return true if the next record exists
   */
  abstract public boolean hasNext();

  /**
   * Move to the next qualified record and return the actual record.
   * @return the record
   */
  abstract public Record next();

  /**
   * Close the cursor. Once the cursor close, all operations related to this cursor should be invalid.
   * @return StatusCode
   */
  abstract StatusCode close();


}
