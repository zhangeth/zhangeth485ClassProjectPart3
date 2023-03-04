package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;

public abstract class Cursor {

  public enum Mode {
    READ_ONLY,
    READ_WRITE,
    WRITE_ONLY
  }

  private TableMetadata tableMetadata;

  private ComparisonOperator operator;

  private Mode mode;

  public final Mode getMode() {
    return mode;
  }

  public final void setMode(Mode mode) {
    this.mode = mode;
  }

  /**
   * The attributeName that the Cursor may hooks to.
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
