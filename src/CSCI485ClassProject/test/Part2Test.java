package CSCI485ClassProject.test;

import CSCI485ClassProject.Cursor;
import CSCI485ClassProject.Indexes;
import CSCI485ClassProject.IndexesImpl;
import CSCI485ClassProject.Records;
import CSCI485ClassProject.RecordsImpl;
import CSCI485ClassProject.StatusCode;
import CSCI485ClassProject.TableManager;
import CSCI485ClassProject.TableManagerImpl;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class Part2Test {

  public static String EmployeeTableName = "Employee";
  public static String[] EmployeeTableAttributeNames = new String[]{"SSN", "Name"};
  public static AttributeType[] EmployeeTableAttributeTypes =
      new AttributeType[]{AttributeType.INT, AttributeType.VARCHAR};
  public static String[] EmployeeTablePKAttributes = new String[]{"SSN"};

  public static int seed = 485;
  public static int updateSeed1 = 488;
  public static int updateSeed2 = 600;


  public static int initialNumberOfRecords = 50;
  public static int updatedNumberOfRecords = initialNumberOfRecords / 2;
  public static int numberOfRecords = 0;

  private TableManager tableManager;
  private Records records;
  private Indexes indexes;

  @Before
  public void init(){
    tableManager = new TableManagerImpl();
    records = new RecordsImpl();
    indexes = new IndexesImpl();
  }

  @Test
  public void unitTest1() {
    tableManager.dropAllTables();

    // create the Employee Table, verify that the table is created
    TableMetadata EmployeeTable = new TableMetadata(EmployeeTableAttributeNames, EmployeeTableAttributeTypes,
        EmployeeTablePKAttributes);
    assertEquals(StatusCode.SUCCESS, tableManager.createTable(EmployeeTableName,
        EmployeeTableAttributeNames, EmployeeTableAttributeTypes, EmployeeTablePKAttributes));
    HashMap<String, TableMetadata> tables = tableManager.listTables();
    assertEquals(1, tables.size());
    assertEquals(EmployeeTable, tables.get(EmployeeTableName));

    Random generator = new Random(seed);
    for (int i = 0; i<initialNumberOfRecords; i++) {
      int idx = generator.nextInt(Integer.MAX_VALUE);
      String name = "Name" + idx;
      long ssn = i;
      assertEquals(StatusCode.SUCCESS, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, new Object[]{ssn}, new String[]{"Name"}, new Object[] {name}));
      numberOfRecords++;
    }

    assertEquals(StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED, records.insertRecord(EmployeeTableName, new String[]{}, new String[]{}, new String[]{"Name"}, new Object[]{"Bob"}));
    assertEquals(StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, new Object[]{initialNumberOfRecords+1}, new String[]{"Name"}, new Object[]{12345}));

    System.out.println("Test1 pass!");
  }

  @Test
  public void unitTest2() {
    Cursor cursor = records.openCursor(EmployeeTableName, Cursor.Mode.READ_ONLY);
    assertNotNull(cursor);

    List<Record> employRecords = new ArrayList<>();
    employRecords.add(records.getFirst(cursor));

    while (true) {
      Record rec = records.getNext(cursor);
      if (rec == null) {
        break;
      }
      employRecords.add(rec);
    }

    List<Record> expectRecords = new ArrayList<>();
    Random generator = new Random(seed);


    for (int i = 0; i<initialNumberOfRecords; i++) {
      int idx = generator.nextInt(Integer.MAX_VALUE);
      String name = "Name" + idx;
      long ssn = i;

      Record rec = new Record();
      rec.setAttrNameAndValue("Name", name);
      rec.setAttrNameAndValue("SSN", ssn);
      expectRecords.add(rec);
    }

    records.abortCursor(cursor);
    // verify the initial records is inserted
    assertEquals(initialNumberOfRecords, employRecords.size());
    assertEquals(true, expectRecords.containsAll(employRecords));
    assertEquals(true, employRecords.containsAll(expectRecords));

    generator = new Random(updateSeed1);

    for (int i = 0; i<updatedNumberOfRecords; i++) {
      int randNum = generator.nextInt(Integer.MAX_VALUE);
      String name = "Name" + randNum;
      long salary = randNum;
      long ssn = numberOfRecords;
      assertEquals(StatusCode.SUCCESS, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, new Object[]{ssn}, new String[]{"Name", "Salary"}, new Object[] {name, salary}));
      numberOfRecords++;
    }


    TableMetadata updatedEmployeeTable = new TableMetadata();
    updatedEmployeeTable.addAttribute("SSN", AttributeType.INT);
    updatedEmployeeTable.addAttribute("Name", AttributeType.VARCHAR);
    updatedEmployeeTable.addAttribute("Salary", AttributeType.INT);
    updatedEmployeeTable.setPrimaryKeys(Collections.singletonList("SSN"));

    // verify the schema changing
    HashMap<String, TableMetadata> tables = tableManager.listTables();
    assertEquals(1, tables.size());
    assertEquals(updatedEmployeeTable, tables.get(EmployeeTableName));

    generator = new Random(updateSeed1);
    // verify the data is inserted
    for (int i = 0; i<initialNumberOfRecords / 2; i++) {
      int randNum = generator.nextInt(Integer.MAX_VALUE);
      long salary = i+1;
      long ssn = i + initialNumberOfRecords;
      String name = "Name" + randNum;
      cursor = records.openCursor(EmployeeTableName, "Salary", salary, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ_ONLY, false);

      Record record = records.getFirst(cursor);
      assertNotNull(record);
      assertEquals(ssn, record.getValueForGivenAttrName("SSN"));
      assertEquals(salary, record.getValueForGivenAttrName("Salary"));
      assertEquals(name, record.getValueForGivenAttrName("Name"));

      assertNull(records.getNext(cursor));
      assertNull(records.getPrevious(cursor));
    }

    System.out.println("Test2 pass!");
  }

  @Test
  public void unitTest3() {
    // create the B+Tree index on the Salary property
    assertEquals(StatusCode.SUCCESS, indexes.createIndex(EmployeeTableName, "Salary", IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX));

    int offset = 1;
    Cursor cursor = records.openCursor(EmployeeTableName, "Salary", offset, ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, Cursor.Mode.READ_ONLY, true);

    // get 10 records with Salary >= 1 using cursor
    List<Record> employSalRecord = new ArrayList<>();
    employSalRecord.add(records.getFirst(cursor));
    for (int i = 1; i < 10; i++) {
      employSalRecord.add(records.getNext(cursor));
    }
    records.abortCursor(cursor);

    // verify the record is correct
    Random generator = new Random(updateSeed1);
    for (int i = 1; i < offset; i++) {
      generator.nextInt(Integer.MAX_VALUE);
    }
    for (int i = 0; i < 10; i++) {
      int randNum = generator.nextInt(Integer.MAX_VALUE);
      long salary = i + offset;
      long ssn = i + initialNumberOfRecords + offset;
      String name = "Name" + randNum;

      Record record = employSalRecord.get(i);
      assertNotNull(record);
      assertEquals(ssn, record.getValueForGivenAttrName("SSN"));
      assertEquals(salary, record.getValueForGivenAttrName("Salary"));
      assertEquals(name, record.getValueForGivenAttrName("Name"));
    }

    System.out.println("Test3 pass!");
  }

  @Test
  public void unitTest4() {
    int offset = 4000;
    Cursor cursor = records.openCursor(EmployeeTableName, "Salary", offset, ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, Cursor.Mode.READ_ONLY, true);

    // get 10 records with Salary >= 4000 using cursor
    List<Record> employSalRecord = new ArrayList<>();
    employSalRecord.add(records.getFirst(cursor));
    for (int i = 1; i < 10; i++) {
      employSalRecord.add(records.getNext(cursor));
    }
    records.abortCursor(cursor);

    // verify the record is correct
    Random generator = new Random(updateSeed1);
    for (int i = 1; i < offset; i++) {
      generator.nextInt(Integer.MAX_VALUE);
    }
    for (int i = 0; i < 10; i++) {
      int randNum = generator.nextInt(Integer.MAX_VALUE);
      long salary = i + offset;
      long ssn = i + initialNumberOfRecords + offset;
      String name = "Name" + randNum;

      Record record = employSalRecord.get(i);
      assertNotNull(record);
      assertEquals(ssn, record.getValueForGivenAttrName("SSN"));
      assertEquals(salary, record.getValueForGivenAttrName("Salary"));
      assertEquals(name, record.getValueForGivenAttrName("Name"));
    }

    generator = new Random(updateSeed1);
    assertEquals(StatusCode.SUCCESS, indexes.createIndex(EmployeeTableName, "Salary", IndexType.NON_CLUSTERED_HASH_INDEX));
    for (int i = 0; i<100; i++) {
      int randNum = generator.nextInt(Integer.MAX_VALUE);
      long salary = i + 1;
      long ssn = i + initialNumberOfRecords;
      String name = "Name" + randNum;

      cursor = records.openCursor(EmployeeTableName, "Name", name, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ_ONLY, true);
      Record record = records.getFirst(cursor);

      assertNotNull(record);
      assertEquals(ssn, record.getValueForGivenAttrName("SSN"));
      assertEquals(salary, record.getValueForGivenAttrName("Salary"));
      assertEquals(name, record.getValueForGivenAttrName("Name"));

      records.abortCursor(cursor);
    }

    // insert records to see if the index structure updated
    long nextSSN = numberOfRecords;
    generator = new Random(updateSeed2);
    for (int i = 0; i<100; i++) {
      int randNum = generator.nextInt(Integer.MAX_VALUE);
      long ssn = numberOfRecords;
      long salary = numberOfRecords;
      String name = "Name" + randNum;
      assertEquals(StatusCode.SUCCESS, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, new Object[]{ssn}, new String[]{"Name", "Salary"}, new Object[] {name, salary}));
      numberOfRecords++;
    }

    // delete the records
    for (int i = 0; i<100; i--) {
      long ssn = nextSSN + i;
      assertEquals(StatusCode.SUCCESS, records.deleteDataRecord(EmployeeTableName, new String[]{"SSN"}, new Object[]{ssn}));
      numberOfRecords--;
    }

    System.out.println("Test4 pass!");
  }

  @Test
  public void unitTest5() {
    for (int i = 0; i<updatedNumberOfRecords; i++) {
      long ssn = i;
      long salary = i+1;

      if (salary % 2 == 0) {
        continue;
      }
      // delete the odd Salary records
      Cursor cursor = records.openCursor(EmployeeTableName, "Salary", salary, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ_WRITE, true);
      Record rec = records.getFirst(cursor);

      assertEquals(ssn, rec.getValueForGivenAttrName("SSN"));
      assertEquals(salary, rec.getValueForGivenAttrName("Salary"));
      assertEquals(StatusCode.SUCCESS, records.deleteRecord(cursor));
      records.commitCursor(cursor);
      numberOfRecords--;
    }

    // verify the deletion
    for (int i = 0; i<updatedNumberOfRecords; i++) {
      long ssn = i + initialNumberOfRecords;
      long salary = i+1;

      Cursor cursor = records.openCursor(EmployeeTableName, "Salary", salary, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ_ONLY, true);
      Record rec = records.getFirst(cursor);
      if (salary % 2 == 0) {
        assertNotNull(rec);
        assertEquals(ssn, rec.getValueForGivenAttrName("SSN"));
        assertEquals(salary, rec.getValueForGivenAttrName("Salary"));
      } else {
        assertNull(rec);
      }
      records.abortCursor(cursor);
    }

    System.out.println("Test5 pass!");
  }

  @Test
  public void unitTest6() {
    // insert odd number records back
    Random generator = new Random(updateSeed1);
    for (int i = 0; i<updatedNumberOfRecords; i++) {
      int randNum = generator.nextInt(Integer.MAX_VALUE);
      long ssn = i + initialNumberOfRecords;
      long salary = i+1;
      String name = "Name" + randNum;

      if (salary % 2 == 0) {
        continue;
      }
      // insert records
      assertEquals(StatusCode.SUCCESS, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, new Object[]{ssn}, new String[]{"Name", "Salary"}, new Object[] {name, salary}));
      numberOfRecords++;
    }

    // verify the insertion
    generator = new Random(updateSeed1);
    for (int i = 0; i<updatedNumberOfRecords; i++) {
      int randNum = generator.nextInt(Integer.MAX_VALUE);
      long ssn = i + initialNumberOfRecords;
      long salary = i+1;
      String name = "Name" + randNum;

      Cursor cursor = records.openCursor(EmployeeTableName, "Salary", salary, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ_ONLY, true);
      Record rec = records.getFirst(cursor);
      assertNotNull(rec);
      assertEquals(ssn, rec.getValueForGivenAttrName("SSN"));
      assertEquals(salary, rec.getValueForGivenAttrName("Salary"));
      assertEquals(name, rec.getValueForGivenAttrName("Name"));
    }

    // try to insert even numbered records and delete the odd numbered records
    generator = new Random(updateSeed1);
    for (int i = 0; i<updatedNumberOfRecords; i++) {
      int randNum = generator.nextInt(Integer.MAX_VALUE);
      long ssn = i + initialNumberOfRecords;
      long salary = i+1;
      String name = "Name" + randNum;

      if (salary % 2 == 0) {
        assertEquals(StatusCode.DATA_RECORD_CREATION_RECORD_ALREADY_EXISTS, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, new Object[]{ssn}, new String[]{"Name", "Salary"}, new Object[] {name, salary}));
      } else {
        // delete the odd numbered records
        Cursor cursor = records.openCursor(EmployeeTableName, "Salary", salary, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ_WRITE, true);
        Record rec = records.getFirst(cursor);

        assertEquals(ssn, rec.getValueForGivenAttrName("SSN"));
        assertEquals(salary, rec.getValueForGivenAttrName("Salary"));
        assertEquals(name, rec.getValueForGivenAttrName("Name"));

        assertEquals(StatusCode.SUCCESS, records.deleteRecord(cursor));
        records.commitCursor(cursor);
        numberOfRecords--;
      }
    }

    // verify the deletion
    generator = new Random(updateSeed1);
    for (int i = 0; i<updatedNumberOfRecords; i++) {
      long ssn = i + initialNumberOfRecords;
      long salary = i+1;

      Cursor cursor = records.openCursor(EmployeeTableName, "Salary", salary, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ_ONLY, true);
      Record rec = records.getFirst(cursor);
      if (salary % 2 == 0) {
        assertNotNull(rec);
        assertEquals(ssn, rec.getValueForGivenAttrName("SSN"));
        assertEquals(salary, rec.getValueForGivenAttrName("Salary"));
      } else {
        assertNull(rec);
      }
      records.abortCursor(cursor);
    }

    // update the even number salary to be odd number salary
    generator = new Random(updateSeed1);
    for (int i = 0; i<updatedNumberOfRecords; i++) {
      int randNum = generator.nextInt(Integer.MAX_VALUE);
      long ssn = i + initialNumberOfRecords;
      long salary = i+1;
      String name = "Name" + randNum;

      if (salary % 2 == 0) {
        Cursor cursor = records.openCursor(EmployeeTableName, "Salary", salary, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ_WRITE, true);
        Record rec = records.getFirst(cursor);

        assertNotNull(rec);
        assertEquals(ssn, rec.getValueForGivenAttrName("SSN"));
        assertEquals(salary, rec.getValueForGivenAttrName("Salary"));
        assertEquals(name, rec.getValueForGivenAttrName("Name"));

        assertEquals(StatusCode.SUCCESS, records.updateRecord(cursor, new String[]{"Salary"}, new Object[]{salary+1}));

        records.commitCursor(cursor);
      }
    }

    // verify the even number salary gone and odd number salary exists
    for (int i = 0; i<updatedNumberOfRecords; i++) {
      long ssn = i + initialNumberOfRecords;
      long salary = i+1;

      Cursor cursor = records.openCursor(EmployeeTableName, "Salary", salary, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ_ONLY, true);
      Record rec = records.getFirst(cursor);
      if (salary % 2 == 1) {
        assertNotNull(rec);
        assertEquals(ssn-1, rec.getValueForGivenAttrName("SSN"));
        assertEquals(salary, rec.getValueForGivenAttrName("Salary"));
      } else {
        assertNull(rec);
      }
      records.abortCursor(cursor);
    }
    System.out.println("Test6 pass!");
  }

  @Test
  public void unitTest7() {
    assertEquals(StatusCode.INDEX_ALREADY_EXISTS_ON_ATTRIBUTE, indexes.createIndex(EmployeeTableName, "Salary", IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX));
    assertEquals(StatusCode.INDEX_NOT_FOUND, indexes.removeIndex(EmployeeTableName, "Salary"));
    assertEquals(StatusCode.INDEX_NOT_FOUND, indexes.removeIndex(EmployeeTableName, "SSN"));
    assertEquals(StatusCode.SUCCESS, indexes.removeIndex(EmployeeTableName, "Salary"));
    assertEquals(StatusCode.SUCCESS, indexes.removeIndex(EmployeeTableName, "Name"));

    System.out.println("Test7 pass!");
  }

  @Test
  public void unitTest8() {
    String perfTestTableName = "perfTest";
    String[] perfTestTableAttributes = new String[] {"attr1", "attr2"};
    String[] perfTestTablePrimaryKeyAttributes = new String[] {"attr1"};

    AttributeType[] perfTestTableAttrType = new AttributeType[] {AttributeType.INT, AttributeType.INT};
    int numberOfRecords = 1000000;

    assertEquals(StatusCode.SUCCESS, tableManager.createTable(perfTestTableName, perfTestTableAttributes, perfTestTableAttrType, perfTestTablePrimaryKeyAttributes));

    Random generator = new Random(seed);
    for (int i = 0; i < numberOfRecords; i++) {
      Long randNum = generator.nextLong();
      assertEquals(StatusCode.SUCCESS, records.insertRecord(perfTestTableName, new String[] {"attr1"}, new Object[]{randNum}, new String[] {"attr2"}, new Object[]{randNum}));
    }

    Cursor cursor;
    long start, end;


    start = System.currentTimeMillis();
    generator = new Random(seed);
    for (int i = 0; i < numberOfRecords; i++) {
      Long randNum = generator.nextLong();
      cursor = records.openCursor(perfTestTableName, "attr1", randNum, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ_ONLY, false);
      assertNotNull(records.getFirst(cursor));
      records.abortCursor(cursor);
    }
    end = System.currentTimeMillis();
    long elapsedTime = end - start;
    System.out.println("Query " + numberOfRecords + " without index: " + elapsedTime);

    // build the B+Tree index on attr2
    assertEquals(StatusCode.SUCCESS, indexes.createIndex(perfTestTableName, "attr2", IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX));
    start = System.currentTimeMillis();
    generator = new Random(seed);
    for (int i = 0; i < numberOfRecords; i++) {
      Long randNum = generator.nextLong();
      cursor = records.openCursor(perfTestTableName, "attr2", randNum, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ_ONLY, true);
      assertNotNull(records.getFirst(cursor));
      records.abortCursor(cursor);
    }
    end = System.currentTimeMillis();
    elapsedTime = end - start;
    System.out.println("Query with Non-clustered B+ Tree index: " + elapsedTime);

    assertEquals(StatusCode.SUCCESS, indexes.removeIndex(perfTestTableName, "attr2"));
    assertEquals(StatusCode.SUCCESS, indexes.createIndex(perfTestTableName, "attr2", IndexType.NON_CLUSTERED_HASH_INDEX));
    start = System.currentTimeMillis();
    generator = new Random(seed);
    for (int i = 0; i < numberOfRecords; i++) {
      Long randNum = generator.nextLong();
      cursor = records.openCursor(perfTestTableName, "attr2", randNum, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ_ONLY, true);
      assertNotNull(records.getFirst(cursor));
      records.abortCursor(cursor);
    }
    end = System.currentTimeMillis();
    elapsedTime = end - start;
    System.out.println("Query with Non-clustered Hash index: " + elapsedTime);

    System.out.println("Test8 pass!");

  }
}