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
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class Part3Test {

  public static String EmployeeTableName = "Employee";
  public static String SSN = "SSN";
  public static String Name = "Name";
  public static String Email = "Email";
  public static String Age = "Age";
  public static String Address = "Address";
  public static String Salary = "Salary";

  public static String[] EmployeeTableAttributeNames = new String[]{SSN, Name, Email, Age, Address};
  public static String[] EmployeeTableNonPKAttributeNames = new String[]{Name, Email, Age, Address};
  public static AttributeType[] EmployeeTableAttributeTypes =
      new AttributeType[]{AttributeType.INT, AttributeType.VARCHAR, AttributeType.VARCHAR, AttributeType.INT, AttributeType.VARCHAR};

  public static String[] UpdatedEmployeeTableNonPKAttributeNames = new String[]{Name, Email, Age, Address, Salary};
  public static String[] EmployeeTablePKAttributes = new String[]{"SSN"};


  public static int initialNumberOfRecords = 100;
  public static int updatedNumberOfRecords = initialNumberOfRecords / 2;

  private TableManager tableManager;
  private Records records;
  private Indexes indexes;

  private String getName(long i) {
    return "Name" + i;
  }

  private String getEmail(long i) {
    return "ABCDEFGH" + i + "@usc.edu";
  }

  private long getAge(long i) {
    return (i+25)%90;
  }

  private String getAddress(long i) {
    return "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + i;
  }

  private long getSalary(long i) {
    return i + 100;
  }

  @Before
  public void init(){
    tableManager = new TableManagerImpl();
    records = new RecordsImpl();
    indexes = new IndexesImpl();
  }
}