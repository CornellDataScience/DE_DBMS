/**
 * Tests the SQLQueryPlan class
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class PhysicalPlanBuilderTest {

   /**
    * Inner class to allow for the mocking of a database catalog
    */
   /*private class MockedCatalog extends DBCatalog {

      *//**
       * Creates catalog without needed to read a file with passed in database hashmap
       *
       * @param db hashmap specifying table name, file path, and columns
       *//*
      public MockedCatalog(HashMap<String, AbstractMap.SimpleImmutableEntry<String,
            ArrayList<String>>> db) {
         databases = db;
         indices = null;

         tmp = "test/testOutput";

         joinType = 0;
         sortType = 0;
         useIndices = false;
      }

      public void setJoinType(int jT) {
         joinType = jT;
      }

      public void setSortType(int sT) {
         sortType = sT;
      }

      public void setUseIndices(boolean indices) {
         useIndices = indices;
      }
   }

   private MockedCatalog mockedCatalog;

   @Before
   public void setUp() {
      // creates database catalog
      HashMap<String, AbstractMap.SimpleImmutableEntry<String, ArrayList<String>>> db = new 
            HashMap<>();
      
      ArrayList<String> columns = new ArrayList<>();
      columns.add("A");
      columns.add("B");
      columns.add("C");
      db.put("TableA", new AbstractMap.SimpleImmutableEntry<>("TableA", columns));
      columns = new ArrayList<>();
      columns.add("D");
      columns.add("E");
      columns.add("F");
      db.put("TableB", new AbstractMap.SimpleImmutableEntry<>("TableB", columns));
      columns = new ArrayList<>();
      columns.add("G");
      columns.add("H");
      columns.add("I");
      db.put("TableC", new AbstractMap.SimpleImmutableEntry<>("TableC", columns));

      mockedCatalog = new MockedCatalog(db);
   }

   // tests whether given a query, the correct tree plan is outputted

   @Test
   public void testSelectDetermination() throws ParseException, IOException {
      String SQLFile = "SELECT * FROM TableA;" + "SELECT TableA.A FROM TableA;" + "SELECT * FROM TableA WHERE TableA.A > 0;" + "SELECT TableA.A FROM TableA WHERE TableA.A > 0;";

      CCJSqlParser parser = new CCJSqlParser(new StringReader(SQLFile));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "temp", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "temp", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "temp", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "temp", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalSelectOperator"));

   }

   @Test
   public void testTNLJoinDetermination() throws ParseException, IOException {
      String SQLFile = "SELECT * FROM TableA, TableB;" + "SELECT * FROM TableA, TableB WHERE TableA.A = TableB.D;" + "SELECT TableA.A FROM TableA, TableB;" + "SELECT TableA.A FROM TableA, TableB WHERE TableA.A = TableB.D;";

      CCJSqlParser parser = new CCJSqlParser(new StringReader(SQLFile));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      SQLFile = "SELECT * FROM TableA, TableB WHERE TableA.A > 0;" +
            "SELECT * FROM TableA, TableB WHERE TableB.D > 0;" +
            "SELECT * FROM TableA, TableB WHERE TableA.A > 0 AND TableB.D > 0;" +
            "SELECT * FROM TableA, TableB WHERE TableA.A > 0 AND TableB.D > 0 AND TableA.A = TableB.D;" +
            "SELECT * FROM TableA, TableB, TableC WHERE TableA.A > 0 AND TableB.D > 0 AND TableC.G > 0;" +
            "SELECT * FROM TableA, TableB, TableC WHERE TableA.A > 0 AND TableB.D > 0 AND TableC.G > 0 AND TableA.A = TableB.D AND TableA.D = TableB.G;";
      parser = new CCJSqlParser(new StringReader(SQLFile));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalTNLJOperator: left - PhysicalSelectOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalTNLJOperator: left - PhysicalSelectOperator and right - PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalTNLJOperator: left - PhysicalSelectOperator and right - PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalTNLJOperator: left - PhysicalTNLJOperator: left - PhysicalSelectOperator and right - PhysicalSelectOperator and right - PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalTNLJOperator: left - PhysicalTNLJOperator: left - PhysicalSelectOperator and right - PhysicalSelectOperator and right - PhysicalSelectOperator"));
   }

   @Test
   public void testBNLJoinDetermination() throws ParseException, IOException {
      String SQLFile = "SELECT * FROM TableA, TableB;" + "SELECT * FROM TableA, TableB WHERE TableA.A = TableB.D;" + "SELECT TableA.A FROM TableA, TableB;" + "SELECT TableA.A FROM TableA, TableB WHERE TableA.A = TableB.D;";

      CCJSqlParser parser = new CCJSqlParser(new StringReader(SQLFile));
      mockedCatalog.setJoinType(1);

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalBNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalBNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalBNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalBNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      SQLFile = "SELECT * FROM TableA, TableB WHERE TableA.A > 0;" +
            "SELECT * FROM TableA, TableB WHERE TableB.D > 0;" +
            "SELECT * FROM TableA, TableB WHERE TableA.A > 0 AND TableB.D > 0;" +
            "SELECT * FROM TableA, TableB WHERE TableA.A > 0 AND TableB.D > 0 AND TableA.A = TableB.D;" +
            "SELECT * FROM TableA, TableB, TableC WHERE TableA.A > 0 AND TableB.D > 0 AND TableC.G > 0;" +
            "SELECT * FROM TableA, TableB, TableC WHERE TableA.A > 0 AND TableB.D > 0 AND TableC.G > 0 AND TableA.A = TableB.D AND TableA.D = TableB.G;";
      parser = new CCJSqlParser(new StringReader(SQLFile));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalBNLJOperator: left - PhysicalSelectOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalBNLJOperator: left - PhysicalScanOperator and right - PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalBNLJOperator: left - PhysicalSelectOperator and right - PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalBNLJOperator: left - PhysicalSelectOperator and right - PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalBNLJOperator: left - PhysicalBNLJOperator: left - PhysicalSelectOperator and right - PhysicalSelectOperator and right - PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalBNLJOperator: left - PhysicalBNLJOperator: left - PhysicalSelectOperator and right - PhysicalSelectOperator and right - PhysicalSelectOperator"));
   }

   @Test
   public void testSMJoinDetermination() throws ParseException, IOException {
      String SQLFile = "SELECT * FROM TableA, TableB WHERE TableA.A = TableB.D;" +
            "SELECT TableA.A FROM TableA, TableB WHERE TableA.A = TableB.D;" +
            "SELECT * FROM TableA, TableB WHERE TableA.A > 0 AND TableB.D > 0 AND TableA.A = TableB.D;" +
            "SELECT * FROM TableA, TableB, TableC WHERE TableA.A > 0 AND TableB.D > 0 AND TableC.G > 0 AND TableA.A = TableB.D AND TableB.D = TableC.G;" +
            "SELECT * FROM TableA, TableB WHERE TableA.B > TableB.D AND TableA.B = TableB.F AND TableB.D < TableA.C;";

      CCJSqlParser parser = new CCJSqlParser(new StringReader(SQLFile));
      mockedCatalog.setJoinType(2);

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalSMJOperator: left - PhysicalInPlaceSortOperator: PhysicalScanOperator and right - PhysicalInPlaceSortOperator: PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalSMJOperator: left - PhysicalInPlaceSortOperator: PhysicalScanOperator and right - PhysicalInPlaceSortOperator: PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalSMJOperator: left - PhysicalInPlaceSortOperator: PhysicalSelectOperator and right - PhysicalInPlaceSortOperator: PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalSMJOperator: left - PhysicalInPlaceSortOperator: PhysicalSMJOperator: left - PhysicalInPlaceSortOperator: PhysicalSelectOperator and right - PhysicalInPlaceSortOperator: PhysicalSelectOperator and right - PhysicalInPlaceSortOperator: PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalSMJOperator: left - PhysicalInPlaceSortOperator: PhysicalScanOperator and right - PhysicalInPlaceSortOperator: PhysicalScanOperator"));
   }

   @Test
   public void testOrderByDetermination() throws ParseException, IOException {
      String SQLFile = "SELECT * FROM TableA ORDER BY TableA.A;" +
            "SELECT TableA.A FROM TableA ORDER BY TableA.A;" +
            "SELECT * FROM TableA WHERE TableA.A > 0 ORDER BY TableA.A;" +
            "SELECT TableA.A FROM TableA WHERE TableA.A > 0 ORDER BY TableA.A;" +
            "SELECT * FROM TableA, TableB ORDER BY TableA.A;" +
            "SELECT * FROM TableA, TableB WHERE TableA.A = TableB.D ORDER BY TableA.A;" +
            "SELECT TableA.A FROM TableA, TableB ORDER BY TableA.A;" +
            "SELECT TableA.A FROM TableA, TableB WHERE TableA.A = TableB.D ORDER BY TableA.A;";

      CCJSqlParser parser = new CCJSqlParser(new StringReader(SQLFile));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalInPlaceSortOperator: PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalInPlaceSortOperator: PhysicalProjectOperator: PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalInPlaceSortOperator: PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalInPlaceSortOperator: PhysicalProjectOperator: PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalInPlaceSortOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalInPlaceSortOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalInPlaceSortOperator: PhysicalProjectOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalProjectOperator: PhysicalInPlaceSortOperator: PhysicalProjectOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));
   }

   @Test
   public void testDistinctInPlaceDetermination() throws ParseException, IOException {
      String SQLFile = "SELECT DISTINCT TableA.A FROM TableA;" +
            "SELECT DISTINCT TableA.A FROM TableA WHERE TableA.A > 0;" +
            "SELECT DISTINCT TableA.A FROM TableA, TableB;" +
            "SELECT DISTINCT TableA.A FROM TableA, TableB WHERE TableA.A = TableB.D;" +
            "SELECT DISTINCT TableA.A FROM TableA ORDER BY TableA.A;" +
            "SELECT DISTINCT TableA.A FROM TableA WHERE TableA.A > 0 ORDER BY TableA.A;" +
            "SELECT DISTINCT TableA.A FROM TableA, TableB ORDER BY TableA.A;" +
            "SELECT DISTINCT TableA.A FROM TableA, TableB WHERE TableA.A = TableB.D ORDER BY TableA.A;" +
            "SELECT DISTINCT * FROM TableA;";

      CCJSqlParser parser = new CCJSqlParser(new StringReader(SQLFile));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalInPlaceSortOperator: PhysicalProjectOperator: PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalInPlaceSortOperator: PhysicalProjectOperator: PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalInPlaceSortOperator: PhysicalProjectOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalInPlaceSortOperator: PhysicalProjectOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalProjectOperator: PhysicalInPlaceSortOperator: PhysicalProjectOperator: PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalProjectOperator: PhysicalInPlaceSortOperator: PhysicalProjectOperator: PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalProjectOperator: PhysicalInPlaceSortOperator: PhysicalProjectOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalProjectOperator: PhysicalInPlaceSortOperator: PhysicalProjectOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalInPlaceSortOperator: PhysicalScanOperator"));
   }

   @Test
   public void testDistinctExternalDetermination() throws ParseException, IOException {
      String SQLFile = "SELECT DISTINCT TableA.A FROM TableA;" +
            "SELECT DISTINCT TableA.A FROM TableA WHERE TableA.A > 0;" +
            "SELECT DISTINCT TableA.A FROM TableA, TableB;" +
            "SELECT DISTINCT TableA.A FROM TableA, TableB WHERE TableA.A = TableB.D;" +
            "SELECT DISTINCT TableA.A FROM TableA ORDER BY TableA.A;" +
            "SELECT DISTINCT TableA.A FROM TableA WHERE TableA.A > 0 ORDER BY TableA.A;" +
            "SELECT DISTINCT TableA.A FROM TableA, TableB ORDER BY TableA.A;" +
            "SELECT DISTINCT TableA.A FROM TableA, TableB WHERE TableA.A = TableB.D ORDER BY TableA.A;" +
            "SELECT DISTINCT * FROM TableA;";

      CCJSqlParser parser = new CCJSqlParser(new StringReader(SQLFile));
      mockedCatalog.setSortType(1);

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalExternalSortOperator: PhysicalProjectOperator: PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalExternalSortOperator: PhysicalProjectOperator: PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalExternalSortOperator: PhysicalProjectOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalExternalSortOperator: PhysicalProjectOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalProjectOperator: PhysicalExternalSortOperator: PhysicalProjectOperator: PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalProjectOperator: PhysicalExternalSortOperator: PhysicalProjectOperator: PhysicalSelectOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalProjectOperator: PhysicalExternalSortOperator: PhysicalProjectOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalProjectOperator: PhysicalExternalSortOperator: PhysicalProjectOperator: PhysicalTNLJOperator: left - PhysicalScanOperator and right - PhysicalScanOperator"));

      statement = parser.Statement();
      queryPlan = new SQLQueryPlan(statement, "test", 1);
      assertTrue(queryPlan.getHead().toString().equals("PhysicalDuplicateEliminationOperator: PhysicalExternalSortOperator: PhysicalScanOperator"));
   }*/
}