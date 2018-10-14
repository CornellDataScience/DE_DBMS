import Project2.DBCatalog;
import Project2.Operators.Physical.PhysicalOperator;
import Project2.SQLQueryPlan;
import Project2.Tuple.TupleWriter;
import Project2.btree.BTree;
import Project2.btree.Index;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;

import static Project2.DBCatalog.deleteDir;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the Operators
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class OperatorsTest {
   /**
    * Inner class to allow for the mocking of DBCatalog
    */
   private class MockedCatalog extends DBCatalog {
      /**
       * Creates catalog without needed to read a file with passed in database hashmap
       *
       * @param db hashmap specifying table name, file path, and columns
       */
      public MockedCatalog(HashMap<String, AbstractMap.SimpleImmutableEntry<String,
            ArrayList<String>>> db) throws IOException {
         databases = db;
         indices = new HashMap<>();
         indices.put("TableG.A", new Index(0, 3, "TableG", "A", "test/testOutput/TableG.A"));

         tmp = "test/testOutput";

         bufferPages = 10;
      }
   }

   /**
    * Inner class to allow for the mocking of TupleWriter
    */
   private class MockedTupleWriter extends TupleWriter {

      private ArrayList<ByteBuffer> buffers;

      /**
       * Creates a TupleWriter that writes to an arraylist rather than a file
       */
      public MockedTupleWriter() {
         buffer = ByteBuffer.allocate(pageSize);
         buffer.position(8);
         tupleCounter = 0;
         buffers = new ArrayList<>();
      }

      @Override
      public void endPage(int colNum) throws IOException {
         byte[] bytes = new byte[pageSize - buffer.position()];
         buffer.put(bytes);

         buffer.putInt(0, colNum);
         buffer.putInt(4, tupleCounter);
         buffer.flip();
         buffers.add(buffer);
         buffer = ByteBuffer.allocate(pageSize);
      }

      /**
       * @return the arraylist of bytebuffers
       */
      public ArrayList<ByteBuffer> getBuffers() {
         return buffers;
      }
   }

   private MockedCatalog mockedCatalog;
   
   @Before
   public void setUp() throws IOException {
      // creates database catalog
      HashMap<String, AbstractMap.SimpleImmutableEntry<String, ArrayList<String>>> db = new
            HashMap<>();

      ArrayList<String> columns = new ArrayList<>();
      columns.add("A");
      columns.add("B");
      columns.add("C");
      db.put("TableA", new AbstractMap.SimpleImmutableEntry<>("test/testSQLFiles/TableA", columns));
      columns = new ArrayList<>();
      columns.add("D");
      columns.add("E");
      columns.add("F");
      db.put("TableB", new AbstractMap.SimpleImmutableEntry<>("test/testSQLFiles/TableB", columns));
      columns = new ArrayList<>();
      columns.add("G");
      columns.add("H");
      columns.add("I");
      db.put("TableC", new AbstractMap.SimpleImmutableEntry<>("test/testSQLFiles/TableC", columns));
      columns = new ArrayList<>();
      columns.add("J");
      columns.add("K");
      columns.add("L");
      db.put("TableD", new AbstractMap.SimpleImmutableEntry<>("test/testSQLFiles/TableD", columns));
      columns = new ArrayList<>();
      columns.add("M");
      columns.add("N");
      columns.add("O");
      db.put("TableE", new AbstractMap.SimpleImmutableEntry<>("test/testSQLFiles/TableE", columns));
      columns = new ArrayList<>();
      columns.add("A");
      columns.add("B");
      columns.add("C");
      db.put("TableF", new AbstractMap.SimpleImmutableEntry<>("test/testSQLFiles/TableF", columns));
      columns = new ArrayList<>();
      columns.add("A");
      columns.add("B");
      columns.add("C");
      db.put("TableG", new AbstractMap.SimpleImmutableEntry<>("test/testSQLFiles/TableG", columns));

      mockedCatalog = new MockedCatalog(db);
   }

   @After
   public void tempFileDeletion() {
      File file = new File (DBCatalog.getTmp());
      for (File child : file.listFiles()) {
         if (!child.getName().equals("testFile")) {
            deleteDir(child);
         }
      }
   }

   // scan
/*   @Test
   public void testScan() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator scan = queryPlan.getHead();

      assertTrue(scan.getNextTuple().toString().equals("1,2,3"));

      scan.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      scan.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 3); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testScanEmpty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableD;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator scan = queryPlan.getHead();

      assertNull(scan.getNextTuple());

      scan.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      scan.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }


   // select
   @Test
   public void testSelect() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA WHERE TableA.A > 1;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator select = queryPlan.getHead();

      assertTrue(select.getNextTuple().toString().equals("4,5,6"));

      select.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      select.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 3); // num cols
      assertTrue(buff.getInt() == 1); // num tuples
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testSelectEmpty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableD T1 WHERE T1.J > 1;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator select = queryPlan.getHead();

      assertNull(select.getNextTuple());

      select.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      select.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }


   // project
   @Test
   public void testProjectAll() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT T1.A, T1.B, T1.C FROM TableA T1;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator project = queryPlan.getHead();

      assertTrue(project.getNextTuple().toString().equals("1,2,3"));

      project.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      project.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 3); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testProjectAllDifferentOrdering() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT T1.B, T1.C, T1.A FROM TableA T1;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator project = queryPlan.getHead();

      assertTrue(project.getNextTuple().toString().equals("2,3,1"));

      project.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      project.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 3); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 4);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testProjectOne() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT T1.B FROM TableA T1;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator project = queryPlan.getHead();

      assertTrue(project.getNextTuple().toString().equals("2"));

      project.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      project.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 1); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 5);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testProjectEmpty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT J FROM TableD;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator project = queryPlan.getHead();

      assertNull(project.getNextTuple());

      project.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      project.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }

   @Test
   public void testProjectColumnsInSelfJoin() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT T1.A, T2.B FROM TableA T1, TableA T2 WHERE T1.A = T2.A;"));
      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);

      PhysicalOperator project = queryPlan.getHead();

      assertTrue(project.getNextTuple().toString().equals("1,2"));

      project.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      project.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 2); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testProjectColumnsInSelfJoinEmpty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT T1.J, T2.K FROM TableD T1, TableD T2 WHERE T1.J = T2.J;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator project = queryPlan.getHead();

      assertNull(project.getNextTuple());

      project.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      project.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }


   // TNL join
   @Test
   public void testTNLJoin1() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA, TableB WHERE TableB.D = TableA.A;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,1,2,3"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testTNLJoin2() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA, TableB;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,4,5,6"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 4); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testTNLJoin3() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA A, TableF F WHERE A.A = F.A;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,1,2,3"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 3); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 8);
      assertTrue(buff.getInt() == 9);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testTNLJoin4() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableC C, TableE E, TableB B WHERE E.M = C.G AND B.D = C.G;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,1,3,1,1,2,3"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 9); // num cols
      assertTrue(buff.getInt() == 6); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 0);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 0);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testTNLJoin1Empty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA, TableD WHERE TableA.A = TableD.J; SELECT * FROM TableD, TableA WHERE TableA.A = TableD.J;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertNull(join.getNextTuple());

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);

      queryPlan = new SQLQueryPlan(parser.Statement(), "temp", 1);
      join = queryPlan.getHead();

      assertNull(join.getNextTuple());

      join.reset();
      writer = new MockedTupleWriter();
      join.dump(writer);
      buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }

   @Test
   public void testTNLJoin2Empty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA, TableD; SELECT * FROM TableD, TableA;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertNull(join.getNextTuple());

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);

      queryPlan = new SQLQueryPlan(parser.Statement(), "temp", 1);
      join = queryPlan.getHead();

      assertNull(join.getNextTuple());

      join.reset();
      writer = new MockedTupleWriter();
      join.dump(writer);
      buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }

   @Test
   public void testSelfTNLJoin() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA T1, TableA T2 WHERE T1.A = T2.A;"));
      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);

      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,1,2,3"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testSelfTNLJoinEmpty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableD T1, TableD T2 WHERE T1.J = T2.J;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertNull(join.getNextTuple());

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }


   // BNL join
   @Test
   public void testBNLJoin1() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA, TableB WHERE TableB.D = TableA.A;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("4,5,6,4,5,6"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testBNLJoin2() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA, TableB;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,4,5,6"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 4); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testBNLJoin3() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA A, TableF F WHERE A.A = F.A;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,1,2,3"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 3); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 8);
      assertTrue(buff.getInt() == 9);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testBNLJoin4() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableC C, TableE E, TableB B WHERE E.M = C.G AND B.D = C.G;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,1,3,1,1,2,3"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 9); // num cols
      assertTrue(buff.getInt() == 6); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 0);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 0);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testBNLJoin1Empty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA, TableD WHERE TableA.A = TableD.J; SELECT * FROM TableD, TableA WHERE TableA.A = TableD.J;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertNull(join.getNextTuple());

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);

      join = new SQLQueryPlan(parser.Statement(), "temp", 1).getHead();

      assertNull(join.getNextTuple());

      join.reset();
      writer = new MockedTupleWriter();
      join.dump(writer);
      buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }

   @Test
   public void testBNLJoin2Empty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA, TableD; SELECT * FROM TableD, TableA;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertNull(join.getNextTuple());

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);

      join = new SQLQueryPlan(parser.Statement(), "temp", 1).getHead();

      assertNull(join.getNextTuple());

      join.reset();
      writer = new MockedTupleWriter();
      join.dump(writer);
      buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }

   @Test
   public void testSelfBNLJoin() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA T1, TableA T2 WHERE T1.A = T2.A;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,1,2,3"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testSelfBNLJoinEmpty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableD T1, TableD T2 WHERE T1.J = T2.J;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertNull(join.getNextTuple());

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }


   // SM join
   @Test
   public void testSMJoin1() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA, TableB WHERE TableB.D = TableA.A;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();
      assertTrue(join.getNextTuple().toString().equals("1,2,3,1,2,3"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testSMJoin2() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA A, TableF F WHERE A.A = F.A;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,1,2,3"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 3); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 8);
      assertTrue(buff.getInt() == 9);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testSMJoin3() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableB B, TableE E WHERE B.F = E.N;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,1,3,1"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 2);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testSMJoin4() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableC C, TableE E, TableB B WHERE E.M = C.G AND B.D = C.G;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,1,2,0,1,2,3"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 9); // num cols
      assertTrue(buff.getInt() == 6); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 0);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 0);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testSMJoin5() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA A, TableG G WHERE A.B = G.A AND G.C = A.B;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,2,0,2"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 0);
      assertTrue(buff.getInt() == 2);

      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 0);
      assertTrue(buff.getInt() == 5);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testSMJoin1Empty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA, TableD WHERE TableA.A = TableD.J; SELECT * FROM TableD, TableA WHERE TableA.A = TableD.J;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertNull(join.getNextTuple());

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);

      join = new SQLQueryPlan(parser.Statement(), "temp", 1).getHead();

      assertNull(join.getNextTuple());

      join.reset();
      writer = new MockedTupleWriter();
      join.dump(writer);
      buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }

   @Test
   public void testSelfSMJoin() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA T1, TableA T2 WHERE T1.A = T2.A;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,1,2,3"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testSelfSMJoinEmpty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableD T1, TableD T2 WHERE T1.J = T2.J;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertNull(join.getNextTuple());

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }


   // in place sort
   @Test
   public void testIPSort1() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableB ORDER BY TableB.E;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator sort = queryPlan.getHead();

      assertTrue(sort.getNextTuple().toString().equals("2,1,3"));

      sort.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      sort.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 3); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 6);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testIPSort2() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA ORDER BY TableA.A;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator sort = queryPlan.getHead();

      assertTrue(sort.getNextTuple().toString().equals("1,2,3"));

      sort.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      sort.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 3); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testIPSortEmpty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableD ORDER BY TableD.J;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator sort = queryPlan.getHead();

      assertNull(sort.getNextTuple());

      sort.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      sort.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }


   // external sort
   @Test
   public void testESort1() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableB ORDER BY TableB.E;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator sort = queryPlan.getHead();

      assertTrue(sort.getNextTuple().toString().equals("2,1,3"));

      sort.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      sort.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 3); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 6);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testESort2() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA ORDER BY TableA.A;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator sort = queryPlan.getHead();

      assertTrue(sort.getNextTuple().toString().equals("1,2,3"));

      sort.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      sort.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 3); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 4);
      assertTrue(buff.getInt() == 5);
      assertTrue(buff.getInt() == 6);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testESortEmpty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableD ORDER BY TableD.J;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator sort = queryPlan.getHead();

      assertNull(sort.getNextTuple());

      sort.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      sort.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }


   // test duplicate elimination
   @Test
   public void testDuplicateElimination1() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT DISTINCT T.G FROM TableC T;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator distinct = queryPlan.getHead();

      assertTrue(distinct.getNextTuple().toString().equals("1"));

      distinct.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      distinct.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 1); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testDuplicateElimination2() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT DISTINCT T.M FROM TableE T; SELECT DISTINCT T.N FROM TableE T; SELECT DISTINCT T.O FROM TableE T;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator distinct = queryPlan.getHead();

      assertTrue(distinct.getNextTuple().toString().equals("1"));

      distinct.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      distinct.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 1); // num cols
      assertTrue(buff.getInt() == 1); // num tuples
      assertTrue(buff.getInt() == 1);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }

      queryPlan = new SQLQueryPlan(parser.Statement(), "temp", 1);
      distinct = queryPlan.getHead();

      assertTrue(distinct.getNextTuple().toString().equals("2"));

      distinct.reset();
      writer = new MockedTupleWriter();
      distinct.dump(writer);
      buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      buff = buffers.get(0);
      assertTrue(buff.getInt() == 1); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }

      queryPlan = new SQLQueryPlan(parser.Statement(), "temp", 1);
      distinct = queryPlan.getHead();

      assertTrue(distinct.getNextTuple().toString().equals("0"));

      distinct.reset();
      writer = new MockedTupleWriter();
      distinct.dump(writer);
      buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      buff = buffers.get(0);
      assertTrue(buff.getInt() == 1); // num cols
      assertTrue(buff.getInt() == 3); // num tuples
      assertTrue(buff.getInt() == 0);
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }

   @Test
   public void testDuplicateEliminationEmpty() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT DISTINCT T.J FROM TableD T;"));

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator distinct = queryPlan.getHead();

      assertNull(distinct.getNextTuple());

      distinct.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      distinct.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 0);
   }


   @Test
   public void testIndexSelectJoin() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM TableA A, TableG G WHERE A.B = G.A AND G.A <= 2;"));

      Index i = DBCatalog.getIndex("TableG", "A");
      String fileName = "test/testSQLFiles/TableG";
      ArrayList<String> columns = mockedCatalog.getColumns(i.getTable());
      new BTree(fileName, columns, i.getFileName(), columns.indexOf(i.getColumn()), i.getOrder(), i.isClustered());

      Statement statement = parser.Statement();
      SQLQueryPlan queryPlan = new SQLQueryPlan(statement, "test", 1);
      PhysicalOperator join = queryPlan.getHead();

      assertTrue(join.getNextTuple().toString().equals("1,2,3,2,0,2"));

      join.reset();
      MockedTupleWriter writer = new MockedTupleWriter();
      join.dump(writer);
      ArrayList<ByteBuffer> buffers = writer.getBuffers();
      assertTrue(buffers.size() == 1);
      ByteBuffer buff = buffers.get(0);
      assertTrue(buff.getInt() == 6); // num cols
      assertTrue(buff.getInt() == 2); // num tuples
      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 0);
      assertTrue(buff.getInt() == 2);

      assertTrue(buff.getInt() == 1);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 3);
      assertTrue(buff.getInt() == 2);
      assertTrue(buff.getInt() == 0);
      assertTrue(buff.getInt() == 5);

      while (buff.position() != DBCatalog.getPAGE_SIZE()) {
         assertTrue(buff.getInt() == 0);
      }
   }*/
}