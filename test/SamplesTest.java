import Project2.DBCatalog;
import Project2.SQLQueryPlan;
import Project2.Tuple.TupleBinaryReader;
import Project2.Tuple.TupleWriter;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;

import static Project2.DBCatalog.*;
import static org.junit.Assert.assertTrue;

/**
 * Tests the operators on the given sample queries
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class SamplesTest {

   private String expected = "samples3/expected/";
   private String input = "samples3/input";
   private String temp = "test/testOutput";

   /**
    * Inner class to allow for the mocking of DBCatalog
    */
   private class MockedCatalog extends DBCatalog {

      /**
       * Creates catalog without needed to read a file with passed in database hashmap. Used for
       * sorting expected output
       */
      public MockedCatalog(String fileName, ArrayList<String> cols) throws IOException {
         databases = new HashMap<>();
         databases.put("A", new AbstractMap.SimpleImmutableEntry<>(expected + fileName, cols));

         tmp = temp;
      }
   }

   /**
    * Inner class to allow for the mocking of TupleWriter
    */
   private class MockedTupleWriter extends TupleWriter {

      private ArrayList<ByteBuffer> buffers;
      private PrintWriter writer;

      /**
       * Creates a TupleWriter that writes to an arraylist rather than a file
       */
      public MockedTupleWriter(String name) throws Exception {
         buffer = ByteBuffer.allocate(pageSize);
         buffer.position(8);
         tupleCounter = 0;
         buffers = new ArrayList<>();
         writer = new PrintWriter(temp + File.separator + name + ".txt", "UTF-8");
      }

      /**
       * Creates a TupleWriter that writes to an arraylist rather than a file
       */
      public MockedTupleWriter() throws Exception {
         buffer = ByteBuffer.allocate(pageSize);
         buffer.position(8);
         tupleCounter = 0;
         buffers = new ArrayList<>();
         writer = null;
      }

      @Override
      public void write(ArrayList<Integer> tuple) throws IOException {
         super.write(tuple);
         if (writer != null) {
            writer.println(tuple);
         }
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

      @Override
      public void end(int colSize) throws IOException {
         super.end(colSize);
         if (writer != null) {
            writer.close();
         }
      }
   }
   
   private void tempFileDeletion() {
      File file = new File (getTmp());
      for (File child : file.listFiles()) {
         //if (!child.getName().equals("testFile") && !(child.isFile() && !child.getName().contains("output") && Integer.parseInt(child.getName().split("_")[0].replaceFirst("query", "")) < 16)) {
         if (!child.getName().equals("testFile")) {
            deleteDir(child);
         }
      }
   }


   @Test
   public void Queries1_12() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM Sailors ORDER BY Sailors.A, Sailors.B, Sailors.C;" +
            "SELECT Sailors.A FROM Sailors ORDER BY Sailors.A;" +
            "SELECT Boats.F, Boats.D FROM Boats ORDER BY Boats.F, Boats.D;" +
            "SELECT Reserves.G, Reserves.H FROM Reserves ORDER BY Reserves.G, Reserves.H;" +
            "SELECT * FROM Sailors WHERE Sailors.B >= Sailors.C ORDER BY Sailors.A, Sailors.B, Sailors.C;" +
            "SELECT Sailors.A FROM Sailors WHERE Sailors.B >= Sailors.C ORDER BY Sailors.A;" +
            "SELECT Sailors.A FROM Sailors WHERE Sailors.B >= Sailors.C AND Sailors.B < Sailors.C ORDER BY Sailors.A;" +
            "SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G ORDER BY Sailors.A, Sailors.B, Sailors.C, Reserves.G, Reserves.H;" +
            "SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D ORDER BY Sailors.A, Sailors.B, Sailors.C, Reserves.G, Reserves.H, Boats.D, Boats.E, Boats.F;" +
            "SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D AND Sailors.B < 150 ORDER BY Sailors.A, Sailors.B, Sailors.C, Reserves.G, Reserves.H, Boats.D, Boats.E, Boats.F;" +
            "SELECT DISTINCT * FROM Sailors ORDER BY Sailors.A, Sailors.B, Sailors.C;" +
            "SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A ORDER BY S1.A, S1.B, S1.C, S2.A, S2.B, S2.C;"));
      CCJSqlParser parser2 = new CCJSqlParser(new StringReader("SELECT * FROM A ORDER BY A.A, A.B, A.C;" +
            "SELECT * FROM A ORDER BY A.A;" +
            "SELECT * FROM A ORDER BY A.A, A.B;" +
            "SELECT * FROM A ORDER BY A.A, A.B;" +
            "SELECT * FROM A ORDER BY A.A, A.B, A.C;" +
            "SELECT * FROM A ORDER BY A.A;" +
            "SELECT * FROM A ORDER BY A.A;" +
            "SELECT * FROM A ORDER BY A.A, A.B, A.C, A.D, A.E;" +
            "SELECT * FROM A ORDER BY A.A, A.B, A.C, A.D, A.E, A.F, A.G, A.H;" +
            "SELECT * FROM A ORDER BY A.A, A.B, A.C, A.D, A.E, A.F, A.G, A.H;" +
            "SELECT * FROM A ORDER BY A.A, A.B, A.C;" +
            "SELECT * FROM A ORDER BY A.A, A.B, A.C, A.D, A.E, A.F;"));

      int[] colSize = {3, 1, 2, 2, 3, 1, 1, 5, 8, 8, 3, 6};
      int[] tupleNum = {1000, 1000, 1000, 1000, 481, 481, 0, 5019, 25224, 19225, 1000, 496964};

      ArrayList<String> cols = new ArrayList<>(8);
      cols.add("A");
      cols.add("B");
      cols.add("C");
      cols.add("D");
      cols.add("E");
      cols.add("F");
      cols.add("G");
      cols.add("H");

      for (int x = 0; x < 12; x++) {
         initialize(input, temp);
         Statement statement = parser.Statement();

         SQLQueryPlan tree = new  SQLQueryPlan(statement, temp, x + 1);
         MockedTupleWriter tupleWriter = new MockedTupleWriter();
         tree.getHead().dump(tupleWriter);
         ArrayList<ByteBuffer> buffers = tupleWriter.getBuffers();
         int buffCount = 0;

         DBCatalog catalog2 = new MockedCatalog("query" + (x+1), new ArrayList<>(cols.subList(0, colSize[x])));
         Statement statement2 = parser2.Statement();

         SQLQueryPlan tree2 = new SQLQueryPlan(statement2, temp, 16 + x);
         MockedTupleWriter tupleWriter2 = new MockedTupleWriter();
         tree2.getHead().dump(tupleWriter2);
         ArrayList<ByteBuffer> buffers2 = tupleWriter2.getBuffers();

         if (buffers.isEmpty()) {
            assertTrue(buffers.size() == buffers2.size());
         } else {
            assertTrue(buffers.get(buffCount).getInt() == buffers2.get(buffCount).getInt());
            int tupleLimit = Math.min(getPageSize(colSize[x]), tupleNum[x]);
            assertTrue(buffers.get(buffCount).getInt() == tupleLimit);
            assertTrue(buffers2.get(buffCount).getInt() == tupleLimit);

            while (buffCount <= tupleNum[x] / 1022) {
               if (buffers.get(buffCount).position() == tupleLimit * colSize[x] * 4 + 8) {
                  buffCount++;
                  tupleLimit = Math.min(tupleNum[x] - getPageSize(colSize[x]) * buffCount, getPageSize(colSize[x]));

                  if (tupleLimit <= 0) {
                     break;
                  }

                  assertTrue(buffers.get(buffCount).getInt() == colSize[x]);
                  assertTrue(buffers.get(buffCount).getInt() == tupleLimit);
                  assertTrue(buffers2.get(buffCount).getInt() == colSize[x]);
                  assertTrue(buffers2.get(buffCount).getInt() == tupleLimit);
               }
               int buffer1INT = buffers.get(buffCount).getInt();
               int buffer2INT = buffers2.get(buffCount).getInt();
               assertTrue(buffer1INT == buffer2INT);
            }
         }

         tempFileDeletion();
      }
   }

   @Test
   public void Queries13_15() throws Exception {
      CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT B.F, B.D FROM Boats B ORDER BY B.D; " +
            "SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C; " +
            "SELECT DISTINCT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;"));
      int[] colSize = {2, 8, 8};

      int[] tupleNum = {1000, 25224, 24764};

      for (int x = 0; x < 3; x++) {
         initialize(input, temp);
         Statement statement = parser.Statement();
         SQLQueryPlan tree = new SQLQueryPlan(statement, temp, x + 13);
         MockedTupleWriter tupleWriter = new MockedTupleWriter("output" + (x + 13));
         tree.getHead().dump(tupleWriter);

         ArrayList<ByteBuffer> buffers = tupleWriter.getBuffers();
         int buffCount = 0;

         TupleBinaryReader tupleReader = new TupleBinaryReader(expected + "query" + (x + 13));
         if (tupleReader.read() == null) {
            assertTrue(buffers.size() == 0);
            continue;
         }
         tupleReader.reset(0);

         ArrayList<Integer> tuple;

         assertTrue(buffers.get(buffCount).getInt() == colSize[x]);
         int tupleLimit = Math.min(getPageSize(colSize[x]), tupleNum[x]);
         assertTrue(buffers.get(buffCount).getInt() == tupleLimit);
         while ((tuple = tupleReader.read()) != null) {
            for (int y : tuple) {
                assertTrue(buffers.get(buffCount).getInt() == y);
            }

            if (buffers.get(buffCount).position() == tupleLimit * colSize[x] * 4 + 8) {
               buffCount++;
               tupleLimit = Math.min(tupleNum[x] - getPageSize(colSize[x]) * buffCount, getPageSize(colSize[x]));

               if (tupleLimit <= 0) {
                  break;
               }

               assertTrue(buffers.get(buffCount).getInt() == colSize[x]);
               assertTrue(buffers.get(buffCount).getInt() == tupleLimit);
            }
         }

         tupleReader.close();
         tempFileDeletion();
      }
   }
}