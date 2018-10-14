import Project2.Tuple.TupleBinaryReader;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertTrue;

/**
 * Tests the Operators
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class BinaryReaderTest {

   @Test
   public void testReset() throws Exception {
      TupleBinaryReader t = new TupleBinaryReader("samples/input/db/data/Boats");

      // from second page
      for (int i = 0; i < 350; i++){
         t.read();
      }

      t.reset(0);
      ArrayList<Integer> read = t.read();
      assertTrue(read.get(0) == 12);
      assertTrue(read.get(1) == 143);
      assertTrue(read.get(2) == 196);

      t.reset(339);
      read = t.read();
      assertTrue(read.get(0) == 26);
      assertTrue(read.get(1) == 186);
      assertTrue(read.get(2) == 63);

      t.reset(599);
      read = t.read();
      assertTrue(read.get(0) == 61);
      assertTrue(read.get(1) == 143);
      assertTrue(read.get(2) == 160);

      t.reset(999);
      read = t.read();
      assertTrue(read.get(0) == 44);
      assertTrue(read.get(1) == 39);
      assertTrue(read.get(2) == 136);

      t.reset(1000);
      read = t.read();
      assertTrue(read == null);

      // from last page
      t.reset(0);
      for (int i = 0; i < 1000; i++){
         t.read();
      }

      t.reset(0);
      read = t.read();
      assertTrue(read.get(0) == 12);
      assertTrue(read.get(1) == 143);
      assertTrue(read.get(2) == 196);

      t.reset(339);
      read = t.read();
      assertTrue(read.get(0) == 26);
      assertTrue(read.get(1) == 186);
      assertTrue(read.get(2) == 63);

      t.reset(599);
      read = t.read();
      assertTrue(read.get(0) == 61);
      assertTrue(read.get(1) == 143);
      assertTrue(read.get(2) == 160);

      t.reset(999);
      read = t.read();
      assertTrue(read.get(0) == 44);
      assertTrue(read.get(1) == 39);
      assertTrue(read.get(2) == 136);

      t.reset(1000);
      read = t.read();
      assertTrue(read == null);

      // all from last page
      t.reset(0);
      for (int i = 0; i < 1000; i++){
         t.read();
      }

      t.reset(0);
      read = t.read();
      assertTrue(read.get(0) == 12);
      assertTrue(read.get(1) == 143);
      assertTrue(read.get(2) == 196);

      t.reset(0);
      for (int i = 0; i < 1000; i++){
         t.read();
      }

      t.reset(339);
      read = t.read();
      assertTrue(read.get(0) == 26);
      assertTrue(read.get(1) == 186);
      assertTrue(read.get(2) == 63);

      t.reset(0);
      for (int i = 0; i < 1000; i++){
         t.read();
      }

      t.reset(599);
      read = t.read();
      assertTrue(read.get(0) == 61);
      assertTrue(read.get(1) == 143);
      assertTrue(read.get(2) == 160);

      t.reset(0);
      for (int i = 0; i < 1000; i++){
         t.read();
      }

      t.reset(999);
      read = t.read();
      assertTrue(read.get(0) == 44);
      assertTrue(read.get(1) == 39);
      assertTrue(read.get(2) == 136);

      t.reset(0);
      for (int i = 0; i < 1000; i++){
         t.read();
      }

      t.reset(1000);
      read = t.read();
      assertTrue(read == null);

      t.close();
   }
}