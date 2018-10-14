import Project2.DBCatalog;
import Project2.btree.BTree;
import Project2.btree.Index;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;

import static Project2.DBCatalog.*;
import static junit.framework.TestCase.assertTrue;

/**
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class BPlusTest {

   @Test
   public void testGivenIndices() throws Exception {
      initialize("samples2/input", "test/testOutput");
      HashMap<String, Index> indices = getIndices();

      for (String x : indices.keySet()) {
         Index i = indices.get(x);
         String fileName = DBCatalog.getInput() + File.separator + "db" + File.separator + "data" + File.separator + i.getTable();
         ArrayList<String> columns = getColumns(i.getTable());
         new BTree(fileName, columns, i.getFileName(), columns.indexOf(i.getColumn()), i.getOrder(), i.isClustered());

         InputStream in1 = new BufferedInputStream(new FileInputStream("samples2/input/db/indexes/" + x));
         InputStream in2 = new BufferedInputStream(new FileInputStream("samples2/expected_indexes/" + x));

         int value1, value2, count = 0;

         do{
            value1 = in1.read();
            value2 = in2.read();
            assertTrue(value1 == value2);
         } while(value1 >= 0);
      }
   }
}