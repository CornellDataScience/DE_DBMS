import Project2.Tuple.Tuple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

/**
 * Tests the Tuple class
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class TupleTest {
   @Test
   public void testEntireRow() {
      ArrayList<Integer> array = new ArrayList<>(Arrays.asList(1,2,23,4,5));
      Tuple tuple = new Tuple(array);
      assertTrue(tuple.toString().equals("1,2,23,4,5"));
   }

   @Test
   public void testSomeColumns1() {
      ArrayList<String> cols = new ArrayList<>();
      cols.add("A");
      cols.add("B");
      cols.add("C");
      ArrayList<String> selectedCols = new ArrayList<>();
      selectedCols.add("B");
      selectedCols.add("C");
      ArrayList<Integer> array = new ArrayList<>(Arrays.asList(1,2,3));
      Tuple tuple = new Tuple(new Tuple(array), cols, selectedCols);
      assertTrue(tuple.toString().equals("2,3"));
   }

   @Test
   public void testSomeColumns2() {
      ArrayList<String> cols = new ArrayList<>();
      cols.add("A");
      cols.add("B");
      cols.add("C");
      ArrayList<String> selectedCols = new ArrayList<>();
      selectedCols.add("A");
      selectedCols.add("C");
      ArrayList<Integer> array = new ArrayList<>(Arrays.asList(1,2,3));
      Tuple tuple = new Tuple(new Tuple(array), cols, selectedCols);
      assertTrue(tuple.toString().equals("1,3"));
   }

   @Test
   public void testColumnsDifferentOrdering() {
      ArrayList<String> cols = new ArrayList<>();
      cols.add("A");
      cols.add("B");
      cols.add("C");
      ArrayList<String> selectedCols = new ArrayList<>();
      selectedCols.add("C");
      selectedCols.add("A");
      ArrayList<Integer> array = new ArrayList<>(Arrays.asList(1,2,3));
      Tuple tuple = new Tuple(new Tuple(array), cols, selectedCols);
      assertTrue(tuple.toString().equals("3,1"));
   }

   @Test
   public void testAllColumns() {
      ArrayList<String> cols = new ArrayList<>();
      cols.add("A");
      cols.add("B");
      cols.add("C");
      ArrayList<String> selectedCols = new ArrayList<>();
      selectedCols.add("A");
      selectedCols.add("B");
      selectedCols.add("C");
      ArrayList<Integer> array = new ArrayList<>(Arrays.asList(1,2,3));
      Tuple tuple = new Tuple(new Tuple(array), cols, selectedCols);
      assertTrue(tuple.toString().equals("1,2,3"));
   }

   @Test
   public void testNoColumns() {
      ArrayList<String> cols = new ArrayList<>();
      cols.add("A");
      cols.add("B");
      cols.add("C");
      ArrayList<String> selectedCols = new ArrayList<>();
      ArrayList<Integer> array = new ArrayList<>(Arrays.asList(1,2,3));
      Tuple tuple = new Tuple(new Tuple(array), cols, selectedCols);
      assertTrue(tuple.toString().equals(""));
   }

   @Test
   public void testGetColValue() {
      ArrayList<Integer> array = new ArrayList<>(Arrays.asList(1,2,3));
      Tuple tuple = new Tuple(array);

      assertTrue(tuple.getColValue(0) == 1);
      assertTrue(tuple.getColValue(1) == 2);
      assertTrue(tuple.getColValue(2) == 3);
   }

   @Test
   public void testEmptyTuple() {
      ArrayList<Integer> array = new ArrayList<>();
      Tuple tuple = new Tuple(array);

      assertTrue(tuple.toString().equals(""));
   }

   @Test
   public void test2TupleConstructor1() {
      ArrayList<Integer> array1 = new ArrayList<>(Arrays.asList(1,2,3));
      ArrayList<Integer> array2 = new ArrayList<>(Arrays.asList(4,5,6));
      Tuple tuple = new Tuple(new Tuple(array1), new Tuple(array2));

      assertTrue(tuple.toString().equals("1,2,3,4,5,6"));
   }

   @Test
   public void test2TupleConstructor2() {
      ArrayList<Integer> array1 = new ArrayList<>(Arrays.asList(1,2,3));
      ArrayList<Integer> array2 = new ArrayList<>();
      Tuple tuple = new Tuple(new Tuple(array1), new Tuple(array2));

      assertTrue(tuple.toString().equals("1,2,3"));
   }

   @Test
   public void test2TupleConstructor3() {
      ArrayList<Integer> array1 = new ArrayList<>();
      ArrayList<Integer> array2 = new ArrayList<>(Arrays.asList(4,5,6));
      Tuple tuple = new Tuple(new Tuple(array1), new Tuple(array2));

      assertTrue(tuple.toString().equals("4,5,6"));
   }

   @Test
   public void test2TupleConstructor4() {
      ArrayList<Integer> array1 = new ArrayList<>(Arrays.asList(1,2,3));
      Tuple tuple = new Tuple(new Tuple(array1), new Tuple(array1));

      assertTrue(tuple.toString().equals("1,2,3,1,2,3"));
   }

   @Test
   public void test2TupleConstructor5() {
      ArrayList<Integer> array = new ArrayList<>(Arrays.asList(1,2,3));
      Tuple temp = new Tuple(array);
      Tuple tuple = new Tuple(temp, temp);

      assertTrue(tuple.toString().equals("1,2,3,1,2,3"));
   }
}