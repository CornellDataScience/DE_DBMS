package Project2.Tuple;

import java.util.ArrayList;
import java.util.Queue;

/**
 * Represents a row in a table containing only the necessary columns
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class Tuple {

   private ArrayList<Integer> tuple;
   public TupleBinaryReader tr;
   public Queue<TupleBinaryReader> queue;

   public int getSize() {
      return tuple.size();
   }
   /**
    * Constructor for selecting entire row
    *
    * @param list the row from the data table
    */
   public Tuple(ArrayList<Integer> list) {
      tuple = list;
   }

   /**
    * Constructor for combining two tuples and selecting entire row
    *
    * @param tuple1 the first tuple (left)
    * @param tuple2 the second tuple (right)
    */
   public Tuple(Tuple tuple1, Tuple tuple2) {
      tuple = new ArrayList<>(tuple1.tuple);
      tuple.addAll(tuple2.tuple);
   }

   /**
    * Constructor for selecting specific columns from tuple
    *
    * @param t the row from the data table
    * @param tableCols a list of all the table's columns
    * @param cols the column names to select
    */
   public Tuple(Tuple t, ArrayList<String> tableCols, ArrayList<String> cols) {
      tuple = new ArrayList<>();
      for (String col : cols) {
         tuple.add(t.tuple.get(tableCols.indexOf(col)));
      }
   }

   /**
    * @param index the integer index of the specified column
    * @return the integer in the given column
    */
   public int getColValue(int index) {
      return tuple.get(index);
   }

   /**
    * @return an arraylist representing the tuple
    */
   public ArrayList<Integer> getAllCols() {
      return tuple;
   }

   /**
    * @return the number of columns in the tuple
    */
   public int getColNum() {
      return tuple.size();
   }

   @Override
   public String toString() {
      StringBuilder s = new StringBuilder();
      if (tuple.size() != 0) {
         for (int x : tuple) {
            s.append(x).append(",");
         }

         return s.deleteCharAt(s.length() - 1).toString();
      } else {
         return "";
      }
   }
}
