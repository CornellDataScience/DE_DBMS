package Project2.Operators.Physical;

import Project2.Tuple.Tuple;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Class for a physical sort operator that will allow order by
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class PhysicalInPlaceSortOperator extends PhysicalSortOperator {

   public PhysicalOperator operator;
   private ArrayList<String> columns;
   public ArrayList<String> sortBy;
   private ArrayList<Tuple> lines;
   private int linesPlaces;

   /**
    * Creates a physical sort operator to order the columns by a given set of columns
    *
    * @param op the operator the project is wrapping
    * @param cols the arraylist of all columns in the table
    * @param sort the arraylist of columns to sort by
    */
   public PhysicalInPlaceSortOperator(PhysicalOperator op, ArrayList<String> cols, ArrayList<String> sort) {
      columns = cols;
      // keeps cols parameter from being altered
      cols = new ArrayList<>(cols);

      cols.removeAll(sort);
      sort.addAll(cols);
      sortBy = sort;
      operator = op;
      lines = null;
      linesPlaces = -1;
   }

   @Override
   public Tuple getNextTuple() throws IOException {
      if (lines == null) {
         Tuple tuple;
         lines = new ArrayList<>();
         while ((tuple = operator.getNextTuple()) != null) {
            lines.add(tuple);
         }

         /*
            Returns a negative integer, zero, or a positive integer as the first argument is less
             than, equal to, or greater than the second.
          */
         lines.sort((o1, o2) -> {
            for (String sortByCol : sortBy) {
               if (o1.getColValue(columns.indexOf(sortByCol)) > o2.getColValue(columns.indexOf
                     (sortByCol))) {
                  return 1;
               } else if (o1.getColValue(columns.indexOf(sortByCol)) < o2.getColValue(columns
                     .indexOf(sortByCol)))
                  return -1;
            }
            return 0;
         });
      }

      // since this is a block operator, lines are sorted first and then returned one by one
      // until they hit the end of the list. linesPlaces is the counter
      linesPlaces ++;
      if (linesPlaces < lines.size() && linesPlaces >= 0) {
         return lines.get(linesPlaces);
      } else {
         return null;
      }
   }

   @Override
   public ArrayList<String> getColumns() {
      return columns;
   }

   @Override
   public void reset() throws IOException {
      operator.reset();
      lines = null;
      linesPlaces = -1;
   }

   /*
      The reset here will receive how many tuples to go back instead of which index in sequence to go to
   */
   @Override
   public void reset(int i) {
      //if i is number of tuples to go back
      linesPlaces = linesPlaces - i - 1;
   }

   @Override
   public String toString() {
//      return getClass().getSimpleName() + ": " + operator.toString();
      return "InMemSort" + sortBy;
   }
}
