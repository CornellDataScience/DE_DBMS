package Project2.Operators.Physical;

import Project2.Tuple.Tuple;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Class for a physical project operator to select desired attributes
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class PhysicalProjectOperator extends PhysicalOperator {

   public PhysicalOperator operator;
   private ArrayList<String> columns;
   private ArrayList<String> selectedColumns;

   /**
    * Creates a physical pproject operator to select columns to return
    *
    * @param op the operator the project is wrapping
    * @param cols the arraylist of all columns in the table
    * @param selectedCols the arraylist of columns to project
    */
   public PhysicalProjectOperator(PhysicalOperator op, ArrayList<String> cols, ArrayList<String> selectedCols) {
      operator = op;
      columns = new ArrayList<>(cols);
      selectedColumns = selectedCols;
   }

   @Override
   public Tuple getNextTuple() throws IOException {
      Tuple tuple = operator.getNextTuple();

      if (tuple != null) {
         return new Tuple(tuple, columns, selectedColumns);
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
   }

   @Override
   public String toString() {
//      return getClass().getSimpleName() + ": " + operator.toString();
      return "Project" + selectedColumns;
   }
}
