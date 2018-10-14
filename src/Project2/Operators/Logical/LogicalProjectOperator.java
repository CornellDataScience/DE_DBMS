package Project2.Operators.Logical;

import Project2.Operators.Physical.PhysicalOperator;
import Project2.Visitors.LogicalOperatorVisitor;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;

/**
 * Class for a logical project operator to select desired attributes
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class LogicalProjectOperator extends LogicalOperator {

   private LogicalOperator operator;
   private ArrayList<String> selectedColumns;

   /**
    * Creates a logical project operator to select columns to return
    *
    * @param op the operator the project is wrapping
    * @param selectedCols the arraylist of columns to project
    */
   public LogicalProjectOperator(LogicalOperator op, ArrayList<String> selectedCols) {
      operator = op;
      selectedColumns = selectedCols;
   }

   /**
    * @return the logical child operator
    */
   public LogicalOperator getOperator() {
      return operator;
   }

   /**
    * @return the arraylist of projected column names
    */
   public ArrayList<String> getSelectedColumns() {
      return selectedColumns;
   }

   @Override
   public PhysicalOperator accept(LogicalOperatorVisitor visitor, int i, boolean b) {
      return visitor.visit(this, i, b);
   }

   @Override
   public Table getTable() {
      return operator.getTable();
   }

   @Override
   public ArrayList<String> getColumns() {
      throw new UnsupportedOperationException();
   }

   @Override
   public String toString() {
      return "Project" + selectedColumns;
   }
}
