package Project2.Operators.Logical;

import Project2.Operators.Physical.PhysicalOperator;
import Project2.Visitors.LogicalOperatorVisitor;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;

/**
 * Class for a logical sort operator that will allow order by
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class LogicalSortOperator extends LogicalOperator {

   private LogicalOperator operator;
   private ArrayList<String> sortBy;

   /**
    * Creates a logical sort operator to order the columns by a given set of columns
    *
    * @param op the operator the project is wrapping
    * @param sort the arraylist of columns to sort by
    */
   public LogicalSortOperator(LogicalOperator op, ArrayList<String> sort) {
      operator = op;
      sortBy = sort;
   }

   /**
    * @return the logical child operator
    */
   public LogicalOperator getOperator() {
      return operator;
   }

   /**
    * @return the arraylist of columns to sort by
    */
   public ArrayList<String> getSortBy() {
      return sortBy;
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
//      return getClass().getSimpleName() + ": " + operator.toString();
      return "Sort" + sortBy;
   }
}
