package Project2.Operators.Logical;

import Project2.Operators.Physical.PhysicalOperator;
import Project2.Visitors.LogicalOperatorVisitor;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;

/**
 * Class for a logical duplication eliminating operator that will allow distinct
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class LogicalDuplicateEliminationOperator extends LogicalOperator {

   private LogicalOperator operator;

   /**
    * Creates a logical operator to remove duplicate entries
    *
    * @param op the LogicalOperator that this rests on
    */
   public LogicalDuplicateEliminationOperator(LogicalOperator op) {
      operator = op;
   }

   /**
    * @return the logical child operator
    */
   public LogicalOperator getOperator() {
      return operator;
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
      return "DupElim";
   }
}
