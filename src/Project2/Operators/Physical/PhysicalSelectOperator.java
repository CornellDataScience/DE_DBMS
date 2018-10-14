package Project2.Operators.Physical;

import Project2.Tuple.Tuple;
import Project2.Visitors.EvaluateExpressionVisitor;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Class for a physical select operator that will allow where clauses
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class PhysicalSelectOperator extends PhysicalOperator {
   public PhysicalOperator child;
   private Expression expression;
   private ArrayList<String> columns;

   /**
    * Creates a physical select operator to select rows based on the where expression
    *
    * @param op the physical child operator
    * @param cols the arraylist of all columns in the table
    * @param expr the select expression
    */
   public PhysicalSelectOperator(PhysicalOperator op, ArrayList<String> cols, Expression expr) {
      child = op;
      columns = cols;
      expression = expr;
   }

   @Override
   public Tuple getNextTuple() throws IOException {
      Tuple tuple;
      while (true) {
         tuple = child.getNextTuple();

         if (tuple != null) {
            EvaluateExpressionVisitor visitor = new EvaluateExpressionVisitor(tuple, columns);
            expression.accept(visitor);
            // evaluates and returns result of the expression
            if (visitor.getResult()) {
               return tuple;
            }
         } else {
            return null;
         }
      }
   }

   @Override
   public ArrayList<String> getColumns() {
      return columns;
   }

   @Override
   public void reset() throws IOException {
      child.reset();
   }

   @Override
   public String toString() {
      return "Select[" + expression + "]";
   }
}
