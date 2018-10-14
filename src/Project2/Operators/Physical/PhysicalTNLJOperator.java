package Project2.Operators.Physical;

import Project2.Tuple.Tuple;
import Project2.Visitors.EvaluateExpressionVisitor;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Class for a physical tuple nested loop join operator to combine two tables
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class PhysicalTNLJOperator extends PhysicalJoinOperator {

   public PhysicalOperator leftOperator;
   public PhysicalOperator rightOperator;
   private Expression expression;
   private ArrayList<String> columns;
   private Tuple currentLeftTuple;
   private boolean tupleSet = false;

   /**
    * Creates a physical tuple nested loop join operator to combine two tables
    *
    * @param left the left child operator that is being joined
    * @param right the right child operator that is being joined
    * @param expr the join expression
    * @param cols an arraylist of the full name of all columns in any table to be joined
    */
   public PhysicalTNLJOperator(PhysicalOperator left, PhysicalOperator right, Expression expr, ArrayList<String> cols) {
      rightOperator = right;
      leftOperator = left;
      expression = expr;
      columns = cols;
   }

   @Override
   public Tuple getNextTuple() throws IOException {
      // checks if there is a previous tuple set
      if (!tupleSet) {
         currentLeftTuple = leftOperator.getNextTuple();
         tupleSet = true;
      }

      Tuple rightTuple, tuple;
      // loops to ensure tuple is returned
      while (true) {
         if (currentLeftTuple != null) {
            // loops through right table, checking if it passes the join expression
            while ((rightTuple = rightOperator.getNextTuple()) != null) {
               tuple = new Tuple(currentLeftTuple, rightTuple);
               if (expression != null) {
                  EvaluateExpressionVisitor visitor = new EvaluateExpressionVisitor(tuple, columns);
                  expression.accept(visitor);
                  if (visitor.getResult()) {
                     return tuple;
                  }
               } else {
                  // if expression is null, all tuples are added
                  return tuple;
               }
            }
            rightOperator.reset();
            currentLeftTuple = leftOperator.getNextTuple();
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
      leftOperator.reset();
      rightOperator.reset();
      tupleSet = false;
   }

   @Override
   public String toString() {
//      return getClass().getSimpleName() + ": left - " + leftOperator.toString() + " and right - "
//            + rightOperator.toString();
      return "TNLJ[" + expression + "]";
   }
}
