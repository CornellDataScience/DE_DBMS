package Project2.Operators.Physical;


import Project2.Tuple.Tuple;
import Project2.Visitors.EvaluateExpressionVisitor;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;
import java.util.ArrayList;

import static Project2.DBCatalog.getBufferPages;
import static Project2.DBCatalog.getPageSize;

/**
 * Class for a physical block nested loop join operator
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class PhysicalBNLJOperator extends PhysicalJoinOperator {

   public PhysicalOperator leftOperator; // outer
   public PhysicalOperator rightOperator; // inner
   private Expression expression;
   private ArrayList<String> columns;

   private int blockSize;
   private ArrayList<Tuple> outerBlock;
   private int blockLocationTuple;
   private Tuple innerTuple;
   private boolean outerBlockLoaded;

   /**
    * Creates a physical block nested loop join operator to combine two tables
    *
    * @param left the left child operator that is being joined
    * @param right the right child operator that is being joined
    * @param expr the join expression
    * @param cols an arraylist of the full name of all columns in any table to be joined
    */
   public PhysicalBNLJOperator(PhysicalOperator left, PhysicalOperator right, Expression expr,
                               ArrayList<String> cols) {
      rightOperator = right;
      leftOperator = left;
      expression = expr;
      columns = cols;

      blockLocationTuple = 0;
      outerBlockLoaded = false;
   }

   @Override
   public Tuple getNextTuple() throws IOException {
      if (!outerBlockLoaded) {
         readOuterBlock();

         // either outer or inner table is empty
         if (innerTuple == null) {
            return null;
         }
      }

      // loops to ensure tuple is returned
      while (true) {

         // if at end of block
         if (blockLocationTuple == outerBlock.size()) {
            innerTuple = rightOperator.getNextTuple();
            blockLocationTuple = 0;

            // if innerTuple is null, then have read all inner
            if (innerTuple == null) {
               // read next outer block
               readOuterBlock();
               if (outerBlock != null) {
                  rightOperator.reset();
                  innerTuple = rightOperator.getNextTuple();
               } else {
                  // if outerBlock is null, then have read all outer for all inner and done
                  return null;
               }
            }
         }

         Tuple outerTuple = outerBlock.get(blockLocationTuple);
         // point to next tuple in block
         blockLocationTuple++;

         Tuple currentTuple = new Tuple(outerTuple, innerTuple);
         if (expression != null) {
            EvaluateExpressionVisitor visitor = new EvaluateExpressionVisitor(currentTuple, columns);

            expression.accept(visitor);
            if (visitor.getResult()) {
               return currentTuple;
            }
         } else {
            // if expression is null, all tuples are added
            return currentTuple;
         }
      }
   }

   @Override
   public ArrayList<String> getColumns() {
      return columns;
   }

   /**
    * Sets outerBlock by reading blockSize number of tuples from the outer operator
    *
    * @throws IOException if there is an issue reading from the input file
    */
   private void readOuterBlock() throws IOException {
      Tuple tuple = leftOperator.getNextTuple();

      if (!outerBlockLoaded && tuple != null) {
         // sets blockSize and loads first innerTuple
         blockSize = getBufferPages() * getPageSize(tuple.getColNum());
         innerTuple = rightOperator.getNextTuple();
         outerBlockLoaded = true;
      }

      // if null, then at the end of the outer table
      if (tuple == null) {
         outerBlock = null;
         outerBlockLoaded = false;
         return;
      } else {
         outerBlock = new ArrayList<>(blockSize);
         outerBlock.add(tuple);
      }

      // fill block
      for (int x = 1; x < blockSize; x++) {
         // if out of tuples
         if ((tuple = leftOperator.getNextTuple()) == null) {
            return;
         }
         outerBlock.add(tuple);
      }
   }

   @Override
   public void reset() throws IOException {
      leftOperator.reset();
      rightOperator.reset();

      outerBlockLoaded = false;
      blockLocationTuple = 0;
   }

   @Override
   public String toString() {
//      return getClass().getSimpleName() + ": left - " + leftOperator.toString() + " and right - "
//            + rightOperator.toString();
      return "BNLJ[" + expression + "]";
   }
}
