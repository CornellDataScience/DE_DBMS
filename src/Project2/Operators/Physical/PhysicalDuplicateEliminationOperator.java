package Project2.Operators.Physical;

import Project2.Tuple.Tuple;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Class for a physical duplication eliminating operator that will allow distinct
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class PhysicalDuplicateEliminationOperator extends PhysicalOperator {

   public PhysicalOperator operator;
   private Tuple previous;

   /**
    * Creates a physical operator to remove duplicate entries
    *
    * @param op the SortOperator that will sort the table
    */
   public PhysicalDuplicateEliminationOperator(PhysicalOperator op) {
      operator = op;
      previous = null;
   }

   @Override
   public Tuple getNextTuple() throws IOException {
      // loops so a tuple is returned each time
      while (true) {
         if (previous == null) {
            previous = operator.getNextTuple();
            return previous;
         } else {
            Tuple tuple = operator.getNextTuple();
            if (tuple != null) {
               if (!tuple.toString().equals(previous.toString())) {
                  previous = tuple;
                  return previous;
               }
            } else {
               return null;
            }
         }
      }
   }

   @Override
   public ArrayList<String> getColumns() {
      return operator.getColumns();
   }

   @Override
   public void reset() throws IOException {
      operator.reset();
      previous = null;
   }

   @Override
   public String toString() {
//      return getClass().getSimpleName() + ": " + operator.toString();
      return "DupElim";
   }
}
