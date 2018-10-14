package Project2.Operators.Physical;

import java.io.IOException;

/**
 * Abstract class for a sort physical operator node
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public abstract class PhysicalSortOperator extends PhysicalOperator {

   /**
    * Resets file reader and thus operator to index i
    */
   public abstract void reset(int i) throws IOException;
}
