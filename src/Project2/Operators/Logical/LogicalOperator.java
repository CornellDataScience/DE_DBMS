package Project2.Operators.Logical;

import Project2.Operators.Physical.PhysicalOperator;
import Project2.Visitors.LogicalOperatorVisitor;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;

/**
 * Top-level abstract class for a logical operator node
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public abstract class LogicalOperator {

   /**
    * For the PhysicalPlanBuilder visitor
    *
    * @param visitor the LogicalOperatorVisitor
    * @param NeedToPrint
    * @return the PhysicalOperator
    */
   public abstract PhysicalOperator accept(LogicalOperatorVisitor visitor, int i, boolean NeedToPrint);

   public abstract Table getTable();

   public abstract ArrayList<String> getColumns();
}
