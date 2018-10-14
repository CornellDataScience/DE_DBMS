package Project2.Visitors;

import Project2.Operators.Logical.*;
import Project2.Operators.Physical.PhysicalOperator;

/**
 * An interface for a visitor to traverse the logical query plan in order to build a physical
 * query plan
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public interface LogicalOperatorVisitor {

   /**
    * For the PhysicalPlanBuilder
    *
    * @param op the LogicalDuplicateEliminationOperator operator
    * @return the new PhysicalOperator
    */
   PhysicalOperator visit(LogicalDuplicateEliminationOperator op, int i, boolean NeedToPrint);

   /**
    * For the PhysicalPlanBuilder
    *
    * @param op the LogicalJoinOperator operator
    * @return the new PhysicalOperator
    */
   PhysicalOperator visit(LogicalJoinOperator op, int i, boolean NeedToPrint);

   /**
    * For the PhysicalPlanBuilder
    *
    * @param op the LogicalProjectOperator operator
    * @return the new PhysicalOperator
    */
   PhysicalOperator visit(LogicalProjectOperator op, int i, boolean NeedToPrint);

   /**
    * For the PhysicalPlanBuilder
    *
    * @param op the LogicalScanOperator operator
    * @return the new PhysicalOperator
    */
   PhysicalOperator visit(LogicalScanOperator op, int i, boolean NeedToPrint);

   /**
    * For the PhysicalPlanBuilder
    *
    * @param op the LogicalSelectOperator operator
    * @return the new PhysicalOperator
    */
   PhysicalOperator visit(LogicalSelectOperator op, int i, boolean NeedToPrint);

   /**
    * For the PhysicalPlanBuilder
    *
    * @param op the LogicalSortOperator operator
    * @return the new PhysicalOperator
    */
   PhysicalOperator visit(LogicalSortOperator op, int i, boolean NeedToPrint);
}
