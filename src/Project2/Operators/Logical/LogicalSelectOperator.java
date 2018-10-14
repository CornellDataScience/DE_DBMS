package Project2.Operators.Logical;

import Project2.Operators.Physical.PhysicalOperator;
import Project2.Visitors.LogicalOperatorVisitor;
import Project2.Visitors.SelectionCondition;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Class for a logical select operator that will allow where clauses
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class LogicalSelectOperator extends LogicalOperator {
   private Table table;
   private HashSet<SelectionCondition> usableSelectCondition;
   private Expression residualExpression;
   private ArrayList<String> cols;
   public int printDashesNumber = 0;
   /**
    * Creates a logical select operator to select rows based on the where expression. At least
    * one of usableSC and residualE must not be null
    *
    * @param t the select table
    * @param usableSC the select condition that may have an index
    * @param residualE the residual select expression
    */
   public LogicalSelectOperator(Table t, HashSet<SelectionCondition> usableSC, Expression residualE, ArrayList<String> cols) {
      table = t;
      usableSelectCondition = usableSC;
      residualExpression = residualE;
      this.cols = cols;
   }

   @Override
   public Table getTable() {
      return table;
   }

   @Override
   public ArrayList<String> getColumns() {
      return cols;
   }

   /**
    * @return the residual expression
    */
   public Expression getResidualExpression() {
      return residualExpression;
   }

   /**
    * @return set of the usable select conditions
    */
   public HashSet<SelectionCondition> getUsableSelectCondition() {
      return usableSelectCondition;
   }

   /**
    * @return the full where expression
    */
   public Expression getFullExpression() {
      Expression expression = null;
      if (residualExpression != null) {
         expression = residualExpression;
         for (SelectionCondition sc : usableSelectCondition) {
            expression = new AndExpression(expression, sc.getExpression());
         }
      } else {
         for (SelectionCondition sc : usableSelectCondition) {
            if (expression == null) {
               expression = sc.getExpression();
            } else {
               expression = new AndExpression(expression, sc.getExpression());
            }
         }
      }
      return expression;
   }

   @Override
   public PhysicalOperator accept(LogicalOperatorVisitor visitor, int i, boolean b) {
      return visitor.visit(this, i, b);
   }

   @Override
   public String toString() {
      String result = "Select[" + getFullExpression() + "]\n";
      for (int i = 0; i < printDashesNumber + 1; i++){
         result += "-";
      }
      result += "Scan[" + table.getName() + "]";
      return result;
   }

   public String toStringLeaf(int printDashNum) {
      String result = "";
      for (int i = 0; i < printDashNum; i++){
         result += "-";
      }
      result += "Select[" + getFullExpression() + "]\n";
      for (int i = 0; i < printDashNum + 1; i++){
         result += "-";
      }
      result += "Leaf[" + table.getName() + "]";
      return result;
   }
}
