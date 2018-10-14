package Project2.Operators.Logical;

import Project2.Operators.Physical.PhysicalOperator;
import Project2.ReversiblePair;
import Project2.Visitors.LogicalOperatorVisitor;
import Project2.Visitors.SelectionCondition;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Class for a logical join operator to combine two tables
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class LogicalJoinOperator extends LogicalOperator {

   private ArrayList<LogicalOperator> operators;
   private ArrayList<String> tables;
   private ArrayList<String> aliases;
   private HashMap<ReversiblePair<String, String>, Expression> usableExpressionMap;
   private HashMap<ReversiblePair<String, String>, Expression> residualExpressionMap;
   private Expression residualExpression;
   private HashSet<SelectionCondition> unionFind;
   public int printDashesNumber = 0;

   /**
    * Creates a logical join operator to combine two tables
    *
    * @param ops an arraylist of the scan/ selects the join is on top of
    * @param tables an arraylist of the table full names for all of the operators
    * @param aliases an arraylist of the table aliases for all of the operators
    * @param uExprMap a map of all of the usable (equals) expressions for a join
    * @param rExprMap a map of all of the unusable (not equals) expression for a join
    * @param uF the union find for the join
    */
   public LogicalJoinOperator(ArrayList<LogicalOperator> ops, ArrayList<String> tables, ArrayList<String> aliases, HashMap<ReversiblePair<String, String>, Expression> uExprMap, HashMap<ReversiblePair<String, String>, Expression> rExprMap, HashSet<SelectionCondition> uF) {
      operators = ops;
      this.tables = tables;
      this.aliases = aliases;
      usableExpressionMap = uExprMap;
      residualExpressionMap = rExprMap;
      unionFind = uF;

      // gets the residual join expression
      residualExpression = null;
      for (Expression expr : rExprMap.values()) {
         if (residualExpression == null) {
            residualExpression = expr;
         } else {
            residualExpression = new AndExpression(residualExpression, expr);
         }
      }
   }

   /**
    * @return the given LogicalOperator
    */
   public LogicalOperator getOperator(int i) {
      return operators.get(i);
   }

   public HashMap<ReversiblePair<String, String>, Expression> getUsableExpressionMap () {
      return usableExpressionMap;
   }

   public HashMap<ReversiblePair<String, String>, Expression> getResidualExpressionMap(){
      return residualExpressionMap;
   }

   public ArrayList<LogicalOperator> getOperators() {
      return operators;
   }

   /**
    * @return an arraylist of child table full names
    */
   public ArrayList<String> getTables() {
      return tables;
   }

   /**
    * @return an arraylist of child table aliases
    */
   public ArrayList<String> getAliases() {
      return aliases;
   }

   @Override
   public PhysicalOperator accept(LogicalOperatorVisitor visitor, int i, boolean b) {
      return visitor.visit(this, i, b);
   }

   @Override
   public Table getTable() {
      throw new UnsupportedOperationException();
   }

   @Override
   public ArrayList<String> getColumns() {
      throw new UnsupportedOperationException();
   }

   @Override
   public String toString() {
      String r = residualExpression == null ? "" : residualExpression.toString();
      String result = "Join[" + r + "]\n";
      for (SelectionCondition sc : unionFind) {
         result += sc + "\n";
      }
      for (LogicalOperator o: operators){
         if (o instanceof LogicalScanOperator)
            result += ((LogicalScanOperator)o).toStringLeaf(printDashesNumber + 1) + "\n";
         else if (o instanceof LogicalSelectOperator)
            result += ((LogicalSelectOperator)o).toStringLeaf(printDashesNumber + 1) + "\n";
         else
            result +=o.toString() + "\n";
      }
      result = result.substring(0, result.length() - 1);
      return result;
   }
}
