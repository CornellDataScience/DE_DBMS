package Project2.Visitors;

import Project2.Visitors.SelectExpressionVisitor.Sign;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

import java.util.ArrayList;

/**
 * Object representing a select conditions
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class SelectionCondition {

   private ArrayList<String> columns;
   private Expression expression;
   private SelectIndex selectIndex;

   /**
    * Creates a new SelectionCondition for the parameters
    *
    * @param column the column the selection pertains to
    * @param expr the selection expression
    * @param si the SelectIndex for the selection
    */
   public SelectionCondition(String column, Expression expr, SelectIndex si) {
      columns = new ArrayList<>();
      columns.add(column);

      expression = expr;
      selectIndex = si;
   }

   /**
    * Creates a new SelectionCondition for an expression with two columns
    *
    * @param column1 the column the selection pertains to
    * @param column2 the column the selection pertains to
    * @param expr the selection expression
    * @param si the SelectIndex for the selection
    */
   public SelectionCondition(String column1, String column2, Expression expr, SelectIndex si) {
      this(column1, expr, si);
      columns.add(column2);
   }

   /**
    * Updates existing select index to reflect new select
    *
    * @param sign the sign of the new expression
    * @param num the value of the new expression
    */
   public void updateSelectIndex(Sign sign, int num) {
      selectIndex.update(sign, num);
   }

   /**
    * @param expr the Expression to add to expression
    */
   public void addExpression(Expression expr) {
      if (expr != null) {
         if (expression == null) {
            expression = expr;
         } else {
            expression = new AndExpression(expression, expr);
         }
      }
   }

   /**
    * Combines two SelectionConditions
    *
    * @param sc the other selectCondition to merge with
    */
   public void merge(SelectionCondition sc) {
      columns.addAll(sc.columns);
      addExpression(sc.expression);

      selectIndex = new SelectIndex(selectIndex, sc.selectIndex);
   }

   /**
    * @return the select expression
    */
   public Expression getExpression() {
      return expression;
   }

   /**
    * @return the select index
    */
   public SelectIndex getSelectIndex() {
      return selectIndex;
   }

   /**
    * @return the columns in the select expression
    */
   public ArrayList<String> getColumns() {
      return columns;
   }

   @Override
   public String toString() {
      return "[" + columns.toString() + ", " + selectIndex.toString() + "]";
   }
}
