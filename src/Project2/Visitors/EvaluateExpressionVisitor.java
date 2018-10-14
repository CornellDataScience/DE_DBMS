package Project2.Visitors;


import Project2.Tuple.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.ArrayList;
import java.util.Stack;

/**
 * Evaluates the given expression. As of project 4, only AND, >, >=, =, !=, <, <=, longs, and
 * columns are supported. All other operations will throw an UnsupportedOperationException
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class EvaluateExpressionVisitor implements ExpressionVisitor {

   private Stack<Boolean> stackBool;
   private Stack<Integer> stackNum;
   private Tuple tuple;
   private ArrayList<String> columns;

   /**
    * Creates a visitor capable of evaluating a where expression
    *
    * @param t the tuple containing values
    * @param cols the arraylist of columns
    */
   public EvaluateExpressionVisitor(Tuple t, ArrayList<String> cols) {
      stackBool = new Stack<>();
      stackNum = new Stack<>();
      tuple = t;
      columns = cols;
   }

   /**
    * Returns value of expression, can be called multiple times
    *
    * @return boolean of whether the tuple passes the expression
    */
   public boolean getResult() {
      return stackBool.peek();
   }

   @Override
   public void visit(NullValue nullValue) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(Function function) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(InverseExpression inverseExpression) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(JdbcParameter jdbcParameter) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(DoubleValue doubleValue) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(LongValue longValue) {
      stackNum.push((int) longValue.toLong());
   }

   @Override
   public void visit(DateValue dateValue) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(TimeValue timeValue) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(TimestampValue timestampValue) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(Parenthesis parenthesis) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(StringValue stringValue) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(Addition addition) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(Division division) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(Multiplication multiplication) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(Subtraction subtraction) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(AndExpression andExpression) {
      andExpression.getRightExpression().accept(this);
      andExpression.getLeftExpression().accept(this);

      boolean bool1 = stackBool.pop();
      boolean bool2 = stackBool.pop();
      stackBool.push(bool1 && bool2);
   }

   @Override
   public void visit(OrExpression orExpression) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(Between between) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(EqualsTo equalsTo) {
      equalsTo.getRightExpression().accept(this);
      equalsTo.getLeftExpression().accept(this);

      stackBool.push(stackNum.pop().intValue() == stackNum.pop().intValue());
   }

   @Override
   public void visit(GreaterThan greaterThan) {
      greaterThan.getRightExpression().accept(this);
      greaterThan.getLeftExpression().accept(this);

      stackBool.push(stackNum.pop() > stackNum.pop());
   }

   @Override
   public void visit(GreaterThanEquals greaterThanEquals) {
      greaterThanEquals.getRightExpression().accept(this);
      greaterThanEquals.getLeftExpression().accept(this);

      stackBool.push(stackNum.pop() >= stackNum.pop());
   }

   @Override
   public void visit(InExpression inExpression) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(IsNullExpression isNullExpression) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(LikeExpression likeExpression) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(MinorThan minorThan) {
      minorThan.getRightExpression().accept(this);
      minorThan.getLeftExpression().accept(this);

      stackBool.push(stackNum.pop() < stackNum.pop());
   }

   @Override
   public void visit(MinorThanEquals minorThanEquals) {
      minorThanEquals.getRightExpression().accept(this);
      minorThanEquals.getLeftExpression().accept(this);

      stackBool.push(stackNum.pop() <= stackNum.pop());
   }

   @Override
   public void visit(NotEqualsTo notEqualsTo) {
      notEqualsTo.getRightExpression().accept(this);
      notEqualsTo.getLeftExpression().accept(this);

      stackBool.push(stackNum.pop().intValue() != stackNum.pop().intValue());
   }

   @Override
   public void visit(Column column) {
      stackNum.push(tuple.getColValue(columns.indexOf(column.getWholeColumnName())));
   }

   @Override
   public void visit(SubSelect subSelect) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(CaseExpression caseExpression) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(WhenClause whenClause) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(ExistsExpression existsExpression) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(AllComparisonExpression allComparisonExpression) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(AnyComparisonExpression anyComparisonExpression) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(Concat concat) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(Matches matches) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(BitwiseAnd bitwiseAnd) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(BitwiseOr bitwiseOr) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void visit(BitwiseXor bitwiseXor) {
      throw new UnsupportedOperationException();
   }
}
