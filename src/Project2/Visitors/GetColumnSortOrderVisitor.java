package Project2.Visitors;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;

/**
 *
 * As of project 4, only AND, >, >=, =, !=, <, <=, longs, and
 * columns are supported. All other operations will throw an UnsupportedOperationException
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class GetColumnSortOrderVisitor implements ExpressionVisitor {

   private LinkedHashSet<String> colsLeftOrdering, colsRightOrdering;
   private ArrayList<String> colsLeft, colsRight;
   private ArrayList<Integer> colsLeftIndices, colsRightIndices;

   /**
    * Creates a visitor capable of determining the sort order for a SMJ on the given expression
    */
   public GetColumnSortOrderVisitor(ArrayList<String> cl, ArrayList<String> cr) {
      colsLeftOrdering = new LinkedHashSet<>();
      colsRightOrdering = new LinkedHashSet<>();
      colsLeft = cl;
      colsRight = cr;
      colsLeftIndices = new ArrayList<>();
      colsRightIndices = new ArrayList<>();
   }

   /**
    * @return an ArrayList of tableLeft sort column ordering
    */
   public ArrayList<String> getColsLeftOrdering() {
      Iterator<String> iterator = colsLeftOrdering.iterator();
      ArrayList<String> ret = new ArrayList<>();
      while (iterator.hasNext()) {
         ret.add(iterator.next());
      }

      return ret;
   }

   /**
    * @return an ArrayList of tableRight sort column ordering
    */
   public ArrayList<String> getColsRightOrdering() {
      Iterator<String> iterator = colsRightOrdering.iterator();
      ArrayList<String> ret = new ArrayList<>();
      while (iterator.hasNext()) {
         ret.add(iterator.next());
      }

      return ret;
   }

   /**
    * @return an ArrayList of the left Columns' indices
    */
   public ArrayList<Integer> getColsLeftIndices() {
      return colsLeftIndices;
   }

   /**
    * @return an ArrayList of the right Columns' indices
    */
   public ArrayList<Integer> getColsRightIndices() {
      return colsRightIndices;
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
      throw new UnsupportedOperationException();
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
      andExpression.getLeftExpression().accept(this);
      andExpression.getRightExpression().accept(this);
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
      equalsTo.getLeftExpression().accept(this);
      equalsTo.getRightExpression().accept(this);
   }

   @Override
   public void visit(GreaterThan greaterThan) {
      greaterThan.getLeftExpression().accept(this);
      greaterThan.getRightExpression().accept(this);
   }

   @Override
   public void visit(GreaterThanEquals greaterThanEquals) {
      greaterThanEquals.getLeftExpression().accept(this);
      greaterThanEquals.getRightExpression().accept(this);
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
      minorThan.getLeftExpression().accept(this);
      minorThan.getRightExpression().accept(this);
   }

   @Override
   public void visit(MinorThanEquals minorThanEquals) {
      minorThanEquals.getLeftExpression().accept(this);
      minorThanEquals.getRightExpression().accept(this);
   }

   @Override
   public void visit(NotEqualsTo notEqualsTo) {
      notEqualsTo.getLeftExpression().accept(this);
      notEqualsTo.getRightExpression().accept(this);
   }

   @Override
   public void visit(Column column) {
      String colName = column.getWholeColumnName();
      if (colsLeft.contains(colName)) {
         colsLeftOrdering.add(colName);
         colsLeftIndices.add(colsLeft.indexOf(colName));
      } else {
         colsRightOrdering.add(colName);
         colsRightIndices.add(colsRight.indexOf(colName));
      }
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
