package Project2.Visitors;


import Project2.ReversiblePair;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;

import static Project2.Visitors.SelectExpressionVisitor.Sign.USABLE;

/**
 * Parses the given where expression for a select operation to determine whether an index is
 * applicable and, if so, the bounds. As of project 4, only AND, >, >=, =, !=, <, <=, longs, and
 * columns are supported. All other operations will throw an UnsupportedOperationException
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class SelectExpressionVisitor implements ExpressionVisitor {

   // only usable allowed in unionFind
   private HashMap<String, SelectionCondition> unionFind; // string is column
   // condiiton string keys are table references
   private HashMap<ReversiblePair<String, String>, Expression> usableJoinConditions;
   private HashMap<ReversiblePair<String, String>, Expression> residualJoinConditions;
   private HashMap<String, Expression> residualSelectConditions;
   private String col1, col2, table1, table2; //col1 matches left expression and col2 right
   private boolean c1Done;
   private Sign sign;
   private int num;

   public enum Sign {
      EQUALS, NOTEQUALS, LESSEQUALS, LESS, GREATEREQUALS, GREATER;

      public static final EnumSet<Sign> USABLE = EnumSet.of(EQUALS, LESSEQUALS, LESS,
            GREATEREQUALS, GREATER);
   }

   /**
    * Creates a visitor capable of parsing a select expression to optimize the select
    */
   public SelectExpressionVisitor() {
      unionFind = new HashMap<>();
      usableJoinConditions = new HashMap<>();
      residualJoinConditions = new HashMap<>();
      residualSelectConditions = new HashMap<>();

      col1 = null;
      table1 = null;
      col2 = null;
      table2 = null;
      c1Done = false;
   }

   /**
    * @return the usable join conditions
    */
   public HashMap<ReversiblePair<String,String>, Expression> getUsableJoinConditions() {
      return usableJoinConditions;
   }

   /**
    * @return the residual join conditions
    */
   public HashMap<ReversiblePair<String,String>, Expression> getResidualJoinConditions() {
      return residualJoinConditions;
   }

   /**
    * @return the residual select conditions
    */
   public HashMap<String, Expression> getResidualSelectConditions() {
      return residualSelectConditions;
   }

   /**
    * @return the evaluated unionFind set
    */
   public HashSet<SelectionCondition> getUnionFindSet() {
      return new HashSet<>(unionFind.values());
   }

   /**
    * Changes the unionFind to be keyed to table, not column
    *
    * @return the evaluated unionFind
    */
   public HashMap<String, HashSet<SelectionCondition>> getUnionFind() {
      HashMap<String, HashSet<SelectionCondition>> uF = new HashMap<>();
      for (String col : unionFind.keySet()) {
         String table = col.split("\\.")[0];
         if (uF.containsKey(table)) {
            uF.get(table).add(unionFind.get(col));
         } else {
            HashSet<SelectionCondition> set = new HashSet<>();
            set.add(unionFind.get(col));
            uF.put(table, set);
         }
      }

      return uF;
   }

   /**
    * Checks if col1, col2 are in the correct ordering and adds given expression to the
    * hashmap. Then, resets  col1, col2, c1Done
    *
    * @param expr the given expression to add with referenced columns col1 and col2
    */
   private void addExpression(BinaryExpression expr) {
      // attr OP attr - either unionFind (and usableJoinConditions) or residualJoinConditions
      if (col1 != null && col2 != null && !table1.equals(table2)) {
         // attr = attr - unionFind
         if (sign == Sign.EQUALS) {
            SelectionCondition sc1 = unionFind.get(col1);
            SelectionCondition sc2 = unionFind.get(col2);

            // handle if conditions are/ aren't already in the hashmap
            if (sc1 != null) {
               if (sc2 != null) { // neither is null, both already in unionFind (merging of two sets)
                  for (String col : sc2.getColumns()) { // change pointers to merged sc
                     unionFind.put(col, sc1);
                  }
                  sc1.merge(sc2);
               } else { // col1 in unionFind
                  unionFind.put(col2, sc1);
               }
            } else if (sc2 != null) { // col2 in unionFind
               unionFind.put(col1, sc2);
            } else { // neither is in unionFind
               SelectionCondition newSC = new SelectionCondition(col1, col2, null, new SelectIndex());
               unionFind.put(col1, newSC);
               unionFind.put(col2, newSC);
            }

            // add to usableJoinConditions
            ReversiblePair<String, String> pair = new ReversiblePair<>(table1, table2);
            Expression tempExpr = usableJoinConditions.get(pair);
            if (tempExpr != null) {
               usableJoinConditions.put(pair, new AndExpression(tempExpr, expr));
            } else {
               usableJoinConditions.put(pair, expr);
            }
         } else { // residualJoinConditions
            // add residual join condition (expr)
            ReversiblePair<String, String> pair = new ReversiblePair<>(table1, table2);
            Expression tempExpr = residualJoinConditions.get(pair);
            if (tempExpr == null) {
               residualJoinConditions.put(pair, expr);
            } else {
               residualJoinConditions.put(pair, new AndExpression(tempExpr, expr));
            }
         }

      // attr OP value or attr OP attr with same table
      } else {
         if (col1 == null) { // value OP attr
            col1 = col2;
            table1 = table2;
            col2 = null;
            table2 = null;
         }

         // if not != or both one column
         boolean usable = (col2 == null) && USABLE.contains(sign);
         if (usable) { // going in unionFind
            SelectionCondition sc = unionFind.get(col1);
            if (sc != null) {
               sc.addExpression(expr);
               sc.updateSelectIndex(sign, num);
            } else {
               SelectIndex si = new SelectIndex();
               si.update(sign, num);
               sc = new SelectionCondition(col1, expr, si);
               unionFind.put(col1, sc);
            }
         } else { // going into residualSelectConditions
            Expression tempExpr = residualSelectConditions.get(table1);
            if (tempExpr != null) {
               residualSelectConditions.put(table1, new AndExpression(tempExpr, expr));
            } else {
               residualSelectConditions.put(table1, expr);
            }
         }
      }

      col1 = null;
      table1 = null;
      col2 = null;
      table2 = null;
      c1Done = false;
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
      num = (int) longValue.getValue();
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
      c1Done = true;
      equalsTo.getRightExpression().accept(this);

      sign = Sign.EQUALS;
      addExpression(equalsTo);
   }

   @Override
   public void visit(GreaterThan greaterThan) {
      greaterThan.getLeftExpression().accept(this);
      c1Done = true;
      greaterThan.getRightExpression().accept(this);

      if (col1 == null ) {
         sign = Sign.LESS;
      } else {
         sign = Sign.GREATER;
      }
      addExpression(greaterThan);
   }

   @Override
   public void visit(GreaterThanEquals greaterThanEquals) {
      greaterThanEquals.getLeftExpression().accept(this);
      c1Done = true;
      greaterThanEquals.getRightExpression().accept(this);

      if (col1 == null ) {
         sign = Sign.LESSEQUALS;
      } else {
         sign = Sign.GREATEREQUALS;
      }
      addExpression(greaterThanEquals);
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
      c1Done = true;
      minorThan.getRightExpression().accept(this);

      if (col1 == null ) {
         sign = Sign.GREATER;
      } else {
         sign = Sign.LESS;
      }
      addExpression(minorThan);
   }

   @Override
   public void visit(MinorThanEquals minorThanEquals) {
      minorThanEquals.getLeftExpression().accept(this);
      c1Done = true;
      minorThanEquals.getRightExpression().accept(this);

      if (col1 == null ) {
         sign = Sign.GREATEREQUALS;
      } else {
         sign = Sign.LESSEQUALS;
      }
      addExpression(minorThanEquals);
   }

   @Override
   public void visit(NotEqualsTo notEqualsTo) {
      notEqualsTo.getLeftExpression().accept(this);
      c1Done = true;
      notEqualsTo.getRightExpression().accept(this);

      sign = Sign.NOTEQUALS;
      addExpression(notEqualsTo);
   }

   @Override
   public void visit(Column column) {
      // checks whether this is the first or second column in the expression
      if (c1Done) {
         col2 = column.getWholeColumnName();
         table2 = col2.split("\\.")[0];
      } else {
         col1 = column.getWholeColumnName();
         table1 = col1.split("\\.")[0];
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
