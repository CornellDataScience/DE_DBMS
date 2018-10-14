package Project2;

import Project2.Operators.Logical.*;
import Project2.Operators.Physical.*;
import Project2.Optimizer.JoinCalculator;
import Project2.Visitors.GetColumnSortOrderVisitor;
import Project2.Visitors.LogicalOperatorVisitor;
import Project2.Visitors.SelectIndex;
import Project2.Visitors.SelectionCondition;
import Project2.btree.Index;
import javafx.util.Pair;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Table;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static Project2.DBCatalog.*;

/**
 * Translates a logical query plan to a physical query plan
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class PhysicalPlanBuilder implements LogicalOperatorVisitor {

   private ArrayList<String> columns;
   private BufferedWriter writer;

   /**
    * Creates a new visitor
    */
   public PhysicalPlanBuilder(BufferedWriter fw) {
      writer = fw;
      columns = new ArrayList<>();
   }

   @Override
   public PhysicalOperator visit(LogicalDuplicateEliminationOperator op, int i, boolean NeedToPrint) {
      //Visit duplicate's child
      addToProperties(op, i);
      PhysicalOperator childOp = op.getOperator().accept(this, i + 1, true);
      // project over a sort
      if (PhysicalSortOperator.class.isAssignableFrom(childOp.getClass())) {
         return new PhysicalDuplicateEliminationOperator(childOp);
      }

      //if child does not have a sort, create a sort to fit with distinct's spec
      childOp = new PhysicalExternalSortOperator(childOp, columns, columns);
      return new PhysicalDuplicateEliminationOperator(childOp);
   }

   @Override
   public PhysicalOperator visit(LogicalJoinOperator op, int i, boolean NeedToPrint) {
      ArrayList<LogicalOperator> ops = op.getOperators();

      JoinCalculator jc = new JoinCalculator(op.getTables(), op.getAliases(), op.getUsableExpressionMap());
      ArrayList<String> results = jc.getResult();
      ArrayList<LogicalOperator> first2 = new ArrayList<>();
      ArrayList<Integer> removal = new ArrayList<>();

      boolean reorder = false;
      if (!results.equals(op.getAliases())) {
         reorder = true;
         i ++;
      }

      for (int p = 0; p < results.size(); p++) {
         for (LogicalOperator lo : ops) {
            if (first2.size() == 2)
               break;
            if (lo.getTable().getAlias() != null && lo.getTable().getAlias().equals(results.get(p)) || lo.getTable().getAlias() == null && lo.getTable().getName().equals(results.get(p))) {
               removal.add(p);
               first2.add(lo);
               break;
            }
         }
      }

      ArrayList<String> joined = new ArrayList<>();
      if (removal.get(1) > removal.get(0)) {
         joined.add(results.get(removal.get(1)));
         results.remove(removal.get(1).intValue());
      }
      joined.add(results.get(0));
      results.remove(0);

      String t1 = joined.get(0);
      String t2 = joined.get(1);
      LogicalOperator op1 = first2.remove(0);
      LogicalOperator op2 = first2.remove(0);
      PhysicalOperator leftp = op1.accept(this,i+1, false);
      ArrayList<String> tempCols1 = new ArrayList<>(columns);
      PhysicalOperator rightp = op2.accept(this,i+1, false);
      ArrayList<String> tempCols2 = new ArrayList<>(columns);
      tempCols2.removeAll(tempCols1);

      boolean bnlj = !op.getUsableExpressionMap().containsKey(new ReversiblePair<>(t1, t2));
      PhysicalJoinOperator headOp;
      Expression e;
      ReversiblePair<String, String> pair = new ReversiblePair<>(t1, t2);
      if (!op.getResidualExpressionMap().containsKey(pair) && !op.getUsableExpressionMap()
            .containsKey(pair)) {
         e = null;
      } else if (!op.getUsableExpressionMap().containsKey(pair)) {
         e = op.getResidualExpressionMap().remove(pair);
      } else if (!op.getResidualExpressionMap().containsKey(pair)) {
         e = op.getUsableExpressionMap().remove(pair);
      } else {
         Expression tmp1 = op.getUsableExpressionMap().remove(pair);
         Expression tmp2 = op.getResidualExpressionMap().remove(pair);
         e = new AndExpression(tmp1, tmp2);
      }

      if (bnlj)
         headOp = new PhysicalBNLJOperator(leftp, rightp, e, columns);
      else {
         GetColumnSortOrderVisitor visitor = new GetColumnSortOrderVisitor(tempCols1, tempCols2);
         e.accept(visitor);
         PhysicalExternalSortOperator sortedLeft = new PhysicalExternalSortOperator(leftp,tempCols1,visitor.getColsLeftOrdering());
         PhysicalExternalSortOperator sortedRight = new PhysicalExternalSortOperator(rightp,tempCols2,visitor.getColsRightOrdering());
         headOp = new PhysicalSMJOperator(sortedLeft, sortedRight, visitor.getColsLeftIndices(), visitor.getColsRightIndices());
      }

      for (String s : results) {
         for (LogicalOperator lo : ops) {
            if (lo.getTable().getAlias() != null && lo.getTable().getAlias().equals(s) || lo.getTable().getAlias() == null && lo.getTable().getName().equals(s)) {
               tempCols1 = new ArrayList<>(columns);
               rightp = lo.accept(this,i+1, false);
               tempCols2 = new ArrayList<>(columns);
               tempCols2.removeAll(tempCols1);

               Pair<Expression, Integer> val = getJoins(joined, s, op.getUsableExpressionMap(), op.getResidualExpressionMap());
               if (val.getValue() == 0)
                  headOp = new PhysicalBNLJOperator(headOp, rightp, val.getKey(),columns);
               else {
                  GetColumnSortOrderVisitor visitor = new GetColumnSortOrderVisitor(tempCols1, tempCols2);
                  val.getKey().accept(visitor);
                  PhysicalExternalSortOperator sortedLeft = new PhysicalExternalSortOperator(headOp,tempCols1,visitor.getColsLeftOrdering());
                  PhysicalExternalSortOperator sortedRight = new PhysicalExternalSortOperator(rightp,tempCols2,visitor.getColsRightOrdering());
                  headOp = new PhysicalSMJOperator(sortedLeft, sortedRight, visitor.getColsLeftIndices(), visitor.getColsRightIndices());
               }
               joined.add(s);
               break;
            }
         }
      }
      if (!reorder) {
         addToProperties(op, i);
         return headOp;
      } else {
         ArrayList<String> cols = new ArrayList<>(columns);
         columns = new ArrayList<>();
         for (LogicalOperator operator : ops) {
            columns.addAll(operator.getColumns());
         }

         try {
            for (int x = 0; x < i; x++)
               writer.write("-");
            writer.write("Project" + columns + "\n");
            writer.flush();
         } catch (Exception exception) {
            exception.printStackTrace();
         }
         addToProperties(op, i - 1);

         return new PhysicalProjectOperator(headOp, cols, columns);
      }
   }

   private Pair<Expression, Integer> getJoins(ArrayList<String> joined, String newTable,
                                              HashMap<ReversiblePair<String, String>, Expression> usable, HashMap<ReversiblePair<String, String>, Expression> residual) {
      Expression result = null;
      int smj = 1;
      for (String s : joined) {
         ReversiblePair<String, String> pair = new ReversiblePair<>(s, newTable);
         Expression tmp;

         if (!usable.containsKey(pair) && !residual.containsKey(pair)) {
            tmp = null;
         } else if (!usable.containsKey(pair)) {
            tmp = residual.remove(pair);
            smj = 0;
         } else if (!residual.containsKey(pair)) {
            tmp = usable.remove(pair);
         } else {
            Expression tmp1 = usable.remove(pair);
            Expression tmp2 = residual.remove(pair);
            smj = 0;
            tmp = new AndExpression(tmp1, tmp2);
         }

         if (tmp != null) {
            if (result == null)
               result = tmp;
            else
               result = new AndExpression(result, tmp);
         }
      }

      return new Pair<>(result, smj);
   }

   @Override
   public PhysicalOperator visit(LogicalProjectOperator op, int i, boolean NeedToPrint) {
      addToProperties(op, i);
      PhysicalOperator childOp = op.getOperator().accept(this, i + 1, true);
      ArrayList<String> cols = new ArrayList<>(columns);
      columns = op.getSelectedColumns();
      return new PhysicalProjectOperator(childOp, cols, columns);
   }

   @Override
   public PhysicalOperator visit(LogicalScanOperator op, int i, boolean NeedToPrint) {
      if (NeedToPrint)
         addToProperties(op, i);
      columns.addAll(DBCatalog.getColumns(op.getTable().getName(), op.getTable().getAlias()));
      return new PhysicalScanOperator(op.getTable());
   }

   @Override
   public PhysicalOperator visit(LogicalSelectOperator op, int i, boolean NeedToPrint) {
      if (NeedToPrint)
         addToProperties(op, i);
      ArrayList<String> cols = DBCatalog.getColumns(op.getTable().getName(), op.getTable().getAlias());
      columns.addAll(cols);

      PhysicalOperator childOp;
      Expression expr = op.getFullExpression();
      Table table = op.getTable();

      String alias = (table.getAlias() != null) ? table.getAlias() : table.getName();

      HashSet<SelectionCondition> indices = op.getUsableSelectCondition();
      // table could have multiple select conditions that could use an index
      for (SelectionCondition sc : indices) {
         SelectIndex indexInfo = sc.getSelectIndex();
         // select condition could have multiple columns that could use an index
         for (String col : sc.getColumns()) {
            if (col.split("\\.")[0].equals(alias)) {

               // check if index exists
               Index index = getIndex(table.getName(), col.split("\\.")[1]);
               if (index != null) {
                  // check if it has less cost to use index
                  if (useIndex(index, indexInfo, col, table)) {

                     // equality index
                     if (indexInfo.getEquality() != null) {
                        childOp = new PhysicalIndexScanOperator(new PhysicalScanOperator(table),
                                cols, cols.indexOf(col), indexInfo.getEquality(), indexInfo
                                .getEquality() + 1, index.isClustered(), index.getFileName());
                        // non-equality index
                     } else {
                        childOp = new PhysicalIndexScanOperator(new PhysicalScanOperator(table),
                                cols, cols.indexOf(col), indexInfo.getMin(), indexInfo
                                .getMax(), index.isClustered(), index.getFileName());
                     }

                     // check if other select conditions not in index
                     expr = op.getResidualExpression();
                     if (expr == null) {
                        return childOp;
                     }
                     return new PhysicalSelectOperator(childOp, cols, expr);
                  }
               }
            }
         }
      }

      // if index doesn't apply
      childOp = new PhysicalScanOperator(table);
      return new PhysicalSelectOperator(childOp, cols, expr);
   }

   /**
    * evaluate cost based on table and index sizes whether to use index or plain selection
    *
    * @param index
    * @param indexInfo
    * @param col
    * @param table
    * @return whether we need to use index or selection
    */
   private boolean useIndex(Index index, SelectIndex indexInfo, String col, Table table) {
      double costScan, costIndex = 0;
      col = col.split("\\.")[1];
      int t = tableInfo.get(table.getName()).numTuples; // the number of tuples

      // calculate cost for scan
      // asking database catalog how many tuples the base table has
      // multiplying by the size of one tuple
      // and dividing that by the page size of 4096 bytes
      costScan = t / getPageSize(tableInfo.get(table.getName()).columnInfo.size());
      // calculate cost for index

      // p is the number of pages in the relation
      // t the number of tuples
      // r the reduction factor : the range of value
      // l the number of leaves in the index
      // the cost for a clustered index is 3 + p * r
      // while for an unclustered index it is 3 + l * r + t * r.

      // calculating the reduction factor
      double r;
      Integer lowKey = indexInfo.getMin();
      Integer highKey = indexInfo.getMax();
      Integer equality = indexInfo.getEquality();
      col = col.split("\\.").length == 1 ? col : col.split("\\.")[1];
      int tableRange = tableInfo.get(table.getName()).getMax(col) - tableInfo.get(table.getName()).getMin(col);
      if (lowKey == null){
         // low key not exists
         lowKey = tableInfo.get(table.getName()).getMin(col);
      }
      if (highKey == null){
         // high key not exists
         highKey = tableInfo.get(table.getName()).getMax(col);
      }
      if (equality != null) {
         r = 1 / tableRange;
      } else {
         r = (highKey - lowKey) / tableRange;
      }

      // clustered
      if (index.isClustered()){
         int p = t / getPageSize(tableInfo.get(table.getName()).columnInfo.size());
         costIndex = 3 + p * r;
      }// unclustered
      else {
         int l; // l the number of leaves in the index

         try {
            File file = new File(index.getFileName());
            ByteBuffer buffer = ByteBuffer.allocate(getPAGE_SIZE());

            FileInputStream inputStream = new FileInputStream(file);
            FileChannel fileChannel = inputStream.getChannel();
            fileChannel.position(0);
            fileChannel.read(buffer);

            buffer.flip();
            buffer.getInt();  // get rootId
            l = buffer.getInt();
            buffer.clear();
            inputStream.close();

            costIndex = 3 + l * r + t * r;
         } catch (java.io.IOException e) {
            e.printStackTrace();
         }
      }
      return costIndex < costScan;
   }

   @Override
   public PhysicalOperator visit(LogicalSortOperator op, int i, boolean NeedToPrint) {
      addToProperties(op, i);
      PhysicalOperator childOp = op.getOperator().accept(this, i + 1, true);
      return new PhysicalExternalSortOperator(childOp, columns, op.getSortBy());
   }

   /**
    * Method to add the operator to the logical properties
    */
   private void addToProperties(LogicalOperator o, int dashes) {
      try {
         for (int i = 0; i < dashes; i++)
            writer.write("-");
         if (o instanceof LogicalJoinOperator){
            ((LogicalJoinOperator) o).printDashesNumber = dashes;
         }
         if (o instanceof LogicalSelectOperator){
            ((LogicalSelectOperator) o).printDashesNumber = dashes;
         }
         writer.write(o.toString());
         writer.write("\n");
         writer.flush();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}
