import Project2.ReversiblePair;
import Project2.Visitors.SelectExpressionVisitor;
import Project2.Visitors.SelectionCondition;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.Test;

import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the SelectExpressionVisitor class
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class SelectExpressionVisitorTest {

   private HashMap<String, HashSet<SelectionCondition>> unionFind;
   private HashMap<ReversiblePair<String, String>, Expression> usableJoinConditions;
   private HashMap<ReversiblePair<String, String>, Expression> residualJoinConditions;
   private HashMap<String, Expression> residualSelectConditions;
   private HashSet<SelectionCondition> unionFindSet;

   private void setUp(String query) throws ParseException {
      CCJSqlParser parser = new CCJSqlParser(new StringReader(query));
      Statement statement = parser.Statement();
      PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();

      // union find parsing logic
      unionFind = new HashMap<>();
      usableJoinConditions = new HashMap<>();
      residualJoinConditions = new HashMap<>();
      residualSelectConditions = new HashMap<>();
      unionFindSet = new HashSet<>();

      Expression expression = select.getWhere();
      if (expression != null) {
         SelectExpressionVisitor visitor = new SelectExpressionVisitor();
         expression.accept(visitor);

         unionFind = visitor.getUnionFind();
         usableJoinConditions = visitor.getUsableJoinConditions();
         residualJoinConditions = visitor.getResidualJoinConditions();
         residualSelectConditions = visitor.getResidualSelectConditions();
         unionFindSet = visitor.getUnionFindSet();
      }
   }

   @Test
   public void testNoSelect() throws ParseException {
      setUp("SELECT * FROM A;");

      assertTrue(unionFind.isEmpty());
      assertTrue(usableJoinConditions.isEmpty());
      assertTrue(residualJoinConditions.isEmpty());
      assertTrue(residualSelectConditions.isEmpty());
      assertTrue(unionFindSet.isEmpty());
   }

   @Test
   public void testSimpleUsableSelect() throws ParseException {
      setUp("SELECT * FROM A WHERE A.A > 5;");

      assertTrue(unionFind.size() == 1);
      assertTrue(unionFind.get("A").size() == 1);
      for (SelectionCondition selectionCondition : unionFind.get("A")) {
         assertTrue(selectionCondition.getColumns().size() == 1 && selectionCondition.getColumns().get(0).equals("A.A"));
         assertTrue(selectionCondition.getSelectIndex().getEquality() == null && selectionCondition.getSelectIndex().getMin() == 6 && selectionCondition.getSelectIndex().getMax() == null);
         assertNotNull(selectionCondition.getExpression());
         assertTrue(unionFindSet.contains(selectionCondition));
      }

      assertTrue(usableJoinConditions.isEmpty());
      assertTrue(residualJoinConditions.isEmpty());
      assertTrue(residualSelectConditions.isEmpty());

      assertTrue(unionFindSet.size() == 1);
   }

   @Test
   public void testAliasSelect() throws ParseException {
      setUp("SELECT * FROM TableA A WHERE A.A > 5;");

      assertTrue(unionFind.size() == 1);
      assertTrue(unionFind.get("A").size() == 1);
      for (SelectionCondition selectionCondition : unionFind.get("A")) {
         assertTrue(selectionCondition.getColumns().size() == 1 && selectionCondition.getColumns().get(0).equals("A.A"));
         assertTrue(selectionCondition.getSelectIndex().getEquality() == null && selectionCondition.getSelectIndex().getMin() == 6 && selectionCondition.getSelectIndex().getMax() == null);
         assertNotNull(selectionCondition.getExpression());
         assertTrue(unionFindSet.contains(selectionCondition));
      }

      assertTrue(usableJoinConditions.isEmpty());
      assertTrue(residualJoinConditions.isEmpty());
      assertTrue(residualSelectConditions.isEmpty());

      assertTrue(unionFindSet.size() == 1);
   }

   @Test
   public void testSelectSameTable() throws ParseException {
      setUp("SELECT * FROM TableA A WHERE A.A >= A.B AND A.A != A.C;");

      assertTrue(unionFind.isEmpty());
      assertTrue(usableJoinConditions.isEmpty());
      assertTrue(residualJoinConditions.isEmpty());

      assertTrue(residualSelectConditions.size() == 1);
      assertTrue(residualSelectConditions.get("A") instanceof AndExpression);

      assertTrue(unionFindSet.isEmpty());
   }

   @Test
   public void testSimpleUnusableSelect() throws ParseException {
      setUp("SELECT * FROM A WHERE A.A != 5;");

      assertTrue(unionFind.isEmpty());
      assertTrue(usableJoinConditions.isEmpty());
      assertTrue(residualJoinConditions.isEmpty());

      assertTrue(residualSelectConditions.size() == 1);

      assertTrue(unionFindSet.isEmpty());
   }

   @Test
   public void testMultipleUnusableSelects() throws ParseException {
      setUp("SELECT * FROM A WHERE A.A != 5 AND A.A != 7 AND A.C != 10;");

      assertTrue(unionFind.isEmpty());
      assertTrue(usableJoinConditions.isEmpty());
      assertTrue(residualJoinConditions.isEmpty());

      assertTrue(residualSelectConditions.size() == 1);
      assertTrue(residualSelectConditions.get("A") instanceof AndExpression);

      assertTrue(unionFindSet.isEmpty());

   }

   @Test
   public void testMultipleUsableSelects() throws ParseException {
      setUp("SELECT * FROM A WHERE A.A >= 5 AND 20 >= A.A AND A.B = 20;");

      assertTrue(unionFind.size() == 1);
      assertTrue(unionFind.get("A").size() == 2);
      for (SelectionCondition selectionCondition : unionFind.get("A")) {
         if (selectionCondition.getColumns().contains("A.A")) {
            assertTrue(selectionCondition.getColumns().size() == 1 && selectionCondition.getColumns().get(0).equals("A.A"));
            assertTrue(selectionCondition.getSelectIndex().getEquality() == null && selectionCondition.getSelectIndex().getMin() == 5 && selectionCondition.getSelectIndex().getMax() == 21);
            assertTrue(selectionCondition.getExpression() instanceof AndExpression);
            assertTrue(unionFindSet.contains(selectionCondition));
         } else {
            assertTrue(selectionCondition.getColumns().size() == 1 && selectionCondition.getColumns().get(0).equals("A.B"));
            assertTrue(selectionCondition.getSelectIndex().getEquality() == 20 && selectionCondition.getSelectIndex().getMin() == null && selectionCondition.getSelectIndex().getMax() == null);
            assertNotNull(selectionCondition.getExpression());
            assertTrue(unionFindSet.contains(selectionCondition));
         }
      }

      assertTrue(usableJoinConditions.isEmpty());
      assertTrue(residualJoinConditions.isEmpty());
      assertTrue(residualSelectConditions.isEmpty());

      assertTrue(unionFindSet.size() == 2);
   }

   @Test
   public void testMixedSelect1() throws ParseException {
      setUp("SELECT * FROM A WHERE A.A != 5 AND A.A > 20 AND A.A < 30;");

      assertTrue(unionFind.size() == 1);
      assertTrue(unionFind.get("A").size() == 1);
      for (SelectionCondition selectionCondition : unionFind.get("A")) {
         assertTrue(selectionCondition.getColumns().size() == 1 && selectionCondition.getColumns().get(0).equals("A.A"));
         assertTrue(selectionCondition.getSelectIndex().getEquality() == null && selectionCondition.getSelectIndex().getMin() == 21 && selectionCondition.getSelectIndex().getMax() == 30);
         assertTrue(selectionCondition.getExpression() instanceof AndExpression);
         assertTrue(unionFindSet.contains(selectionCondition));
      }

      assertTrue(usableJoinConditions.isEmpty());
      assertTrue(residualJoinConditions.isEmpty());

      assertTrue(residualSelectConditions.size() == 1);

      assertTrue(unionFindSet.size() == 1);
   }

   @Test
   public void testMixedSelect2() throws ParseException {
      setUp("SELECT * FROM A WHERE A.B != 5 AND A.A > 20 AND A.A <= 30;");

      assertTrue(unionFind.size() == 1);
      assertTrue(unionFind.get("A").size() == 1);
      for (SelectionCondition selectionCondition : unionFind.get("A")) {
         assertTrue(selectionCondition.getColumns().size() == 1 && selectionCondition.getColumns().get(0).equals("A.A"));
         assertTrue(selectionCondition.getSelectIndex().getEquality() == null && selectionCondition.getSelectIndex().getMin() == 21 && selectionCondition.getSelectIndex().getMax() == 31);
         assertTrue(selectionCondition.getExpression() instanceof AndExpression);
         assertTrue(unionFindSet.contains(selectionCondition));
      }

      assertTrue(usableJoinConditions.isEmpty());
      assertTrue(residualJoinConditions.isEmpty());

      assertTrue(residualSelectConditions.size() == 1);

      assertTrue(unionFindSet.size() == 1);
   }

   @Test
   public void testJoinNoSelect() throws ParseException {
      setUp("SELECT * FROM A, B;");

      assertTrue(unionFind.isEmpty());
      assertTrue(usableJoinConditions.isEmpty());
      assertTrue(residualJoinConditions.isEmpty());
      assertTrue(residualSelectConditions.isEmpty());
      assertTrue(unionFindSet.isEmpty());
   }
}
