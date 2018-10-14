package Project2.Visitors;


import javafx.util.Pair;
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
public class JoinExpressionVisitor implements ExpressionVisitor {


    private ArrayList<Pair<String, String>> joins;
    private Stack<String> chars;

    /**
     * Creates a visitor capable of evaluating a where expression
     *
     */
    public JoinExpressionVisitor() {
       joins = new ArrayList<>();
       chars = new Stack<>();
    }

    /**
     * Returns value of expression, can be called multiple times
     *
     * @return boolean of whether the tuple passes the expression
     */
    public ArrayList<Pair<String, String>> getResult() {
        return joins;
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
        chars.push(stringValue.getValue());
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
        Pair<String, String> newJoin = new Pair<>(chars.pop(),chars.pop());
        joins.add(newJoin);
    }

    @Override
    public void visit(GreaterThan greaterThan) {throw new UnsupportedOperationException();}

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {throw new UnsupportedOperationException();}

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
    public void visit(MinorThan minorThan) {throw new UnsupportedOperationException();}

    @Override
    public void visit(MinorThanEquals minorThanEquals) {throw new UnsupportedOperationException();}

    @Override
    public void visit(NotEqualsTo notEqualsTo) { throw new UnsupportedOperationException(); }


    @Override
    public void visit(Column column) {
        chars.push(column.toString());
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
