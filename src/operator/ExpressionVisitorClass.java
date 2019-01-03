package operator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import DBSystem.Table;
import DBSystem.ColumnTab;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ExpressionVisitorClass implements ExpressionVisitor {

	/**
	 * Class for the Expression class that can visit the expression recursively to
	 * find out the whether the condition is true or not, what it differs from
	 * ExpressionVisitorV1 is that it only takes in one table for column reference,
	 * while V1 will take two
	 * 
	 * The logic is based on recursion and visitor pattern. We start the function by
	 * visiting the root node. Then for every kind of node, we add its data(boolean
	 * or number) to the result list, and then visit its children nodes if it has
	 * them.
	 * 
	 * @author Rong Tan (rt389)
	 */

	Expression expression;
	Table table;
	private Stack<HashSet<Integer>> indicesStack;
	private Stack<Double> numberStack;
	private Stack<Integer> ColumnOrNumberStack;
	// 0 means a number, 1 means column
	private Stack<ColumnTab> columnStack;

	public ExpressionVisitorClass(Expression expression, Table table) {
		this.expression = expression;
		this.table = table;
		indicesStack = new Stack<HashSet<Integer>>();
		numberStack = new Stack<Double>();
		ColumnOrNumberStack = new Stack<Integer>();
		columnStack = new Stack<ColumnTab>();
	}

	/**
	 * Method to get the set of all the ids of the tuples in the column when
	 * evaluated
	 * 
	 * @return whether hashset containing all the ids of tuples evaluated
	 */
	public Set<Integer> evaluate() {
		return indicesStack.peek();
	}

	/**
	 * Method to push the Column to the column stack
	 * 
	 * @param the
	 *            Column to evaluate
	 */
	@Override
	public void visit(Column arg0) {
		ColumnTab col = table.getCol(arg0.getColumnName());
		columnStack.push(col);
		ColumnOrNumberStack.push(1);
	}

	/**
	 * Method to attach the evaluation of AndExpression depending on the left and
	 * right side
	 * 
	 * @param the
	 *            AndExpression to evaluate
	 */
	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		HashSet<Integer> tmp2 = indicesStack.pop();
		HashSet<Integer> tmp1 = indicesStack.pop();
		// find the intersection of two indices set
		HashSet<Integer> intersection = new HashSet<Integer>(tmp1);
		intersection.retainAll(tmp2);
		indicesStack.push(intersection);
	}

	/**
	 * Method to attach the evaluation of OrExpression depending on the left and
	 * right side
	 * 
	 * @param the
	 *            OrExpression to evaluate
	 */
	@Override
	public void visit(OrExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		HashSet<Integer> tmp2 = indicesStack.pop();
		HashSet<Integer> tmp1 = indicesStack.pop();
		// find the union of two indices set
		HashSet<Integer> intersection = new HashSet<Integer>(tmp1);
		intersection.addAll(tmp2);
		indicesStack.push(intersection);
	}

	/**
	 * Method to attach the value of the Expression to NumberStack
	 * 
	 * @param the
	 *            LongValue to evaluate
	 */
	@Override
	public void visit(LongValue arg0) {
		numberStack.push(arg0.toDouble());
		ColumnOrNumberStack.push(0);
	}

	/**
	 * Method to use the two values and a compare method to compare two values
	 * compare method 0: equals =
	 * 				  1: not equals !=
	 * 				  2: greater than >
	 * 				  3: greater than equals >=
	 * 				  4: minor than <
	 * 				  5: minor than equals <=
	 * @param d1,
	 *            d2 indicating two values compareMethod indicating which comparison
	 *            method we are using
	 */
	public boolean compareValue(double d1, double d2, int compareMethod) {
		boolean result = false;
		switch (compareMethod) {
		case 0:
			result = d1 == d2;
			break;
		case 1:
			result = d1 != d2;
			break;
		case 2:
			result = d1 > d2;
			break;
		case 3:
			result = d1 >= d2;
			break;
		case 4:
			result = d1 < d2;
			break;
		case 5:
			result = d1 <= d2;
			break;
		default:
			System.out.println("compare value function error");
			break;
		}
		return result;
	}

	/**
	 * Method to use the two CoN and a compare method to push the result indices to
	 * indicesStack
	 * 
	 * @param CoN1,
	 *            CoN2 indicating whether the two parameters at the top of the
	 *            stacks are column or number compareMethod indicating which
	 *            comparison method we are using
	 */
	public void compareHelper(int CoN1, int CoN2, int compareMethod) {
		HashSet<Integer> result = new HashSet<Integer>();
		// check if both are number
		if (CoN1 == 0 && CoN2 == 0) {
			double tmp2 = numberStack.pop();
			double tmp1 = numberStack.pop();
			if (compareValue(tmp1, tmp2, compareMethod)) {
				// create a new set containing all the ids
				for (int i = 0; i < table.getTupleNum(); i++)
					result.add(i);
				indicesStack.push(result);
			} else {
				// create a new set containing no id
				indicesStack.push(result);
			}
			// CoN1 is number, CoN2 is column; or CoN1 is column, CoN2 is number
		} else if ((CoN1 == 0 && CoN2 == 1) || (CoN1 == 1 && CoN2 == 0)) {
			double tmp = numberStack.pop();
			ColumnTab columnToCompare = columnStack.pop();
			// get all the indices of tuples which value in columnToCompare equals to tmp1
			for (int i = 0; i < columnToCompare.getSize(); i++) {
				if (compareValue((Double) columnToCompare.getData(i) , tmp, compareMethod)) {
					result.add(i);
				}
			}
			indicesStack.push(result);
			// CoN1 is column, CoN2 is column
		} else if (CoN1 == 1 && CoN2 == 1) {
			ColumnTab column1 = columnStack.pop();
			ColumnTab column2 = columnStack.pop();
			for (int i = 0; i < column1.getSize(); i++) {
				if (compareValue(column1.getData(i), column2.getData(i), compareMethod)) {
					result.add(i);
				}
			}
			indicesStack.push(result);
		}
	}

	/**
	 * cases: a.id == 2 a.id + 2 == 2 a.id == a.gpa a.id + 2 == a.gpa + 2 a.id *
	 * a.gpa == 2
	 */
	/**
	 * Method to attach the evaluation of EqualsTo expression
	 * 
	 * @param the
	 *            EqualsTo to evaluate
	 */
	@Override
	public void visit(EqualsTo arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int CoN2 = ColumnOrNumberStack.pop();
		int CoN1 = ColumnOrNumberStack.pop();
		compareHelper(CoN1, CoN2, 0);
	}

	/**
	 * Method to attach the evaluation of NotEqualsTo expression
	 * 
	 * @param the
	 *            NotEqualsTo to evaluate
	 */
	@Override
	public void visit(NotEqualsTo arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int CoN2 = ColumnOrNumberStack.pop();
		int CoN1 = ColumnOrNumberStack.pop();
		compareHelper(CoN1, CoN2, 1);
	}

	/**
	 * Method to attach the evaluation of GreaterThan expression
	 * 
	 * @param the
	 *            GreaterThan to evaluate
	 */
	@Override
	public void visit(GreaterThan arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int CoN2 = ColumnOrNumberStack.pop();
		int CoN1 = ColumnOrNumberStack.pop();
		compareHelper(CoN1, CoN2, 2);
	}

	/**
	 * Method to attach the evaluation of GreaterThanEquals expression
	 * 
	 * @param the
	 *            GreaterThanEquals to evaluate
	 */
	@Override
	public void visit(GreaterThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int CoN2 = ColumnOrNumberStack.pop();
		int CoN1 = ColumnOrNumberStack.pop();
		compareHelper(CoN1, CoN2, 3);
	}

	/**
	 * Method to attach the evaluation of MinorThan expression
	 * 
	 * @param the
	 *            MinorThan to evaluate
	 */
	@Override
	public void visit(MinorThan arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int CoN2 = ColumnOrNumberStack.pop();
		int CoN1 = ColumnOrNumberStack.pop();
		compareHelper(CoN1, CoN2, 4);
	}

	/**
	 * Method to attach the evaluation of MinorThanEquals expression
	 * 
	 * @param the
	 *            MinorThanEquals to evaluate
	 */
	@Override
	public void visit(MinorThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int CoN2 = ColumnOrNumberStack.pop();
		int CoN1 = ColumnOrNumberStack.pop();
		compareHelper(CoN1, CoN2, 5);
	}

	// Methods which do not need implementation

	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DoubleValue arg0) {
		// TODO

	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Division arg0) {
		// TODO

	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO

	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO

	}

	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub

	}

}
