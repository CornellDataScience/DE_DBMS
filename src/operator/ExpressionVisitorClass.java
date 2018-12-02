package operator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Stack;

import DBSystem.Table;
import DBSystem.Column;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
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
	// 0 means a column, 1 means original column from table, 2 means modified column in ModifiedColumnStack
	private Stack<Column> columnStack;

	public ExpressionVisitorClass(Expression expression, Table table) {
		this.expression = expression;
		this.table = table;
		indicesStack = new Stack<HashSet<Integer>>();
	}

	/**
	 * Method to get the set of all the ids of the tuples in the column when
	 * evaluated
	 * 
	 * @return whether hashset containing all the ids of tuples evaluated
	 */
	public HashSet<Integer> evaluate() {
		return indicesStack.peek();
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
	 *            Column to evaluate
	 */
	public void visit(Column arg0) {
		
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
	 * cases:
	 * a.id == 2
	 * a.id + 2 == 2
	 * a.id == a.gpa
	 * a.id + 2 == a.gpa + 2
	 * a.id * a.gpa == 2
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
		HashSet<Integer> result = new HashSet<Integer>();
		// check if both are number
		if (CoN1 == 0 && CoN2 == 0) {
			double tmp2 = numberStack.pop();
			double tmp1 = numberStack.pop();
			if (tmp1 == tmp2) {
				// create a new set containing all the ids 
				for (int i = 0; i < table.getTupleNum(); i++)
					result.add(i);
				indicesStack.push(result);
			} else {
				// create a new set containing no id
				indicesStack.push(result);
			}
		// CoN1 is number, CoN2 is column
		} else if (CoN1 == 0 && CoN2 == 1) {
			double tmp1 = numberStack.pop();
			Column columnToCompare = columnStack.pop();
			// get all the indices of tuples which value in columnToCompare equals to tmp1
			for (int i = 0; i < columnToCompare.getSize(); i++) {
				if (columnToCompare.getData(i) instanceof Double && (Double)columnToCompare.getData(i) == tmp1) {
					result.add(i);
				}
			}
			indicesStack.push(result);
		} 
		// TODO if one of them is column?
		// return all the indices that matches the boolean value
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
		double tmp2 = numberStack.pop();
		double tmp1 = numberStack.pop();
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
		double tmp2 = numberStack.pop();
		double tmp1 = numberStack.pop();
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
		double tmp2 = numberStack.pop();
		double tmp1 = numberStack.pop();
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
		double tmp2 = numberStack.pop();
		double tmp1 = numberStack.pop();
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
		double tmp2 = numberStack.pop();
		double tmp1 = numberStack.pop();
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

	@Override
	public void visit(net.sf.jsqlparser.schema.Column arg0) {
		// TODO Auto-generated method stub
		
	}

}
