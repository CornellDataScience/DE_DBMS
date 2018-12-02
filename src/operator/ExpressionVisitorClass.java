package operator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;

import DBSystem.Table;
import DBSystem.Tuple;
import DBSystem.dbCatalog;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ExpressionVisitorClass implements ExpressionVisitor{
	
	/**
	 * Class for the Expression class that can visit the expression recursively
	 * to find out the whether the condition is true or not, what it differs
	 * from ExpressionVisitorV1 is that it only takes in one table for column
	 * reference, while V1 will take two
	 * 
	 * The logic is based on recursion and visitor pattern. We start the
	 * function by visiting the root node. Then for every kind of node, we add
	 * its data(boolean or number) to the result list, and then visit its
	 * children nodes if it has them.
	 * 
	 * @author Rong Tan (rt389) Akhil Gopu (akg68)
	 */

	private Stack<Boolean> booleanStack;
	private Stack<Double> numberStack;
	private Tuple t; // running string representation
	public boolean status = false;
	private HashSet<String> tableName = new HashSet<String>();
	private HashMap<String, Tuple> map = new HashMap<String, Tuple>();

	public ExpressionVisitorClass(String name) {
		booleanStack = new Stack<Boolean>();
		numberStack = new Stack<Double>();
		tableName.add(name);
	}
	
	public ExpressionVisitorClass(String name, Tuple t,  String name2, Tuple t2) {
		booleanStack = new Stack<Boolean>();
		numberStack = new Stack<Double>();
		tableName.add(name);
		tableName.add(name2);
		map.put(name, t);
		map.put(name2, t2);
	}
	/**
	 * Method to get the result boolean when visitor is done
	 * 
	 * @return whether the expression is evaluated as true or false
	 */
	public boolean evaluate() {
		return booleanStack.peek();
	}

	/**
	 * Method to set the Tuple to be visited in the Visitor
	 * 
	 * @param the
	 *            Tuple to evaluate
	 */
	public void setT(Tuple t) {
		this.t = t;
	}
	
	/**
	 * Method to attach the evaluation of AndExpression depending on the left
	 * and right side
	 * 
	 * @param the
	 *            AndExpression to evaluate
	 */
	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		boolean tmp2 = booleanStack.pop();
		boolean tmp1 = booleanStack.pop();
		booleanStack.push(tmp1 && tmp2);
	}

	/**
	 * Method to attach the value of the Expression to NumberStack
	 * 
	 * @param the
	 *            Column to evaluate
	 */
	@Override
	public void visit(Column arg0) {
		String name = arg0.getTable().getName();
		if (tableName.contains(name) == false)
		{
			status = true;
			numberStack.push(0.0);
			return;
		}
		if (tableName.contains(name) || (tableName.contains(dbCatalog.getAlias(name).getName())))
		{
			String colName = arg0.getColumnName();
			Table table = dbCatalog.getTable(name);
			if (table == null)
				table = dbCatalog.getAlias(name);
			if (map.isEmpty())
			{	
				// TODO visit specific value in column
//				String specificValue = (String) table.getTupleSpecific(t, colName);
//				Double value = Double.valueOf(specificValue);
//				numberStack.push(value);
			}
			else
			{	
				// TODO visit specific value in column
//				Tuple t = map.get(name);
//				if (t == null)
//					t = map.get(dbCatalog.getAlias(name).getName());
//				String specificValue = (String) table.getTupleSpecific(t, colName);
//				Double value = Double.valueOf(specificValue);
//				numberStack.push(value);
			}
		}
		else
		{
			status = true;
			numberStack.push(0.0);
		}
		return;
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
	}

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
		double tmp2 = numberStack.pop();
		double tmp1 = numberStack.pop();
		booleanStack.push(tmp1 == tmp2);
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
		booleanStack.push(tmp1 != tmp2);
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
		booleanStack.push(tmp1 > tmp2);
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
		booleanStack.push(tmp1 >= tmp2);
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
		booleanStack.push(tmp1 < tmp2);
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
		booleanStack.push(tmp1 <= tmp2);
	}

	// Methods which do not need implementation

	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub

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
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub

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
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(OrExpression arg0) {
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

	public List<Integer> operate() {
		// TODO Auto-generated method stub
		return null;
	}

}
