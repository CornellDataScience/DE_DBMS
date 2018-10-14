package Project2;

import Project2.Operators.Logical.*;
import Project2.Operators.Physical.*;
import Project2.Visitors.SelectExpressionVisitor;
import Project2.Visitors.SelectIndex;
import Project2.Visitors.SelectionCondition;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static Project2.DBCatalog.getColumns;

/**
 * Takes care of the logic to create both the logical and physical SQL query tree
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class SQLQueryPlan {

	private LogicalOperator logicalHead;
	private PhysicalOperator physicalHead;
	private BufferedWriter bw2; // write buffer for physical operator prop
	/**
	 * Creates a new SQL query plan with the head of the tree in head
	 *
	 * @param statement the statement to parse into the tree plan
	 */
	public SQLQueryPlan(Statement statement, String tmpOutput, int counter) throws IOException {
		// assign parameters to variables, cast
		PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();
		Table table = (Table) select.getFromItem();
		String tableName = table.getName();
		// start tables list
		ArrayList<Table> tables = new ArrayList<>();
		tables.add(table);

		// set name (alias or not) for table
		String alias = getAlias(table);

		// start col lists
		ArrayList<String> columns = getColumns(tableName, alias);

		// expand filenames and cols to all join tables
		Table joinTable;
		String joinTableName;
		if (select.getJoins() != null) {
			for (Object join : select.getJoins()) {
				joinTable = (Table) ((Join) join).getRightItem();
				joinTableName = joinTable.getName();
				// add table
				tables.add(joinTable);

				// add to cols
				columns.addAll(getColumns(joinTableName, getAlias(joinTable)));
			}
		}

		// get sort columns for orderBy
		ArrayList<String> sortColumns = new ArrayList<>();
		if (select.getOrderByElements() != null) {
			for (Object item : select.getOrderByElements()) {
				sortColumns.add(((Column) ((OrderByElement) item).getExpression()).getWholeColumnName());

			}
		}

		// get projected columns for project
		ArrayList<String> selectedColumns = new ArrayList<>();
		if (!(select.getSelectItems().get(0) instanceof AllColumns)) {
			for (Object item : select.getSelectItems()) {
				selectedColumns.add(((Column) ((SelectExpressionItem) item).getExpression()).getWholeColumnName());

			}
		} else {
			// if no project, then all columns are projected - used only on
			// distinct w/ no project
			selectedColumns.addAll(columns);
		}

		// start of logic to build query plan - logical
		// union find parsing logic
		HashMap<String, HashSet<SelectionCondition>> unionFind = new HashMap<>();
		HashMap<ReversiblePair<String, String>, Expression> usableJoinConditions = new HashMap<>();
		HashMap<ReversiblePair<String, String>, Expression> residualJoinConditions = new HashMap<>();
		HashMap<String, Expression> residualSelectConditions = new HashMap<>();
		HashSet<SelectionCondition> unionFindSet = new HashSet<>();

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

		// select join, select, or scan
		if (select.getJoins() != null) {
			ArrayList<LogicalOperator> operators = new ArrayList<>();
			ArrayList<String> tableNames = new ArrayList<>();
			ArrayList<String> aliases = new ArrayList<>();

			while (tables.size() != 0) {
				// removes the table
				table = tables.remove(0);
				tableNames.add(table.getName());
				alias = getAlias(table);
				aliases.add(alias);

				// get select conditions
				operators.add(getSelectConditions(table, unionFind.get(alias), residualSelectConditions.get(alias)));
			}
			logicalHead = new LogicalJoinOperator(operators, tableNames, aliases, usableJoinConditions, residualJoinConditions, unionFindSet);
		} else {
			// select
			logicalHead = getSelectConditions(table, unionFind.get(alias), residualSelectConditions.get(alias));
		}

		// project
		if (!(select.getSelectItems().get(0) instanceof AllColumns)) { // projected columns
			logicalHead = new LogicalProjectOperator(logicalHead, selectedColumns);
		}

		// since after project, all columns will only be projected columns
		// distinct
		if (select.getDistinct() != null) {
			// order by
			if (select.getOrderByElements() != null) {
				logicalHead = new LogicalSortOperator(logicalHead, sortColumns);
			} else {
				logicalHead = new LogicalSortOperator(logicalHead, selectedColumns);
			}

			logicalHead = new LogicalDuplicateEliminationOperator(logicalHead);
		} else {
			// order by
			if (select.getOrderByElements() != null) {
				logicalHead = new LogicalSortOperator(logicalHead, sortColumns);
			}
		}

		// creates the logical query plan
		File prop = new File(tmpOutput + File.separator + "query" + counter + "_logicalplan.txt");
		prop.createNewFile();
		BufferedWriter bw = new BufferedWriter(new FileWriter(prop));
		// creates the physical query plan
		File physicalprop = new File(tmpOutput + File.separator + "query" + counter + "_physicalplan.txt");
		physicalprop.createNewFile();
		bw2 = new BufferedWriter(new FileWriter(physicalprop));
		PhysicalPlanBuilder builder = new PhysicalPlanBuilder(bw);
		physicalHead = logicalHead.accept(builder, 0, true);
		bw.close();
		// print Physical Plan
		printPhysicalPlan(physicalHead, 0);
		bw2.write("\n");
		bw2.flush();
		bw2.close();
	}

	/**
	 * Creates a new SQL query plan with the head of the tree in head
	 *
	 * @param o PhysicalOperator to be printed
	 *        i number of dashes
	 */
	private void printPhysicalPlan(PhysicalOperator o, int i){
		String temp = "";
		for (int x = 0; x < i; x++){
			temp += "-";
		}
		temp += o.toString() + "\n";
		try {
			bw2.write(temp);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (o instanceof PhysicalBNLJOperator){
			printPhysicalPlan(((PhysicalBNLJOperator)o).leftOperator, i+1);
			printPhysicalPlan(((PhysicalBNLJOperator)o).rightOperator, i+1);
		} else if (o instanceof PhysicalDuplicateEliminationOperator){
			printPhysicalPlan(((PhysicalDuplicateEliminationOperator)o).operator, i+1);
		} else if (o instanceof PhysicalExternalSortOperator){
			printPhysicalPlan(((PhysicalExternalSortOperator)o).operator, i+1);
		} else if (o instanceof PhysicalIndexScanOperator){
			// do nothing
		} else if (o instanceof PhysicalInPlaceSortOperator){
			printPhysicalPlan(((PhysicalInPlaceSortOperator)o).operator, i+1);
		} else if (o instanceof PhysicalProjectOperator){
			printPhysicalPlan(((PhysicalProjectOperator)o).operator, i+1);
		} else if (o instanceof PhysicalScanOperator){
			// do nothing
		} else if (o instanceof PhysicalSelectOperator){
			printPhysicalPlan(((PhysicalSelectOperator)o).child, i+1);
		} else if (o instanceof PhysicalSMJOperator){
			printPhysicalPlan(((PhysicalSMJOperator)o).leftOperator, i+1);
			printPhysicalPlan(((PhysicalSMJOperator)o).rightOperator, i+1);
		} else if (o instanceof PhysicalTNLJOperator){
			printPhysicalPlan(((PhysicalTNLJOperator)o).leftOperator, i+1);
			printPhysicalPlan(((PhysicalTNLJOperator)o).rightOperator, i+1);
		}
	}

	/**
	 * Gets operator head of the physical query plan
	 *
	 * @return operator head
	 */
	public PhysicalOperator getHead() {
		return physicalHead;
	}

	/**
	 * Gets the alias if it exists or the table's name if not
	 *
	 * @param table the table
	 * @return the table name as used in the query
	 */
	private String getAlias(Table table) {
		if (table.getAlias() != null) {
			return table.getAlias();
		} else {
			return table.getName();
		}
	}

	private LogicalOperator getSelectConditions (Table table, HashSet<SelectionCondition> scSet, Expression residualSelectCondition) {
		HashSet<SelectionCondition> set = new HashSet<>();
		if (scSet != null) {
			for (SelectionCondition sc : scSet) {
				if (sc.getColumns().size() == 1) {
					SelectIndex index = sc.getSelectIndex();
					ColumnInfo columnInfo = DBCatalog.tableInfo.get(table.getAlias() == null ? table.getName() : table.getAlias()).getColumn(sc.getColumns().get(0).split("\\.")[1]);
					if (index.getEquality() != null) {
						if (columnInfo.getMin() > index.getEquality() || columnInfo.getMax() < index.getEquality()) {
							columnInfo.setVValue(0);
						} else {
							columnInfo.setMin(index.getEquality());
							columnInfo.setMax(index.getEquality());
							columnInfo.setVValue(1);
						}
					} else {
						if (index.getMin() != null && columnInfo.getMin() < index.getMin()) {
							columnInfo.setMin(index.getMin());
						}
						if (index.getMax() != null && columnInfo.getMax() > (index.getMax() - 1)) {
							columnInfo.setMax(index.getMax() - 1);
						}
						columnInfo.setVValue(columnInfo.getMax() - columnInfo.getMin() + 1);
					}
					set.add(sc);
				}
			}
		}

		if (set.isEmpty() && residualSelectCondition == null) {
			return new LogicalScanOperator(table, getColumns(table.getName(), table.getAlias()));
		} else {
			return new LogicalSelectOperator(table, set, residualSelectCondition, getColumns(table.getName(), table.getAlias()));
		}
	}
}
