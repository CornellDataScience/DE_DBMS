package operator;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import DBSystem.ColumnTab;
import DBSystem.Table;
import net.sf.jsqlparser.statement.select.Distinct;

public class DuplicateElimination extends Operator {

	/**
	 * DuplicateEliminationOperator which exits as long as there is a DISTICT
	 * keyword in the SQL language
	 * 
	 */
	
	private Distinct dist;
	private Operator child;
	private int queryNum;
	private String outputdir;
	
	public DuplicateElimination(Operator sortOp, Distinct distinct, String loc, int queryNum) {
		child = sortOp;
		dist = distinct;
		outputdir = loc;
		this.queryNum = queryNum;
	}
	
	// We assume the input from its child is in sorted order
	@Override
	public Table operate() {
		Table t = child.operate();
		// we will eliminate all duplicate tuples in this table t
		List<ColumnTab> t_columns = t.getColumns();
		// initialize new columns
		List<ColumnTab> new_columns = new ArrayList<ColumnTab>();
		for (ColumnTab c: t_columns) {
			new_columns.add(new ColumnTab(c.getName(), new ArrayList<Double>()));
		}
		
		int index = 0, num = t_columns.get(0).getSize();
		List<Double> prev_tuple = new ArrayList<Double>();
		while (index < num) {
			List<Double> new_tuple = new ArrayList<Double>();
			// get all the objects from the columns at index, compare to the last one
			for (ColumnTab c: t_columns) {
				new_tuple.add(c.getData(index));
			}
			if (!prev_tuple.equals(new_tuple)) {
				// not a duplicate, add this tuple to the new column
				for (int i = 0; i < new_columns.size(); i++) {
					new_columns.get(i).addData(new_tuple.get(i));
				}
			}
			prev_tuple = new_tuple;
			index++;
		}
		Table result = new Table("duplicateEliminated", new_columns);
		return result;
	}

	@Override
	public void reset() {
		child.reset();
	}

	@Override
	public void dump() throws FileNotFoundException {
		PrintStream out = new PrintStream(new FileOutputStream(outputdir + "/query" + queryNum + ".txt"));
		Table t = operate();
		System.setOut(out);
		System.out.println(t.toString());
		out.close();
	}
	
}
