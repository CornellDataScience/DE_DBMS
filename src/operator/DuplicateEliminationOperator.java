package operator;

import java.util.ArrayList;
import java.util.List;

import DBSystem.Column;
import DBSystem.Table;

public class DuplicateEliminationOperator extends Operator {

	/**
	 * DuplicateEliminationOperator which exits as long as there is a DISTICT
	 * keyword in the SQL language
	 * 
	 */

	private Operator child;
	
	// We assume the input from its child is in sorted order
	@Override
	public Table operate() {
		Table t = child.operate();
		// we will eliminate all duplicate tuples in this table t
		List<Column> t_columns = t.getColumns();
		// initialize new columns
		List<Column> new_columns = new ArrayList<Column>();
		for (Column c: t_columns) {
			new_columns.add(new Column(c.getName(), new ArrayList<Object>()));
		}
		
		int index = 0, num = t_columns.get(0).getSize();
		List<Object> prev_tuple = new ArrayList<Object>();
		while (index < num) {
			List<Object> new_tuple = new ArrayList<Object>();
			// get all the objects from the columns at index, compare to the last one
			for (Column c: t_columns) {
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

}
