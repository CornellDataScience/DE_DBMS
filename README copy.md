# 4321Project

Top level runnable class is Interpreter

– the selection pushing
There are four data structures the selection is stored in. UnionFind is keyed to
column name and stores SelectionConditions. "Usable" selections are stored here.
UsableJoinConditions stores attr = attr expressions where the attr are from different
tables (these are also stored in the unionFind); this is keyed to the table and
stores a hashset of all the expressions. ResidualJoinConditions contains expressions
between two tables that aren't equality and is keyed by both table's names.
ResidualSelectConditions stores select conditions that aren't usable (not equals
and between two columns in the same table) and is keyed by the table. The selection
is parsed in the SelectExpressionVisitor class.

– the choice of implementation for each logical selection operator
The choice of selection implementations is done in the class of Physical Plan Builder. In the class, we add changes to the function of visit(LogicalSelectOperator op)
In the orignal method, we rely on the configuration file to tell us which selection implentation to choose from when we are given a Logical Select Operator in the logical query plan. In Project 5, we will first check if indexInfo is null, if it is, it means we can only use Physical Select Operator for plain scan. Otherwise we will pass indexInfo to a helper function called useIndex(), and the function will return a boolean value whether to use index or scan based on their costs from table and index sizes.
For Scan, we will calculate cost as t * s / p, where t is the number of tuples the base table has, s is the size of one tuple, and p is the page size, which is 4096 bytes.
For Index, the cost for a clustered index is 3 + p ∗ r while for an unclustered index it is 3 + l ∗ r + t ∗ r, where p is the number of pages in the relation, t the number of tuples, r the reduction factor and l the number of leaves in the index.

– the choice of the join order
The choice of join order is done with dynamic programming. It starts with a minimum of two tables. For the two table case, the table with the larger tuple size is chosen to be the outer relation and then added to the Dynamic Computing map. Our map is structured as follows (sortedOrder -> (joinedOrder, VValue)) (ex. (ABC -> (CAB, 100))) If there are more than 2 tables to be joined (a,b,c), theres an iterative procedure performed which temporarily removes one of the elements from the list and calculates the join order. In this case, the join orders calculated would be (ab, ac, bc). Whichever one is the fastest would have the removed table appended to the end signifying that the first 2 tables be joined and then the third and then the next ones. The next joins use the old VVavlues calculated to calculate the proceeding VValues and figuring out which join order would produce the smallest intermediate join size. 

– the choice of implementation for each join operator
As we see that the SMJ is much faster than BNLJ in Project 3's bench marking, we will use SMJ if possible. If not, we choose to use Block Nested Loop Join. 

- Other things for the grader to know
- In the case when we need to print join operators and there’s no residual join expression in the logical join operator, it prints empty brackets.