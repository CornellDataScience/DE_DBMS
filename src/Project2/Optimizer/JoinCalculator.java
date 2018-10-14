package Project2.Optimizer;

import Project2.DBCatalog;
import Project2.ReversiblePair;
import Project2.Visitors.JoinExpressionVisitor;
import javafx.util.Pair;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;

public class JoinCalculator {

    private ArrayList<String> tables;
    private ArrayList<String> aliases;
    private HashMap<ArrayList<Integer>, Pair<ArrayList<Integer>, Float>> dpMap;
    private HashMap<ReversiblePair<String, String>, Expression> joinConditions;
    //ordered list -> sorting order, intermeidate join size
    //tmpOrders -> columnInfos
    private HashMap<Integer, ArrayList<ArrayList<Integer>>> sets;
    private ArrayList<Integer> result;

    public JoinCalculator (ArrayList<String> tables, ArrayList<String> aliases, HashMap<ReversiblePair<String, String>, Expression> joinConditions) {
        this.tables = tables;
        this.aliases = aliases;
        this.joinConditions = joinConditions;

        dpMap = new HashMap<>();
        getSubsets();
        int j = 1;
        while (sets.containsKey(j)) {
            ArrayList<ArrayList<Integer>> tmp = sets.get(j);
            if (j == 2) {
                for (ArrayList<Integer> o : tmp) {
                    String t1 = tables.get(o.get(0));
                    String t2 = tables.get(o.get(1));
                    int tuples1 = DBCatalog.getTableInfo(t1).getSize();
                    int tuples2 = DBCatalog.getTableInfo(t2).getSize();
                    ArrayList<Integer> order = new ArrayList<>();
                    if (tuples1 <= tuples2) {
                        order.add(o.get(0));
                        order.add(o.get(1));
                    } else {
                        order.add(o.get(1));
                        order.add(o.get(0));
                    }
                    if (tables.size() == 2)
                        result = order;
                    dpMap.put(o, new Pair<>(order, 0f));
                }
            }
            else if (j > 2) {
                for (ArrayList<Integer> lst : tmp) {
                    float minJoinSize = Float.MAX_VALUE;
                    ArrayList<Integer> bestJoin = new ArrayList<>();
                    for (int k = 0; k < lst.size(); k++) {
                        ArrayList<Integer> clone = new ArrayList<>(lst);
                        Integer tmpNum = clone.remove(k);
                        ArrayList<Expression> joins = getJoins(new ArrayList<>(clone.subList(0, clone.size()-1)), clone.get(clone.size()-1));
                        if (joins.size() == 0)
                            continue;
                        ArrayList<Pair<String, String>> joinValues = new ArrayList<>();
                        for (Expression e : joins) {
                            JoinExpressionVisitor jev = new JoinExpressionVisitor();
                            e.accept(jev);
                            joinValues.addAll(jev.getResult());
                        }
                        float joinSize = computeVValue(joinValues);
                        float oldJoinSize = dpMap.get(clone).getValue();
                        joinSize += oldJoinSize;
                        if (joinSize <= minJoinSize) {
                            clone.add(tmpNum);
                            bestJoin = clone;
                            minJoinSize = joinSize;
                        }
                    }
                    dpMap.put(lst, new Pair<>(bestJoin, minJoinSize));
                    if (j == tables.size()) {
                        result = bestJoin;
                    }
                }
            }

            j++;
        }
    }

    public ArrayList<String> getResult() {
        ArrayList<String> stringResult = new ArrayList<>();
        for (Integer i : result) {
            stringResult.add(aliases.get(i));
        }
        return stringResult;
    }

    private void getSubsets() {
        int n = tables.size();
        sets = new HashMap<>();
        for (int i = 0; i < (1<<n); i++) {
            ArrayList<Integer> tmp = new ArrayList<>();
            for (int j = 0; j < n; j++)
                if ((i & (1 << j)) > 0)
                    tmp.add(j);

            ArrayList<ArrayList<Integer>> tmpResult;
            if (sets.containsKey(tmp.size()))
                tmpResult = sets.get(tmp.size());
            else
                tmpResult = new ArrayList<>();
            tmpResult.add(tmp);
            sets.put(tmp.size(), tmpResult);
        }
    }

    ArrayList<ArrayList<Integer>> getSubsets( ArrayList<Integer> set, int num) {
        int n = set.size();
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        for (int i = 0; i < (1<<n); i++)
        {
            ArrayList<Integer> tmp = new ArrayList<>();
            for (int j = 0; j < n; j++)
                if ((i & (1 << j)) > 0)
                    tmp.add(set.get(j));

            if (tmp.size() == num)
                result.add(tmp);
        }
        return result;
    }

    private ArrayList<Expression> getJoins(ArrayList<Integer> nums, int joinNum) {
        ArrayList<Expression> result = new ArrayList<>();
        for (Integer num : nums) {
            Expression e = (joinConditions.get(new ReversiblePair<>(aliases.get(num), aliases.get
                  (joinNum))));
            if (e != null)
                result.add(e);
        }
        return result;
    }

    private float computeVValue(ArrayList<Pair<String, String>> joinValues) {
        if (joinValues.size() == 0) {
            return 10000000000000f;
        }
        float result;
        int tupleSize1=0;
        int tupleSize2=0;
        float counter = 1;
        for (int i = 0; i < joinValues.size(); i++) {
            int tmp1 = joinValues.get(i).getKey().indexOf('.');
            int tmp2 = joinValues.get(i).getValue().indexOf('.');
            try {
                tupleSize1 = DBCatalog.getTableInfo(joinValues.get(i).getKey().substring(0, tmp1)).getSize();
                tupleSize2 = DBCatalog.getTableInfo(joinValues.get(i).getValue().substring(0, tmp2)).getSize();
            }
            catch (Exception e) {
                tupleSize1 = DBCatalog.getTableInfo(tables.get(aliases.indexOf(joinValues.get(i).getKey().substring(0, tmp1)))).getSize();
                tupleSize2 = DBCatalog.getTableInfo(tables.get(aliases.indexOf(joinValues.get(i).getKey().substring(0, tmp1)))).getSize();
            }
            String key = joinValues.get(i).getKey();
            String val = joinValues.get(i).getValue();
            int vval1;
            int vval2;
            try {
                vval1 = DBCatalog.getTableInfo(key.substring(0, tmp1)).getVValue(key.substring(tmp1 + 1));
                vval2 = DBCatalog.getTableInfo(val.substring(0, tmp2)).getVValue(val.substring(tmp2 + 1));
            }
            catch (Exception e) {
                vval1 = DBCatalog.getTableInfo(tables.get(aliases.indexOf(key.substring(0, tmp1)))).getVValue(key.substring(tmp1 + 1));
                vval2 = DBCatalog.getTableInfo(tables.get(aliases.indexOf(val.substring(0, tmp2)))).getVValue(val.substring(tmp2 + 1));
            }
            counter *= Math.max(vval1, vval2);
        }
        result = (float)(tupleSize1*tupleSize2)/counter;
        if (result < 1.0)
            return 1f;
        else
            return result;
    }
}
