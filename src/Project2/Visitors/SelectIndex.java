package Project2.Visitors;

import Project2.Visitors.SelectExpressionVisitor.Sign;

/**
 * Represents a select index object
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class SelectIndex {

   private Integer min; // min bound (inclusive)
   private Integer max; // max bound (exclusive)
   private Integer equality;

   /**
    * Creates a new SelectIndex with all null fields
    */
   public SelectIndex() {
      min = null;
      max = null;
      equality = null;
   }

   /**
    * Creates a new SelectIndex from two SelectIndices
    *
    * @param si1 the first SelectIndex
    * @param si2 the second SelectIndex
    */
   public SelectIndex(SelectIndex si1, SelectIndex si2) {
      if (si1.equality == null) {
         equality = si2.equality;
      } else {
         equality = si1.equality;
      }

      if (si1.min == null) {
         min = si2.min;
      } else if (si2.min == null) {
         min = si1.min;
      } else {
         min = Math.max(si2.min, si1.min);
      }

      if (si1.max == null) {
         max = si2.max;
      } else if (si2.max == null) {
         max = si1.max;
      } else {
         max = Math.min(si2.max, si1.max);
      }
   }

   /**
    * Updates to reflect new select
    *
    * @param sign the sign of the new expression
    * @param num the value of the new expression
    */
   public void update(Sign sign, int num) {
      switch (sign) {
         case EQUALS:
            equality = num;
            break;
         case GREATER:
            if (min == null || (num + 1) > min) {
               min = num + 1;
            }
            break;
         case GREATEREQUALS:
            if (min == null || num > min) {
               min = num;
            }
            break;
         case LESS:
            if (max == null || num < max) {
               max = num;
            }
            break;
         case LESSEQUALS:
            if (max == null || (num + 1) < max) {
               max = num + 1;
            }
            break;
      }
   }

   /**
    * @return the min value
    */
   public Integer getMin() {
      return min;
   }

   /**
    * @return the max value
    */
   public Integer getMax() {
      return max;
   }

   /**
    * @return the equals value
    */
   public Integer getEquality() {
      return equality;
   }

   @Override
   public String toString() {
      String e = equality == null ? null : equality.toString();
      String mi = min == null ? null : min.toString();
      String ma = max == null ? null : max.toString();
      return "equals " + e + ", min " + mi + ", max " + ma;
   }
}
