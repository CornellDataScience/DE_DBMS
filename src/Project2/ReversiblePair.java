package Project2;


import javafx.util.Pair;

import java.util.Objects;

/**
 * Utility class for a pair that is reverse-equivalent aka (a, b) = (b, a)
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class ReversiblePair<A, B> extends Pair {
   public ReversiblePair(A o, B o2) {
      super(o, o2);
   }

   @Override
   public boolean equals(Object var1) {
      if (!(var1 instanceof ReversiblePair)) {
         return false;
      } else if (Objects.equals(getKey(), ((ReversiblePair)var1).getKey()) && Objects.equals(getValue(), ((ReversiblePair)var1).getValue())) {
         return true;
      } else if (Objects.equals(getKey(), ((ReversiblePair)var1).getValue()) && Objects.equals(getValue(), ((ReversiblePair)var1).getKey())) {
         return true;
      }
      return false;
   }

   @Override
   public int hashCode() {
      if (getKey() == null && getValue() == null) {
         return 0;
      } else if (getKey() == null) {
         return getValue().hashCode() + 1;
      } else if (getValue() == null) {
         return getKey().hashCode() + 1;
      } else {
         return getKey().hashCode() + getValue().hashCode() + 1;
      }
   }
}
