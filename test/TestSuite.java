import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Runs full tests suite.
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
      TupleTest.class,
      BinaryReaderTest.class,
      //PhysicalPlanBuilderTest.class,
      //OperatorsTest.class,
      SamplesTest.class,
      BPlusTest.class
})
public class TestSuite {
}
