import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class ExSpec extends Spec with ShouldMatchers {
  it ("should be equal") {
    2 should equal (1)
  }
}
