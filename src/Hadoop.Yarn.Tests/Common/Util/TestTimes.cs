using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class TestTimes
	{
		[NUnit.Framework.Test]
		public virtual void TestNegativeStartTimes()
		{
			long elapsed = Times.Elapsed(-5, 10, true);
			NUnit.Framework.Assert.AreEqual("Elapsed time is not 0", 0, elapsed);
			elapsed = Times.Elapsed(-5, 10, false);
			NUnit.Framework.Assert.AreEqual("Elapsed time is not -1", -1, elapsed);
		}

		[NUnit.Framework.Test]
		public virtual void TestNegativeFinishTimes()
		{
			long elapsed = Times.Elapsed(5, -10, false);
			NUnit.Framework.Assert.AreEqual("Elapsed time is not -1", -1, elapsed);
		}

		[NUnit.Framework.Test]
		public virtual void TestNegativeStartandFinishTimes()
		{
			long elapsed = Times.Elapsed(-5, -10, false);
			NUnit.Framework.Assert.AreEqual("Elapsed time is not -1", -1, elapsed);
		}

		[NUnit.Framework.Test]
		public virtual void TestPositiveStartandFinishTimes()
		{
			long elapsed = Times.Elapsed(5, 10, true);
			NUnit.Framework.Assert.AreEqual("Elapsed time is not 5", 5, elapsed);
			elapsed = Times.Elapsed(5, 10, false);
			NUnit.Framework.Assert.AreEqual("Elapsed time is not 5", 5, elapsed);
		}

		[NUnit.Framework.Test]
		public virtual void TestFinishTimesAheadOfStartTimes()
		{
			long elapsed = Times.Elapsed(10, 5, true);
			NUnit.Framework.Assert.AreEqual("Elapsed time is not -1", -1, elapsed);
			elapsed = Times.Elapsed(10, 5, false);
			NUnit.Framework.Assert.AreEqual("Elapsed time is not -1", -1, elapsed);
			// use Long.MAX_VALUE to ensure started time is after the current one
			elapsed = Times.Elapsed(long.MaxValue, 0, true);
			NUnit.Framework.Assert.AreEqual("Elapsed time is not -1", -1, elapsed);
		}
	}
}
