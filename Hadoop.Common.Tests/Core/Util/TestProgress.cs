using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestProgress
	{
		[NUnit.Framework.Test]
		public virtual void TestSet()
		{
			Progress progress = new Progress();
			progress.Set(float.NaN);
			NUnit.Framework.Assert.AreEqual(0, progress.GetProgress(), 0.0);
			progress.Set(float.NegativeInfinity);
			NUnit.Framework.Assert.AreEqual(0, progress.GetProgress(), 0.0);
			progress.Set(-1);
			NUnit.Framework.Assert.AreEqual(0, progress.GetProgress(), 0.0);
			progress.Set((float)1.1);
			NUnit.Framework.Assert.AreEqual(1, progress.GetProgress(), 0.0);
			progress.Set(float.PositiveInfinity);
			NUnit.Framework.Assert.AreEqual(1, progress.GetProgress(), 0.0);
		}
	}
}
