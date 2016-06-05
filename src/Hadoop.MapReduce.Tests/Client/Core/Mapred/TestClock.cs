using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>test Clock class</summary>
	public class TestClock
	{
		public virtual void TestClock()
		{
			Clock clock = new Clock();
			long templateTime = Runtime.CurrentTimeMillis();
			long time = clock.GetTime();
			NUnit.Framework.Assert.AreEqual(templateTime, time, 30);
		}
	}
}
