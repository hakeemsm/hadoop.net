using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.Window
{
	public class TestRollingWindow
	{
		internal readonly int WindowLen = 60000;

		internal readonly int BucketCnt = 10;

		internal readonly int BucketLen = WindowLen / BucketCnt;

		[NUnit.Framework.Test]
		public virtual void TestBasics()
		{
			RollingWindow window = new RollingWindow(WindowLen, BucketCnt);
			long time = 1;
			NUnit.Framework.Assert.AreEqual("The initial sum of rolling window must be 0", 0, 
				window.GetSum(time));
			time = WindowLen + BucketLen * 3 / 2;
			NUnit.Framework.Assert.AreEqual("The initial sum of rolling window must be 0", 0, 
				window.GetSum(time));
			window.IncAt(time, 5);
			NUnit.Framework.Assert.AreEqual("The sum of rolling window does not reflect the recent update"
				, 5, window.GetSum(time));
			time += BucketLen;
			window.IncAt(time, 6);
			NUnit.Framework.Assert.AreEqual("The sum of rolling window does not reflect the recent update"
				, 11, window.GetSum(time));
			time += WindowLen - BucketLen;
			NUnit.Framework.Assert.AreEqual("The sum of rolling window does not reflect rolling effect"
				, 6, window.GetSum(time));
			time += BucketLen;
			NUnit.Framework.Assert.AreEqual("The sum of rolling window does not reflect rolling effect"
				, 0, window.GetSum(time));
		}

		[NUnit.Framework.Test]
		public virtual void TestReorderedAccess()
		{
			RollingWindow window = new RollingWindow(WindowLen, BucketCnt);
			long time = 2 * WindowLen + BucketLen * 3 / 2;
			window.IncAt(time, 5);
			time++;
			NUnit.Framework.Assert.AreEqual("The sum of rolling window does not reflect the recent update"
				, 5, window.GetSum(time));
			long reorderedTime = time - 2 * BucketLen;
			window.IncAt(reorderedTime, 6);
			NUnit.Framework.Assert.AreEqual("The sum of rolling window does not reflect the reordered update"
				, 11, window.GetSum(time));
			time = reorderedTime + WindowLen;
			NUnit.Framework.Assert.AreEqual("The sum of rolling window does not reflect rolling effect"
				, 5, window.GetSum(time));
		}
	}
}
