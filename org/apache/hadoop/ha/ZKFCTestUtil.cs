using Sharpen;

namespace org.apache.hadoop.ha
{
	public class ZKFCTestUtil
	{
		/// <exception cref="System.Exception"/>
		public static void waitForHealthState(org.apache.hadoop.ha.ZKFailoverController zkfc
			, org.apache.hadoop.ha.HealthMonitor.State state, org.apache.hadoop.test.MultithreadedTestUtil.TestContext
			 ctx)
		{
			while (zkfc.getLastHealthState() != state)
			{
				if (ctx != null)
				{
					ctx.checkException();
				}
				java.lang.Thread.sleep(50);
			}
		}
	}
}
