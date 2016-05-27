using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	public class ZKFCTestUtil
	{
		/// <exception cref="System.Exception"/>
		public static void WaitForHealthState(ZKFailoverController zkfc, HealthMonitor.State
			 state, MultithreadedTestUtil.TestContext ctx)
		{
			while (zkfc.GetLastHealthState() != state)
			{
				if (ctx != null)
				{
					ctx.CheckException();
				}
				Sharpen.Thread.Sleep(50);
			}
		}
	}
}
