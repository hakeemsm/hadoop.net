using Sharpen;

namespace org.apache.hadoop.ha
{
	public abstract class ActiveStandbyElectorTestUtil
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.ActiveStandbyElectorTestUtil
			)));

		private const long LOG_INTERVAL_MS = 500;

		/// <exception cref="System.Exception"/>
		public static void waitForActiveLockData(org.apache.hadoop.test.MultithreadedTestUtil.TestContext
			 ctx, org.apache.zookeeper.server.ZooKeeperServer zks, string parentDir, byte[] 
			activeData)
		{
			long st = org.apache.hadoop.util.Time.now();
			long lastPrint = st;
			while (true)
			{
				if (ctx != null)
				{
					ctx.checkException();
				}
				try
				{
					org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
					byte[] data = zks.getZKDatabase().getData(parentDir + "/" + org.apache.hadoop.ha.ActiveStandbyElector
						.LOCK_FILENAME, stat, null);
					if (activeData != null && java.util.Arrays.equals(activeData, data))
					{
						return;
					}
					if (org.apache.hadoop.util.Time.now() > lastPrint + LOG_INTERVAL_MS)
					{
						LOG.info("Cur data: " + org.apache.hadoop.util.StringUtils.byteToHexString(data));
						lastPrint = org.apache.hadoop.util.Time.now();
					}
				}
				catch (org.apache.zookeeper.KeeperException.NoNodeException)
				{
					if (activeData == null)
					{
						return;
					}
					if (org.apache.hadoop.util.Time.now() > lastPrint + LOG_INTERVAL_MS)
					{
						LOG.info("Cur data: no node");
						lastPrint = org.apache.hadoop.util.Time.now();
					}
				}
				java.lang.Thread.sleep(50);
			}
		}

		/// <exception cref="System.Exception"/>
		public static void waitForElectorState(org.apache.hadoop.test.MultithreadedTestUtil.TestContext
			 ctx, org.apache.hadoop.ha.ActiveStandbyElector elector, org.apache.hadoop.ha.ActiveStandbyElector.State
			 state)
		{
			while (elector.getStateForTests() != state)
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
