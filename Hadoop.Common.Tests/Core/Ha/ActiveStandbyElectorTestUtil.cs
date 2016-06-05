using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Org.Apache.Zookeeper.Server;


namespace Org.Apache.Hadoop.HA
{
	public abstract class ActiveStandbyElectorTestUtil
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ActiveStandbyElectorTestUtil
			));

		private const long LogIntervalMs = 500;

		/// <exception cref="System.Exception"/>
		public static void WaitForActiveLockData(MultithreadedTestUtil.TestContext ctx, ZooKeeperServer
			 zks, string parentDir, byte[] activeData)
		{
			long st = Time.Now();
			long lastPrint = st;
			while (true)
			{
				if (ctx != null)
				{
					ctx.CheckException();
				}
				try
				{
					Stat stat = new Stat();
					byte[] data = zks.GetZKDatabase().GetData(parentDir + "/" + ActiveStandbyElector.
						LockFilename, stat, null);
					if (activeData != null && Arrays.Equals(activeData, data))
					{
						return;
					}
					if (Time.Now() > lastPrint + LogIntervalMs)
					{
						Log.Info("Cur data: " + StringUtils.ByteToHexString(data));
						lastPrint = Time.Now();
					}
				}
				catch (KeeperException.NoNodeException)
				{
					if (activeData == null)
					{
						return;
					}
					if (Time.Now() > lastPrint + LogIntervalMs)
					{
						Log.Info("Cur data: no node");
						lastPrint = Time.Now();
					}
				}
				Thread.Sleep(50);
			}
		}

		/// <exception cref="System.Exception"/>
		public static void WaitForElectorState(MultithreadedTestUtil.TestContext ctx, ActiveStandbyElector
			 elector, ActiveStandbyElector.State state)
		{
			while (elector.GetStateForTests() != state)
			{
				if (ctx != null)
				{
					ctx.CheckException();
				}
				Thread.Sleep(50);
			}
		}
	}
}
