using Javax.Management;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>Make sure HA-related metrics are updated and reported appropriately.</summary>
	public class TestHAMetrics
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.TestHAMetrics
			));

		/// <exception cref="System.Exception"/>
		public virtual void TestHAMetrics()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			conf.SetInt(DFSConfigKeys.DfsHaLogrollPeriodKey, int.MaxValue);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(1).Build();
			FileSystem fs = null;
			try
			{
				cluster.WaitActive();
				FSNamesystem nn0 = cluster.GetNamesystem(0);
				FSNamesystem nn1 = cluster.GetNamesystem(1);
				NUnit.Framework.Assert.AreEqual(nn0.GetHAState(), "standby");
				NUnit.Framework.Assert.IsTrue(0 < nn0.GetMillisSinceLastLoadedEdits());
				NUnit.Framework.Assert.AreEqual(nn1.GetHAState(), "standby");
				NUnit.Framework.Assert.IsTrue(0 < nn1.GetMillisSinceLastLoadedEdits());
				cluster.TransitionToActive(0);
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanName = new ObjectName("Hadoop:service=NameNode,name=NameNodeStatus"
					);
				long ltt1 = (long)mbs.GetAttribute(mxbeanName, "LastHATransitionTime");
				NUnit.Framework.Assert.IsTrue("lastHATransitionTime should be > 0", ltt1 > 0);
				NUnit.Framework.Assert.AreEqual("active", nn0.GetHAState());
				NUnit.Framework.Assert.AreEqual(0, nn0.GetMillisSinceLastLoadedEdits());
				NUnit.Framework.Assert.AreEqual("standby", nn1.GetHAState());
				NUnit.Framework.Assert.IsTrue(0 < nn1.GetMillisSinceLastLoadedEdits());
				cluster.TransitionToStandby(0);
				long ltt2 = (long)mbs.GetAttribute(mxbeanName, "LastHATransitionTime");
				NUnit.Framework.Assert.IsTrue("lastHATransitionTime should be > " + ltt1, ltt2 > 
					ltt1);
				cluster.TransitionToActive(1);
				NUnit.Framework.Assert.AreEqual("standby", nn0.GetHAState());
				NUnit.Framework.Assert.IsTrue(0 < nn0.GetMillisSinceLastLoadedEdits());
				NUnit.Framework.Assert.AreEqual("active", nn1.GetHAState());
				NUnit.Framework.Assert.AreEqual(0, nn1.GetMillisSinceLastLoadedEdits());
				Sharpen.Thread.Sleep(2000);
				// make sure standby gets a little out-of-date
				NUnit.Framework.Assert.IsTrue(2000 <= nn0.GetMillisSinceLastLoadedEdits());
				NUnit.Framework.Assert.AreEqual(0, nn0.GetPendingDataNodeMessageCount());
				NUnit.Framework.Assert.AreEqual(0, nn1.GetPendingDataNodeMessageCount());
				fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				DFSTestUtil.CreateFile(fs, new Path("/foo"), 10, (short)1, 1L);
				NUnit.Framework.Assert.IsTrue(0 < nn0.GetPendingDataNodeMessageCount());
				NUnit.Framework.Assert.AreEqual(0, nn1.GetPendingDataNodeMessageCount());
				long millisSinceLastLoadedEdits = nn0.GetMillisSinceLastLoadedEdits();
				HATestUtil.WaitForStandbyToCatchUp(cluster.GetNameNode(1), cluster.GetNameNode(0)
					);
				NUnit.Framework.Assert.AreEqual(0, nn0.GetPendingDataNodeMessageCount());
				NUnit.Framework.Assert.AreEqual(0, nn1.GetPendingDataNodeMessageCount());
				long newMillisSinceLastLoadedEdits = nn0.GetMillisSinceLastLoadedEdits();
				// Since we just waited for the standby to catch up, the time since we
				// last loaded edits should be very low.
				NUnit.Framework.Assert.IsTrue("expected " + millisSinceLastLoadedEdits + " > " + 
					newMillisSinceLastLoadedEdits, millisSinceLastLoadedEdits > newMillisSinceLastLoadedEdits
					);
			}
			finally
			{
				IOUtils.Cleanup(Log, fs);
				cluster.Shutdown();
			}
		}
	}
}
