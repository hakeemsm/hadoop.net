using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This test ensures that the balancer bandwidth is dynamically adjusted
	/// correctly.
	/// </summary>
	public class TestBalancerBandwidth
	{
		private static readonly Configuration conf = new Configuration();

		private const int NumOfDatanodes = 2;

		private const int DefaultBandwidth = 1024 * 1024;

		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.TestBalancerBandwidth
			));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBalancerBandwidth()
		{
			/* Set bandwidthPerSec to a low value of 1M bps. */
			conf.SetLong(DFSConfigKeys.DfsDatanodeBalanceBandwidthpersecKey, DefaultBandwidth
				);
			/* Create and start cluster */
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumOfDatanodes
				).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem fs = cluster.GetFileSystem();
				AList<DataNode> datanodes = cluster.GetDataNodes();
				// Ensure value from the configuration is reflected in the datanodes.
				NUnit.Framework.Assert.AreEqual(DefaultBandwidth, (long)datanodes[0].GetBalancerBandwidth
					());
				NUnit.Framework.Assert.AreEqual(DefaultBandwidth, (long)datanodes[1].GetBalancerBandwidth
					());
				// Dynamically change balancer bandwidth and ensure the updated value
				// is reflected on the datanodes.
				long newBandwidth = 12 * DefaultBandwidth;
				// 12M bps
				fs.SetBalancerBandwidth(newBandwidth);
				// Give it a few seconds to propogate new the value to the datanodes.
				try
				{
					Sharpen.Thread.Sleep(5000);
				}
				catch (Exception)
				{
				}
				NUnit.Framework.Assert.AreEqual(newBandwidth, (long)datanodes[0].GetBalancerBandwidth
					());
				NUnit.Framework.Assert.AreEqual(newBandwidth, (long)datanodes[1].GetBalancerBandwidth
					());
				// Dynamically change balancer bandwidth to 0. Balancer bandwidth on the
				// datanodes should remain as it was.
				fs.SetBalancerBandwidth(0);
				// Give it a few seconds to propogate new the value to the datanodes.
				try
				{
					Sharpen.Thread.Sleep(5000);
				}
				catch (Exception)
				{
				}
				NUnit.Framework.Assert.AreEqual(newBandwidth, (long)datanodes[0].GetBalancerBandwidth
					());
				NUnit.Framework.Assert.AreEqual(newBandwidth, (long)datanodes[1].GetBalancerBandwidth
					());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new Org.Apache.Hadoop.Hdfs.TestBalancerBandwidth().TestBalancerBandwidth();
		}
	}
}
