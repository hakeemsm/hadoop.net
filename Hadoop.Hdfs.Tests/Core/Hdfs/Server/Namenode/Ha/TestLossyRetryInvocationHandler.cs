using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// This test makes sure that when
	/// <see cref="Org.Apache.Hadoop.Hdfs.DFSConfigKeys.DfsClientTestDropNamenodeResponseNumKey
	/// 	"/>
	/// is set,
	/// DFSClient instances can still be created within NN/DN (e.g., the fs instance
	/// used by the trash emptier thread in NN)
	/// </summary>
	public class TestLossyRetryInvocationHandler
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartNNWithTrashEmptier()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = new HdfsConfiguration();
			// enable both trash emptier and dropping response
			conf.SetLong("fs.trash.interval", 360);
			conf.SetInt(DFSConfigKeys.DfsClientTestDropNamenodeResponseNumKey, 2);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
					()).NumDataNodes(0).Build();
				cluster.WaitActive();
				cluster.TransitionToActive(0);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
