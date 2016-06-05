using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestNameNodeRpcServer
	{
		/*
		* Test the MiniDFSCluster functionality that allows "dfs.datanode.address",
		* "dfs.datanode.http.address", and "dfs.datanode.ipc.address" to be
		* configurable. The MiniDFSCluster.startDataNodes() API now has a parameter
		* that will check these properties if told to do so.
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNamenodeRpcBindAny()
		{
			Configuration conf = new HdfsConfiguration();
			// The name node in MiniDFSCluster only binds to 127.0.0.1.
			// We can set the bind address to 0.0.0.0 to make it listen
			// to all interfaces.
			conf.Set(DFSConfigKeys.DfsNamenodeRpcBindHostKey, "0.0.0.0");
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				cluster.WaitActive();
				NUnit.Framework.Assert.AreEqual("0.0.0.0", ((NameNodeRpcServer)cluster.GetNameNodeRpc
					()).GetClientRpcServer().GetListenerAddress().GetHostName());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				// Reset the config
				conf.Unset(DFSConfigKeys.DfsNamenodeRpcBindHostKey);
			}
		}
	}
}
