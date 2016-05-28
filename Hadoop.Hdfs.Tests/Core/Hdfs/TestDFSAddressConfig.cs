using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestDFSAddressConfig
	{
		/*
		* Test the MiniDFSCluster functionality that allows "dfs.datanode.address",
		* "dfs.datanode.http.address", and "dfs.datanode.ipc.address" to be
		* configurable. The MiniDFSCluster.startDataNodes() API now has a parameter
		* that will check these properties if told to do so.
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDFSAddressConfig()
		{
			Configuration conf = new HdfsConfiguration();
			/*-------------------------------------------------------------------------
			* By default, the DataNode socket address should be localhost (127.0.0.1).
			*------------------------------------------------------------------------*/
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			cluster.WaitActive();
			AList<DataNode> dns = cluster.GetDataNodes();
			DataNode dn = dns[0];
			string selfSocketAddr = dn.GetXferAddress().ToString();
			System.Console.Out.WriteLine("DN Self Socket Addr == " + selfSocketAddr);
			NUnit.Framework.Assert.IsTrue(selfSocketAddr.Contains("/127.0.0.1:"));
			/*-------------------------------------------------------------------------
			* Shut down the datanodes, reconfigure, and bring them back up.
			* Even if told to use the configuration properties for dfs.datanode,
			* MiniDFSCluster.startDataNodes() should use localhost as the default if
			* the dfs.datanode properties are not set.
			*------------------------------------------------------------------------*/
			for (int i = 0; i < dns.Count; i++)
			{
				MiniDFSCluster.DataNodeProperties dnp = cluster.StopDataNode(i);
				NUnit.Framework.Assert.IsNotNull("Should have been able to stop simulated datanode"
					, dnp);
			}
			conf.Unset(DFSConfigKeys.DfsDatanodeAddressKey);
			conf.Unset(DFSConfigKeys.DfsDatanodeHttpAddressKey);
			conf.Unset(DFSConfigKeys.DfsDatanodeIpcAddressKey);
			cluster.StartDataNodes(conf, 1, true, HdfsServerConstants.StartupOption.Regular, 
				null, null, null, false, true);
			dns = cluster.GetDataNodes();
			dn = dns[0];
			selfSocketAddr = dn.GetXferAddress().ToString();
			System.Console.Out.WriteLine("DN Self Socket Addr == " + selfSocketAddr);
			// assert that default self socket address is 127.0.0.1
			NUnit.Framework.Assert.IsTrue(selfSocketAddr.Contains("/127.0.0.1:"));
			/*-------------------------------------------------------------------------
			* Shut down the datanodes, reconfigure, and bring them back up.
			* This time, modify the dfs.datanode properties and make sure that they
			* are used to configure sockets by MiniDFSCluster.startDataNodes().
			*------------------------------------------------------------------------*/
			for (int i_1 = 0; i_1 < dns.Count; i_1++)
			{
				MiniDFSCluster.DataNodeProperties dnp = cluster.StopDataNode(i_1);
				NUnit.Framework.Assert.IsNotNull("Should have been able to stop simulated datanode"
					, dnp);
			}
			conf.Set(DFSConfigKeys.DfsDatanodeAddressKey, "0.0.0.0:0");
			conf.Set(DFSConfigKeys.DfsDatanodeHttpAddressKey, "0.0.0.0:0");
			conf.Set(DFSConfigKeys.DfsDatanodeIpcAddressKey, "0.0.0.0:0");
			cluster.StartDataNodes(conf, 1, true, HdfsServerConstants.StartupOption.Regular, 
				null, null, null, false, true);
			dns = cluster.GetDataNodes();
			dn = dns[0];
			selfSocketAddr = dn.GetXferAddress().ToString();
			System.Console.Out.WriteLine("DN Self Socket Addr == " + selfSocketAddr);
			// assert that default self socket address is 0.0.0.0
			NUnit.Framework.Assert.IsTrue(selfSocketAddr.Contains("/0.0.0.0:"));
			cluster.Shutdown();
		}
	}
}
