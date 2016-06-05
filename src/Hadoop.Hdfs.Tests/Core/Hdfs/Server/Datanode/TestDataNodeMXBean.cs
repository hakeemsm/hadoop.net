using System.Collections.Generic;
using Javax.Management;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Class for testing
	/// <see cref="DataNodeMXBean"/>
	/// implementation
	/// </summary>
	public class TestDataNodeMXBean
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDataNodeMXBean()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				IList<DataNode> datanodes = cluster.GetDataNodes();
				NUnit.Framework.Assert.AreEqual(datanodes.Count, 1);
				DataNode datanode = datanodes[0];
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanName = new ObjectName("Hadoop:service=DataNode,name=DataNodeInfo"
					);
				// get attribute "ClusterId"
				string clusterId = (string)mbs.GetAttribute(mxbeanName, "ClusterId");
				NUnit.Framework.Assert.AreEqual(datanode.GetClusterId(), clusterId);
				// get attribute "Version"
				string version = (string)mbs.GetAttribute(mxbeanName, "Version");
				NUnit.Framework.Assert.AreEqual(datanode.GetVersion(), version);
				// get attribute "RpcPort"
				string rpcPort = (string)mbs.GetAttribute(mxbeanName, "RpcPort");
				NUnit.Framework.Assert.AreEqual(datanode.GetRpcPort(), rpcPort);
				// get attribute "HttpPort"
				string httpPort = (string)mbs.GetAttribute(mxbeanName, "HttpPort");
				NUnit.Framework.Assert.AreEqual(datanode.GetHttpPort(), httpPort);
				// get attribute "NamenodeAddresses"
				string namenodeAddresses = (string)mbs.GetAttribute(mxbeanName, "NamenodeAddresses"
					);
				NUnit.Framework.Assert.AreEqual(datanode.GetNamenodeAddresses(), namenodeAddresses
					);
				// get attribute "getVolumeInfo"
				string volumeInfo = (string)mbs.GetAttribute(mxbeanName, "VolumeInfo");
				NUnit.Framework.Assert.AreEqual(ReplaceDigits(datanode.GetVolumeInfo()), ReplaceDigits
					(volumeInfo));
				// Ensure mxbean's XceiverCount is same as the DataNode's
				// live value.
				int xceiverCount = (int)mbs.GetAttribute(mxbeanName, "XceiverCount");
				NUnit.Framework.Assert.AreEqual(datanode.GetXceiverCount(), xceiverCount);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private static string ReplaceDigits(string s)
		{
			return s.ReplaceAll("[0-9]+", "_DIGITS_");
		}
	}
}
