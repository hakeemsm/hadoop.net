using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestDefaultBlockPlacementPolicy
	{
		private const short ReplicationFactor = (short)3;

		private const int DefaultBlockSize = 1024;

		private MiniDFSCluster cluster = null;

		private NamenodeProtocols nameNodeRpc = null;

		private FSNamesystem namesystem = null;

		private PermissionStatus perm = null;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			StaticMapping.ResetMap();
			Configuration conf = new HdfsConfiguration();
			string[] racks = new string[] { "/RACK0", "/RACK0", "/RACK2", "/RACK3", "/RACK2" };
			string[] hosts = new string[] { "/host0", "/host1", "/host2", "/host3", "/host4" };
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, DefaultBlockSize);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, DefaultBlockSize / 2);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(5).Racks(racks).Hosts(hosts
				).Build();
			cluster.WaitActive();
			nameNodeRpc = cluster.GetNameNodeRpc();
			namesystem = cluster.GetNamesystem();
			perm = new PermissionStatus("TestDefaultBlockPlacementPolicy", null, FsPermission
				.GetDefault());
		}

		[TearDown]
		public virtual void Teardown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Verify rack-local node selection for the rack-local client in case of no
		/// local node
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalRackPlacement()
		{
			string clientMachine = "client.foo.com";
			// Map client to RACK2
			string clientRack = "/RACK2";
			StaticMapping.AddNodeToRack(clientMachine, clientRack);
			TestPlacement(clientMachine, clientRack);
		}

		/// <summary>Verify Random rack node selection for remote client</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRandomRackSelectionForRemoteClient()
		{
			string clientMachine = "client.foo.com";
			// Don't map client machine to any rack,
			// so by default it will be treated as /default-rack
			// in that case a random node should be selected as first node.
			TestPlacement(clientMachine, null);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestPlacement(string clientMachine, string clientRack)
		{
			// write 5 files and check whether all times block placed
			for (int i = 0; i < 5; i++)
			{
				string src = "/test-" + i;
				// Create the file with client machine
				HdfsFileStatus fileStatus = namesystem.StartFile(src, perm, clientMachine, clientMachine
					, EnumSet.Of(CreateFlag.Create), true, ReplicationFactor, DefaultBlockSize, null
					, false);
				LocatedBlock locatedBlock = nameNodeRpc.AddBlock(src, clientMachine, null, null, 
					fileStatus.GetFileId(), null);
				NUnit.Framework.Assert.AreEqual("Block should be allocated sufficient locations", 
					ReplicationFactor, locatedBlock.GetLocations().Length);
				if (clientRack != null)
				{
					NUnit.Framework.Assert.AreEqual("First datanode should be rack local", clientRack
						, locatedBlock.GetLocations()[0].GetNetworkLocation());
				}
				nameNodeRpc.AbandonBlock(locatedBlock.GetBlock(), fileStatus.GetFileId(), src, clientMachine
					);
			}
		}
	}
}
