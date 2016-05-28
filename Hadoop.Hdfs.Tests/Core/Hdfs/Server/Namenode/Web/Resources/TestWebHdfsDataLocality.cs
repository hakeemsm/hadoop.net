using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Web.Resources
{
	/// <summary>Test WebHDFS which provides data locality using HTTP redirection.</summary>
	public class TestWebHdfsDataLocality
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestWebHdfsDataLocality
			));

		private const string Rack0 = "/rack0";

		private const string Rack1 = "/rack1";

		private const string Rack2 = "/rack2";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDataLocality()
		{
			Configuration conf = WebHdfsTestUtil.CreateConf();
			string[] racks = new string[] { Rack0, Rack0, Rack1, Rack1, Rack2, Rack2 };
			int nDataNodes = racks.Length;
			Log.Info("nDataNodes=" + nDataNodes + ", racks=" + Arrays.AsList(racks));
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(nDataNodes
				).Racks(racks).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				NameNode namenode = cluster.GetNameNode();
				DatanodeManager dm = namenode.GetNamesystem().GetBlockManager().GetDatanodeManager
					();
				Log.Info("dm=" + dm);
				long blocksize = DFSConfigKeys.DfsBlockSizeDefault;
				string f = "/foo";
				{
					//test CREATE
					for (int i = 0; i < nDataNodes; i++)
					{
						//set client address to a particular datanode
						DataNode dn = cluster.GetDataNodes()[i];
						string ipAddr = dm.GetDatanode(dn.GetDatanodeId()).GetIpAddr();
						//The chosen datanode must be the same as the client address
						DatanodeInfo chosen = NamenodeWebHdfsMethods.ChooseDatanode(namenode, f, PutOpParam.OP
							.Create, -1L, blocksize, null);
						NUnit.Framework.Assert.AreEqual(ipAddr, chosen.GetIpAddr());
					}
				}
				//create a file with one replica.
				Path p = new Path(f);
				FSDataOutputStream @out = dfs.Create(p, (short)1);
				@out.Write(1);
				@out.Close();
				//get replica location.
				LocatedBlocks locatedblocks = NameNodeAdapter.GetBlockLocations(namenode, f, 0, 1
					);
				IList<LocatedBlock> lb = locatedblocks.GetLocatedBlocks();
				NUnit.Framework.Assert.AreEqual(1, lb.Count);
				DatanodeInfo[] locations = lb[0].GetLocations();
				NUnit.Framework.Assert.AreEqual(1, locations.Length);
				DatanodeInfo expected = locations[0];
				{
					//For GETFILECHECKSUM, OPEN and APPEND,
					//the chosen datanode must be the same as the replica location.
					//test GETFILECHECKSUM
					DatanodeInfo chosen = NamenodeWebHdfsMethods.ChooseDatanode(namenode, f, GetOpParam.OP
						.Getfilechecksum, -1L, blocksize, null);
					NUnit.Framework.Assert.AreEqual(expected, chosen);
				}
				{
					//test OPEN
					DatanodeInfo chosen = NamenodeWebHdfsMethods.ChooseDatanode(namenode, f, GetOpParam.OP
						.Open, 0, blocksize, null);
					NUnit.Framework.Assert.AreEqual(expected, chosen);
				}
				{
					//test APPEND
					DatanodeInfo chosen = NamenodeWebHdfsMethods.ChooseDatanode(namenode, f, PostOpParam.OP
						.Append, -1L, blocksize, null);
					NUnit.Framework.Assert.AreEqual(expected, chosen);
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExcludeDataNodes()
		{
			Configuration conf = WebHdfsTestUtil.CreateConf();
			string[] racks = new string[] { Rack0, Rack0, Rack1, Rack1, Rack2, Rack2 };
			string[] hosts = new string[] { "DataNode1", "DataNode2", "DataNode3", "DataNode4"
				, "DataNode5", "DataNode6" };
			int nDataNodes = hosts.Length;
			Log.Info("nDataNodes=" + nDataNodes + ", racks=" + Arrays.AsList(racks) + ", hosts="
				 + Arrays.AsList(hosts));
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Hosts(hosts).NumDataNodes
				(nDataNodes).Racks(racks).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				NameNode namenode = cluster.GetNameNode();
				DatanodeManager dm = namenode.GetNamesystem().GetBlockManager().GetDatanodeManager
					();
				Log.Info("dm=" + dm);
				long blocksize = DFSConfigKeys.DfsBlockSizeDefault;
				string f = "/foo";
				//create a file with three replica.
				Path p = new Path(f);
				FSDataOutputStream @out = dfs.Create(p, (short)3);
				@out.Write(1);
				@out.Close();
				//get replica location.
				LocatedBlocks locatedblocks = NameNodeAdapter.GetBlockLocations(namenode, f, 0, 1
					);
				IList<LocatedBlock> lb = locatedblocks.GetLocatedBlocks();
				NUnit.Framework.Assert.AreEqual(1, lb.Count);
				DatanodeInfo[] locations = lb[0].GetLocations();
				NUnit.Framework.Assert.AreEqual(3, locations.Length);
				//For GETFILECHECKSUM, OPEN and APPEND,
				//the chosen datanode must be different with exclude nodes.
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < 2; i++)
				{
					sb.Append(locations[i].GetXferAddr());
					{
						// test GETFILECHECKSUM
						DatanodeInfo chosen = NamenodeWebHdfsMethods.ChooseDatanode(namenode, f, GetOpParam.OP
							.Getfilechecksum, -1L, blocksize, sb.ToString());
						for (int j = 0; j <= i; j++)
						{
							Assert.AssertNotEquals(locations[j].GetHostName(), chosen.GetHostName());
						}
					}
					{
						// test OPEN
						DatanodeInfo chosen = NamenodeWebHdfsMethods.ChooseDatanode(namenode, f, GetOpParam.OP
							.Open, 0, blocksize, sb.ToString());
						for (int j = 0; j <= i; j++)
						{
							Assert.AssertNotEquals(locations[j].GetHostName(), chosen.GetHostName());
						}
					}
					{
						// test APPEND
						DatanodeInfo chosen = NamenodeWebHdfsMethods.ChooseDatanode(namenode, f, PostOpParam.OP
							.Append, -1L, blocksize, sb.ToString());
						for (int j = 0; j <= i; j++)
						{
							Assert.AssertNotEquals(locations[j].GetHostName(), chosen.GetHostName());
						}
					}
					sb.Append(",");
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		public TestWebHdfsDataLocality()
		{
			{
				DFSTestUtil.SetNameNodeLogLevel(Level.All);
			}
		}
	}
}
