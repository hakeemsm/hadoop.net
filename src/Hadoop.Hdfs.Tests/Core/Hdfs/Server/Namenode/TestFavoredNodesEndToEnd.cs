using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestFavoredNodesEndToEnd
	{
		private static MiniDFSCluster cluster;

		private static Configuration conf;

		private const int NumDataNodes = 10;

		private const int NumFiles = 10;

		private static readonly byte[] SomeBytes = Sharpen.Runtime.GetBytesForString(new 
			string("foo"));

		private static DistributedFileSystem dfs;

		private static AList<DataNode> datanodes;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUpBeforeClass()
		{
			conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Build();
			cluster.WaitClusterUp();
			dfs = cluster.GetFileSystem();
			datanodes = cluster.GetDataNodes();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDownAfterClass()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFavoredNodesEndToEnd()
		{
			//create 10 files with random preferred nodes
			for (int i = 0; i < NumFiles; i++)
			{
				Random rand = new Random(Runtime.CurrentTimeMillis() + i);
				//pass a new created rand so as to get a uniform distribution each time
				//without too much collisions (look at the do-while loop in getDatanodes)
				IPEndPoint[] datanode = GetDatanodes(rand);
				Path p = new Path("/filename" + i);
				FSDataOutputStream @out = dfs.Create(p, FsPermission.GetDefault(), true, 4096, (short
					)3, 4096L, null, datanode);
				@out.Write(SomeBytes);
				@out.Close();
				BlockLocation[] locations = GetBlockLocations(p);
				//verify the files got created in the right nodes
				foreach (BlockLocation loc in locations)
				{
					string[] hosts = loc.GetNames();
					string[] hosts1 = GetStringForInetSocketAddrs(datanode);
					NUnit.Framework.Assert.IsTrue(CompareNodes(hosts, hosts1));
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWhenFavoredNodesNotPresent()
		{
			//when we ask for favored nodes but the nodes are not there, we should
			//get some other nodes. In other words, the write to hdfs should not fail
			//and if we do getBlockLocations on the file, we should see one blklocation
			//and three hosts for that
			IPEndPoint[] arbitraryAddrs = new IPEndPoint[3];
			for (int i = 0; i < 3; i++)
			{
				arbitraryAddrs[i] = GetArbitraryLocalHostAddr();
			}
			Path p = new Path("/filename-foo-bar");
			FSDataOutputStream @out = dfs.Create(p, FsPermission.GetDefault(), true, 4096, (short
				)3, 4096L, null, arbitraryAddrs);
			@out.Write(SomeBytes);
			@out.Close();
			GetBlockLocations(p);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWhenSomeNodesAreNotGood()
		{
			// 4 favored nodes
			IPEndPoint[] addrs = new IPEndPoint[4];
			string[] hosts = new string[addrs.Length];
			for (int i = 0; i < addrs.Length; i++)
			{
				addrs[i] = datanodes[i].GetXferAddress();
				hosts[i] = addrs[i].Address.GetHostAddress() + ":" + addrs[i].Port;
			}
			//make some datanode not "good" so that even if the client prefers it,
			//the namenode would not give it as a replica to write to
			DatanodeInfo d = cluster.GetNameNode().GetNamesystem().GetBlockManager().GetDatanodeManager
				().GetDatanodeByXferAddr(addrs[0].Address.GetHostAddress(), addrs[0].Port);
			//set the decommission status to true so that 
			//BlockPlacementPolicyDefault.isGoodTarget returns false for this dn
			d.SetDecommissioned();
			Path p = new Path("/filename-foo-bar-baz");
			short replication = (short)3;
			FSDataOutputStream @out = dfs.Create(p, FsPermission.GetDefault(), true, 4096, replication
				, 4096L, null, addrs);
			@out.Write(SomeBytes);
			@out.Close();
			//reset the state
			d.StopDecommission();
			BlockLocation[] locations = GetBlockLocations(p);
			NUnit.Framework.Assert.AreEqual(replication, locations[0].GetNames().Length);
			//also make sure that the datanode[0] is not in the list of hosts
			for (int i_1 = 0; i_1 < replication; i_1++)
			{
				string loc = locations[0].GetNames()[i_1];
				int j = 0;
				for (; j < hosts.Length && !loc.Equals(hosts[j]); j++)
				{
				}
				NUnit.Framework.Assert.IsTrue("j=" + j, j > 0);
				NUnit.Framework.Assert.IsTrue("loc=" + loc + " not in host list " + Arrays.AsList
					(hosts) + ", j=" + j, j < hosts.Length);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFavoredNodesEndToEndForAppend()
		{
			// create 10 files with random preferred nodes
			for (int i = 0; i < NumFiles; i++)
			{
				Random rand = new Random(Runtime.CurrentTimeMillis() + i);
				// pass a new created rand so as to get a uniform distribution each time
				// without too much collisions (look at the do-while loop in getDatanodes)
				IPEndPoint[] datanode = GetDatanodes(rand);
				Path p = new Path("/filename" + i);
				// create and close the file.
				dfs.Create(p, FsPermission.GetDefault(), true, 4096, (short)3, 4096L, null, null)
					.Close();
				// re-open for append
				FSDataOutputStream @out = dfs.Append(p, EnumSet.Of(CreateFlag.Append), 4096, null
					, datanode);
				@out.Write(SomeBytes);
				@out.Close();
				BlockLocation[] locations = GetBlockLocations(p);
				// verify the files got created in the right nodes
				foreach (BlockLocation loc in locations)
				{
					string[] hosts = loc.GetNames();
					string[] hosts1 = GetStringForInetSocketAddrs(datanode);
					NUnit.Framework.Assert.IsTrue(CompareNodes(hosts, hosts1));
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private BlockLocation[] GetBlockLocations(Path p)
		{
			DFSTestUtil.WaitReplication(dfs, p, (short)3);
			BlockLocation[] locations = dfs.GetClient().GetBlockLocations(p.ToUri().GetPath()
				, 0, long.MaxValue);
			NUnit.Framework.Assert.IsTrue(locations.Length == 1 && locations[0].GetHosts().Length
				 == 3);
			return locations;
		}

		private string[] GetStringForInetSocketAddrs(IPEndPoint[] datanode)
		{
			string[] strs = new string[datanode.Length];
			for (int i = 0; i < datanode.Length; i++)
			{
				strs[i] = datanode[i].Address.GetHostAddress() + ":" + datanode[i].Port;
			}
			return strs;
		}

		private bool CompareNodes(string[] dnList1, string[] dnList2)
		{
			for (int i = 0; i < dnList1.Length; i++)
			{
				bool matched = false;
				for (int j = 0; j < dnList2.Length; j++)
				{
					if (dnList1[i].Equals(dnList2[j]))
					{
						matched = true;
						break;
					}
				}
				if (matched == false)
				{
					NUnit.Framework.Assert.Fail(dnList1[i] + " not a favored node");
				}
			}
			return true;
		}

		private IPEndPoint[] GetDatanodes(Random rand)
		{
			//Get some unique random indexes
			int idx1 = rand.Next(NumDataNodes);
			int idx2;
			do
			{
				idx2 = rand.Next(NumDataNodes);
			}
			while (idx1 == idx2);
			int idx3;
			do
			{
				idx3 = rand.Next(NumDataNodes);
			}
			while (idx2 == idx3 || idx1 == idx3);
			IPEndPoint[] addrs = new IPEndPoint[3];
			addrs[0] = datanodes[idx1].GetXferAddress();
			addrs[1] = datanodes[idx2].GetXferAddress();
			addrs[2] = datanodes[idx3].GetXferAddress();
			return addrs;
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		private IPEndPoint GetArbitraryLocalHostAddr()
		{
			Random rand = new Random(Runtime.CurrentTimeMillis());
			int port = rand.Next(65535);
			while (true)
			{
				bool conflict = false;
				foreach (DataNode d in datanodes)
				{
					if (d.GetXferAddress().Port == port)
					{
						port = rand.Next(65535);
						conflict = true;
					}
				}
				if (conflict == false)
				{
					break;
				}
			}
			return new IPEndPoint(Sharpen.Runtime.GetLocalHost(), port);
		}

		public TestFavoredNodesEndToEnd()
		{
			{
				((Log4JLogger)LogFactory.GetLog(typeof(BlockPlacementPolicy))).GetLogger().SetLevel
					(Level.All);
			}
		}
	}
}
