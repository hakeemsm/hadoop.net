using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Race between two threads simultaneously calling
	/// FSNamesystem.getAdditionalBlock().
	/// </summary>
	public class TestAddBlockRetry
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestAddBlockRetry));

		private const short Replication = 3;

		private Configuration conf;

		private MiniDFSCluster cluster;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Retry addBlock() while another thread is in chooseTarget().</summary>
		/// <remarks>
		/// Retry addBlock() while another thread is in chooseTarget().
		/// See HDFS-4452.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRetryAddBlockWhileInChooseTarget()
		{
			string src = "/testRetryAddBlockWhileInChooseTarget";
			FSNamesystem ns = cluster.GetNamesystem();
			NamenodeProtocols nn = cluster.GetNameNodeRpc();
			// create file
			nn.Create(src, FsPermission.GetFileDefault(), "clientName", new EnumSetWritable<CreateFlag
				>(EnumSet.Of(CreateFlag.Create)), true, (short)3, 1024, null);
			// start first addBlock()
			Log.Info("Starting first addBlock for " + src);
			LocatedBlock[] onRetryBlock = new LocatedBlock[1];
			DatanodeStorageInfo[] targets = ns.GetNewBlockTargets(src, INodeId.GrandfatherInodeId
				, "clientName", null, null, null, onRetryBlock);
			NUnit.Framework.Assert.IsNotNull("Targets must be generated", targets);
			// run second addBlock()
			Log.Info("Starting second addBlock for " + src);
			nn.AddBlock(src, "clientName", null, null, INodeId.GrandfatherInodeId, null);
			NUnit.Framework.Assert.IsTrue("Penultimate block must be complete", CheckFileProgress
				(src, false));
			LocatedBlocks lbs = nn.GetBlockLocations(src, 0, long.MaxValue);
			NUnit.Framework.Assert.AreEqual("Must be one block", 1, lbs.GetLocatedBlocks().Count
				);
			LocatedBlock lb2 = lbs.Get(0);
			NUnit.Framework.Assert.AreEqual("Wrong replication", Replication, lb2.GetLocations
				().Length);
			// continue first addBlock()
			LocatedBlock newBlock = ns.StoreAllocatedBlock(src, INodeId.GrandfatherInodeId, "clientName"
				, null, targets);
			NUnit.Framework.Assert.AreEqual("Blocks are not equal", lb2.GetBlock(), newBlock.
				GetBlock());
			// check locations
			lbs = nn.GetBlockLocations(src, 0, long.MaxValue);
			NUnit.Framework.Assert.AreEqual("Must be one block", 1, lbs.GetLocatedBlocks().Count
				);
			LocatedBlock lb1 = lbs.Get(0);
			NUnit.Framework.Assert.AreEqual("Wrong replication", Replication, lb1.GetLocations
				().Length);
			NUnit.Framework.Assert.AreEqual("Blocks are not equal", lb1.GetBlock(), lb2.GetBlock
				());
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool CheckFileProgress(string src, bool checkall)
		{
			FSNamesystem ns = cluster.GetNamesystem();
			ns.ReadLock();
			try
			{
				return ns.CheckFileProgress(src, ns.dir.GetINode(src).AsFile(), checkall);
			}
			finally
			{
				ns.ReadUnlock();
			}
		}

		/*
		* Since NameNode will not persist any locations of the block, addBlock()
		* retry call after restart NN should re-select the locations and return to
		* client. refer HDFS-5257
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddBlockRetryShouldReturnBlockWithLocations()
		{
			string src = "/testAddBlockRetryShouldReturnBlockWithLocations";
			NamenodeProtocols nameNodeRpc = cluster.GetNameNodeRpc();
			// create file
			nameNodeRpc.Create(src, FsPermission.GetFileDefault(), "clientName", new EnumSetWritable
				<CreateFlag>(EnumSet.Of(CreateFlag.Create)), true, (short)3, 1024, null);
			// start first addBlock()
			Log.Info("Starting first addBlock for " + src);
			LocatedBlock lb1 = nameNodeRpc.AddBlock(src, "clientName", null, null, INodeId.GrandfatherInodeId
				, null);
			NUnit.Framework.Assert.IsTrue("Block locations should be present", lb1.GetLocations
				().Length > 0);
			cluster.RestartNameNode();
			nameNodeRpc = cluster.GetNameNodeRpc();
			LocatedBlock lb2 = nameNodeRpc.AddBlock(src, "clientName", null, null, INodeId.GrandfatherInodeId
				, null);
			NUnit.Framework.Assert.AreEqual("Blocks are not equal", lb1.GetBlock(), lb2.GetBlock
				());
			NUnit.Framework.Assert.IsTrue("Wrong locations with retry", lb2.GetLocations().Length
				 > 0);
		}
	}
}
