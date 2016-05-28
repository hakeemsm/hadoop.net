using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests DatanodeDescriptor.getBlocksScheduled() at the
	/// NameNode.
	/// </summary>
	/// <remarks>
	/// This class tests DatanodeDescriptor.getBlocksScheduled() at the
	/// NameNode. This counter is supposed to keep track of blocks currently
	/// scheduled to a datanode.
	/// </remarks>
	public class TestBlocksScheduledCounter
	{
		internal MiniDFSCluster cluster = null;

		internal FileSystem fs = null;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (fs != null)
			{
				fs.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlocksScheduledCounter()
		{
			cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
			//open a file an write a few bytes:
			FSDataOutputStream @out = fs.Create(new Path("/testBlockScheduledCounter"));
			for (int i = 0; i < 1024; i++)
			{
				@out.Write(i);
			}
			// flush to make sure a block is allocated.
			@out.Hflush();
			AList<DatanodeDescriptor> dnList = new AList<DatanodeDescriptor>();
			DatanodeManager dm = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
				();
			dm.FetchDatanodes(dnList, dnList, false);
			DatanodeDescriptor dn = dnList[0];
			NUnit.Framework.Assert.AreEqual(1, dn.GetBlocksScheduled());
			// close the file and the counter should go to zero.
			@out.Close();
			NUnit.Framework.Assert.AreEqual(0, dn.GetBlocksScheduled());
		}
	}
}
