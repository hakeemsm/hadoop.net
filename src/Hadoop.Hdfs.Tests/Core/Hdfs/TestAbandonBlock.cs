using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Test abandoning blocks, which clients do on pipeline creation failure.</summary>
	public class TestAbandonBlock
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.TestAbandonBlock
			));

		private static readonly Configuration Conf = new HdfsConfiguration();

		internal static readonly string FileNamePrefix = "/" + typeof(Org.Apache.Hadoop.Hdfs.TestAbandonBlock
			).Name + "_";

		private MiniDFSCluster cluster;

		private DistributedFileSystem fs;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			cluster = new MiniDFSCluster.Builder(Conf).NumDataNodes(2).Build();
			fs = cluster.GetFileSystem();
			cluster.WaitActive();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fs.Close();
			cluster.Shutdown();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAbandonBlock()
		{
			string src = FileNamePrefix + "foo";
			// Start writing a file but do not close it
			FSDataOutputStream fout = fs.Create(new Path(src), true, 4096, (short)1, 512L);
			for (int i = 0; i < 1024; i++)
			{
				fout.Write(123);
			}
			fout.Hflush();
			long fileId = ((DFSOutputStream)fout.GetWrappedStream()).GetFileId();
			// Now abandon the last block
			DFSClient dfsclient = DFSClientAdapter.GetDFSClient(fs);
			LocatedBlocks blocks = dfsclient.GetNamenode().GetBlockLocations(src, 0, int.MaxValue
				);
			int orginalNumBlocks = blocks.LocatedBlockCount();
			LocatedBlock b = blocks.GetLastLocatedBlock();
			dfsclient.GetNamenode().AbandonBlock(b.GetBlock(), fileId, src, dfsclient.clientName
				);
			// call abandonBlock again to make sure the operation is idempotent
			dfsclient.GetNamenode().AbandonBlock(b.GetBlock(), fileId, src, dfsclient.clientName
				);
			// And close the file
			fout.Close();
			// Close cluster and check the block has been abandoned after restart
			cluster.RestartNameNode();
			blocks = dfsclient.GetNamenode().GetBlockLocations(src, 0, int.MaxValue);
			NUnit.Framework.Assert.AreEqual("Blocks " + b + " has not been abandoned.", orginalNumBlocks
				, blocks.LocatedBlockCount() + 1);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestQuotaUpdatedWhenBlockAbandoned()
		{
			// Setting diskspace quota to 3MB
			fs.SetQuota(new Path("/"), HdfsConstants.QuotaDontSet, 3 * 1024 * 1024);
			// Start writing a file with 2 replicas to ensure each datanode has one.
			// Block Size is 1MB.
			string src = FileNamePrefix + "test_quota1";
			FSDataOutputStream fout = fs.Create(new Path(src), true, 4096, (short)2, 1024 * 1024
				);
			for (int i = 0; i < 1024; i++)
			{
				fout.WriteByte(123);
			}
			// Shutdown one datanode, causing the block abandonment.
			cluster.GetDataNodes()[0].Shutdown();
			// Close the file, new block will be allocated with 2MB pending size.
			try
			{
				fout.Close();
			}
			catch (QuotaExceededException)
			{
				NUnit.Framework.Assert.Fail("Unexpected quota exception when closing fout");
			}
		}
	}
}
