using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	public class TestDebugAdmin
	{
		private MiniDFSCluster cluster;

		private DistributedFileSystem fs;

		private DebugAdmin admin;

		private DataNode datanode;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Configuration conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
			admin = new DebugAdmin(conf);
			datanode = cluster.GetDataNodes()[0];
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
				cluster = null;
			}
		}

		/// <exception cref="System.Exception"/>
		private string RunCmd(string[] cmd)
		{
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			TextWriter @out = new TextWriter(bytes);
			TextWriter oldErr = System.Console.Error;
			TextWriter oldOut = System.Console.Out;
			Runtime.SetErr(@out);
			Runtime.SetOut(@out);
			int ret;
			try
			{
				ret = admin.Run(cmd);
			}
			finally
			{
				Runtime.SetErr(oldErr);
				Runtime.SetOut(oldOut);
				IOUtils.CloseStream(@out);
			}
			return "ret: " + ret + ", " + bytes.ToString().ReplaceAll(Runtime.GetProperty("line.separator"
				), string.Empty);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRecoverLease()
		{
			NUnit.Framework.Assert.AreEqual("ret: 1, You must supply a -path argument to recoverLease."
				, RunCmd(new string[] { "recoverLease", "-retries", "1" }));
			FSDataOutputStream @out = fs.Create(new Path("/foo"));
			@out.Write(123);
			@out.Close();
			NUnit.Framework.Assert.AreEqual("ret: 0, recoverLease SUCCEEDED on /foo", RunCmd(
				new string[] { "recoverLease", "-path", "/foo" }));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestVerifyBlockChecksumCommand()
		{
			DFSTestUtil.CreateFile(fs, new Path("/bar"), 1234, (short)1, unchecked((int)(0xdeadbeef
				)));
			FsDatasetSpi<object> fsd = datanode.GetFSDataset();
			ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, new Path("/bar"));
			FilePath blockFile = FsDatasetTestUtil.GetBlockFile(fsd, block.GetBlockPoolId(), 
				block.GetLocalBlock());
			NUnit.Framework.Assert.AreEqual("ret: 1, You must specify a meta file with -meta"
				, RunCmd(new string[] { "verify", "-block", blockFile.GetAbsolutePath() }));
			FilePath metaFile = FsDatasetTestUtil.GetMetaFile(fsd, block.GetBlockPoolId(), block
				.GetLocalBlock());
			NUnit.Framework.Assert.AreEqual("ret: 0, Checksum type: " + "DataChecksum(type=CRC32C, chunkSize=512)"
				, RunCmd(new string[] { "verify", "-meta", metaFile.GetAbsolutePath() }));
			NUnit.Framework.Assert.AreEqual("ret: 0, Checksum type: " + "DataChecksum(type=CRC32C, chunkSize=512)"
				 + "Checksum verification succeeded on block file " + blockFile.GetAbsolutePath(
				), RunCmd(new string[] { "verify", "-meta", metaFile.GetAbsolutePath(), "-block"
				, blockFile.GetAbsolutePath() }));
		}
	}
}
