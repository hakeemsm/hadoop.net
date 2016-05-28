using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Util;
using Org.Hamcrest;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	public class TestSnapshotFileLength
	{
		private const long Seed = 0;

		private const short Replication = 1;

		private const int Blocksize = 1024;

		private static readonly Configuration conf = new Configuration();

		private static MiniDFSCluster cluster;

		private static DistributedFileSystem hdfs;

		private readonly Path dir = new Path("/TestSnapshotFileLength");

		private readonly Path sub;

		private readonly string file1Name = "file1";

		private readonly string snapshot1 = "snapshot1";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf.SetLong(DFSConfigKeys.DfsNamenodeMinBlockSizeKey, Blocksize);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, Blocksize);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			hdfs = cluster.GetFileSystem();
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

		/// <summary>
		/// Test that we cannot read a file beyond its snapshot length
		/// when accessing it via a snapshot path.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotfileLength()
		{
			hdfs.Mkdirs(sub);
			int bytesRead;
			byte[] buffer = new byte[Blocksize * 8];
			int origLen = Blocksize + 1;
			int toAppend = Blocksize;
			FSDataInputStream fis = null;
			FileStatus fileStatus = null;
			// Create and write a file.
			Path file1 = new Path(sub, file1Name);
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, 0, Blocksize, Replication, Seed);
			DFSTestUtil.AppendFile(hdfs, file1, origLen);
			// Create a snapshot on the parent directory.
			hdfs.AllowSnapshot(sub);
			hdfs.CreateSnapshot(sub, snapshot1);
			Path file1snap1 = SnapshotTestHelper.GetSnapshotPath(sub, snapshot1, file1Name);
			FileChecksum snapChksum1 = hdfs.GetFileChecksum(file1snap1);
			Assert.AssertThat("file and snapshot file checksums are not equal", hdfs.GetFileChecksum
				(file1), CoreMatchers.Is(snapChksum1));
			// Append to the file.
			FSDataOutputStream @out = hdfs.Append(file1);
			// Nothing has been appended yet. All checksums should still be equal.
			Assert.AssertThat("file and snapshot checksums (open for append) are not equal", 
				hdfs.GetFileChecksum(file1), CoreMatchers.Is(snapChksum1));
			Assert.AssertThat("snapshot checksum (post-open for append) has changed", hdfs.GetFileChecksum
				(file1snap1), CoreMatchers.Is(snapChksum1));
			try
			{
				AppendTestUtil.Write(@out, 0, toAppend);
				// Test reading from snapshot of file that is open for append
				byte[] dataFromSnapshot = DFSTestUtil.ReadFileBuffer(hdfs, file1snap1);
				Assert.AssertThat("Wrong data size in snapshot.", dataFromSnapshot.Length, CoreMatchers.Is
					(origLen));
				// Verify that checksum didn't change
				Assert.AssertThat("snapshot file checksum (pre-close) has changed", hdfs.GetFileChecksum
					(file1), CoreMatchers.Is(snapChksum1));
				Assert.AssertThat("snapshot checksum (post-append) has changed", hdfs.GetFileChecksum
					(file1snap1), CoreMatchers.Is(snapChksum1));
			}
			finally
			{
				@out.Close();
			}
			Assert.AssertThat("file and snapshot file checksums (post-close) are equal", hdfs
				.GetFileChecksum(file1), CoreMatchers.Not(snapChksum1));
			Assert.AssertThat("snapshot file checksum (post-close) has changed", hdfs.GetFileChecksum
				(file1snap1), CoreMatchers.Is(snapChksum1));
			// Make sure we can read the entire file via its non-snapshot path.
			fileStatus = hdfs.GetFileStatus(file1);
			Assert.AssertThat(fileStatus.GetLen(), CoreMatchers.Is((long)origLen + toAppend));
			fis = hdfs.Open(file1);
			bytesRead = fis.Read(0, buffer, 0, buffer.Length);
			Assert.AssertThat(bytesRead, CoreMatchers.Is(origLen + toAppend));
			fis.Close();
			// Try to open the file via its snapshot path.
			fis = hdfs.Open(file1snap1);
			fileStatus = hdfs.GetFileStatus(file1snap1);
			Assert.AssertThat(fileStatus.GetLen(), CoreMatchers.Is((long)origLen));
			// Make sure we can only read up to the snapshot length.
			bytesRead = fis.Read(0, buffer, 0, buffer.Length);
			Assert.AssertThat(bytesRead, CoreMatchers.Is(origLen));
			fis.Close();
			byte[] dataFromSnapshot_1 = DFSTestUtil.ReadFileBuffer(hdfs, file1snap1);
			Assert.AssertThat("Wrong data size in snapshot.", dataFromSnapshot_1.Length, CoreMatchers.Is
				(origLen));
		}

		/// <summary>
		/// Adding as part of jira HDFS-5343
		/// Test for checking the cat command on snapshot path it
		/// cannot read a file beyond snapshot file length
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotFileLengthWithCatCommand()
		{
			FSDataInputStream fis = null;
			FileStatus fileStatus = null;
			int bytesRead;
			byte[] buffer = new byte[Blocksize * 8];
			hdfs.Mkdirs(sub);
			Path file1 = new Path(sub, file1Name);
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, Seed);
			hdfs.AllowSnapshot(sub);
			hdfs.CreateSnapshot(sub, snapshot1);
			DFSTestUtil.AppendFile(hdfs, file1, Blocksize);
			// Make sure we can read the entire file via its non-snapshot path.
			fileStatus = hdfs.GetFileStatus(file1);
			NUnit.Framework.Assert.AreEqual("Unexpected file length", Blocksize * 2, fileStatus
				.GetLen());
			fis = hdfs.Open(file1);
			bytesRead = fis.Read(buffer, 0, buffer.Length);
			NUnit.Framework.Assert.AreEqual("Unexpected # bytes read", Blocksize * 2, bytesRead
				);
			fis.Close();
			Path file1snap1 = SnapshotTestHelper.GetSnapshotPath(sub, snapshot1, file1Name);
			fis = hdfs.Open(file1snap1);
			fileStatus = hdfs.GetFileStatus(file1snap1);
			NUnit.Framework.Assert.AreEqual(fileStatus.GetLen(), Blocksize);
			// Make sure we can only read up to the snapshot length.
			bytesRead = fis.Read(buffer, 0, buffer.Length);
			NUnit.Framework.Assert.AreEqual("Unexpected # bytes read", Blocksize, bytesRead);
			fis.Close();
			TextWriter outBackup = System.Console.Out;
			TextWriter errBackup = System.Console.Error;
			ByteArrayOutputStream bao = new ByteArrayOutputStream();
			Runtime.SetOut(new TextWriter(bao));
			Runtime.SetErr(new TextWriter(bao));
			// Make sure we can cat the file upto to snapshot length
			FsShell shell = new FsShell();
			try
			{
				ToolRunner.Run(conf, shell, new string[] { "-cat", "/TestSnapshotFileLength/sub1/.snapshot/snapshot1/file1"
					 });
				NUnit.Framework.Assert.AreEqual("Unexpected # bytes from -cat", Blocksize, bao.Size
					());
			}
			finally
			{
				Runtime.SetOut(outBackup);
				Runtime.SetErr(errBackup);
			}
		}

		public TestSnapshotFileLength()
		{
			sub = new Path(dir, "sub1");
		}
	}
}
