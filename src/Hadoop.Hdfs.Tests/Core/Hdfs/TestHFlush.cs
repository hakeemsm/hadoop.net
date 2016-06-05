using System.Threading;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// Class contains a set of tests to verify the correctness of
	/// newly introduced
	/// <see cref="Org.Apache.Hadoop.FS.FSDataOutputStream.Hflush()"/>
	/// method
	/// </summary>
	public class TestHFlush
	{
		private readonly string fName = "hflushtest.dat";

		/// <summary>
		/// The test uses
		/// <see cref="DoTheJob(Org.Apache.Hadoop.Conf.Configuration, string, long, short, bool, Sharpen.EnumSet{E})
		/// 	"/>
		/// 
		/// to write a file with a standard block size
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void HFlush_01()
		{
			DoTheJob(new HdfsConfiguration(), fName, AppendTestUtil.BlockSize, (short)2, false
				, EnumSet.NoneOf<HdfsDataOutputStream.SyncFlag>());
		}

		/// <summary>
		/// The test uses
		/// <see cref="DoTheJob(Org.Apache.Hadoop.Conf.Configuration, string, long, short, bool, Sharpen.EnumSet{E})
		/// 	"/>
		/// 
		/// to write a file with a custom block size so the writes will be
		/// happening across block' boundaries
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void HFlush_02()
		{
			Configuration conf = new HdfsConfiguration();
			int customPerChecksumSize = 512;
			int customBlockSize = customPerChecksumSize * 3;
			// Modify defaul filesystem settings
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, customPerChecksumSize);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, customBlockSize);
			DoTheJob(conf, fName, customBlockSize, (short)2, false, EnumSet.NoneOf<HdfsDataOutputStream.SyncFlag
				>());
		}

		/// <summary>
		/// The test uses
		/// <see cref="DoTheJob(Org.Apache.Hadoop.Conf.Configuration, string, long, short, bool, Sharpen.EnumSet{E})
		/// 	"/>
		/// 
		/// to write a file with a custom block size so the writes will be
		/// happening across block's and checksum' boundaries
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void HFlush_03()
		{
			Configuration conf = new HdfsConfiguration();
			int customPerChecksumSize = 400;
			int customBlockSize = customPerChecksumSize * 3;
			// Modify defaul filesystem settings
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, customPerChecksumSize);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, customBlockSize);
			DoTheJob(conf, fName, customBlockSize, (short)2, false, EnumSet.NoneOf<HdfsDataOutputStream.SyncFlag
				>());
		}

		/// <summary>
		/// Test hsync (with updating block length in NameNode) while no data is
		/// actually written yet
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void HSyncUpdateLength_00()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			DistributedFileSystem fileSystem = cluster.GetFileSystem();
			try
			{
				Path path = new Path(fName);
				FSDataOutputStream stm = fileSystem.Create(path, true, 4096, (short)2, AppendTestUtil
					.BlockSize);
				System.Console.Out.WriteLine("Created file " + path.ToString());
				((DFSOutputStream)stm.GetWrappedStream()).Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag
					.UpdateLength));
				long currentFileLength = fileSystem.GetFileStatus(path).GetLen();
				NUnit.Framework.Assert.AreEqual(0L, currentFileLength);
				stm.Close();
			}
			finally
			{
				fileSystem.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Test hsync with END_BLOCK flag.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void HSyncEndBlock_00()
		{
			int preferredBlockSize = 1024;
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, preferredBlockSize);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			DistributedFileSystem fileSystem = cluster.GetFileSystem();
			FSDataOutputStream stm = null;
			try
			{
				Path path = new Path("/" + fName);
				stm = fileSystem.Create(path, true, 4096, (short)2, AppendTestUtil.BlockSize);
				System.Console.Out.WriteLine("Created file " + path.ToString());
				((DFSOutputStream)stm.GetWrappedStream()).Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag
					.EndBlock));
				long currentFileLength = fileSystem.GetFileStatus(path).GetLen();
				NUnit.Framework.Assert.AreEqual(0L, currentFileLength);
				LocatedBlocks blocks = fileSystem.dfs.GetLocatedBlocks(path.ToString(), 0);
				NUnit.Framework.Assert.AreEqual(0, blocks.GetLocatedBlocks().Count);
				// write a block and call hsync(end_block) at the block boundary
				stm.Write(new byte[preferredBlockSize]);
				((DFSOutputStream)stm.GetWrappedStream()).Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag
					.EndBlock));
				currentFileLength = fileSystem.GetFileStatus(path).GetLen();
				NUnit.Framework.Assert.AreEqual(preferredBlockSize, currentFileLength);
				blocks = fileSystem.dfs.GetLocatedBlocks(path.ToString(), 0);
				NUnit.Framework.Assert.AreEqual(1, blocks.GetLocatedBlocks().Count);
				// call hsync then call hsync(end_block) immediately
				stm.Write(new byte[preferredBlockSize / 2]);
				stm.Hsync();
				((DFSOutputStream)stm.GetWrappedStream()).Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag
					.EndBlock));
				currentFileLength = fileSystem.GetFileStatus(path).GetLen();
				NUnit.Framework.Assert.AreEqual(preferredBlockSize + preferredBlockSize / 2, currentFileLength
					);
				blocks = fileSystem.dfs.GetLocatedBlocks(path.ToString(), 0);
				NUnit.Framework.Assert.AreEqual(2, blocks.GetLocatedBlocks().Count);
				stm.Write(new byte[preferredBlockSize / 4]);
				stm.Hsync();
				currentFileLength = fileSystem.GetFileStatus(path).GetLen();
				NUnit.Framework.Assert.AreEqual(preferredBlockSize + preferredBlockSize / 2 + preferredBlockSize
					 / 4, currentFileLength);
				blocks = fileSystem.dfs.GetLocatedBlocks(path.ToString(), 0);
				NUnit.Framework.Assert.AreEqual(3, blocks.GetLocatedBlocks().Count);
			}
			finally
			{
				IOUtils.Cleanup(null, stm, fileSystem);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// The test calls
		/// <see cref="DoTheJob(Org.Apache.Hadoop.Conf.Configuration, string, long, short, bool, Sharpen.EnumSet{E})
		/// 	"/>
		/// while requiring the semantic of
		/// <see cref="Org.Apache.Hadoop.Hdfs.Client.HdfsDataOutputStream.SyncFlag.UpdateLength
		/// 	"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void HSyncUpdateLength_01()
		{
			DoTheJob(new HdfsConfiguration(), fName, AppendTestUtil.BlockSize, (short)2, true
				, EnumSet.Of(HdfsDataOutputStream.SyncFlag.UpdateLength));
		}

		/// <summary>
		/// The test calls
		/// <see cref="DoTheJob(Org.Apache.Hadoop.Conf.Configuration, string, long, short, bool, Sharpen.EnumSet{E})
		/// 	"/>
		/// while requiring the semantic of
		/// <see cref="Org.Apache.Hadoop.Hdfs.Client.HdfsDataOutputStream.SyncFlag.EndBlock"/
		/// 	>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void HSyncEndBlock_01()
		{
			DoTheJob(new HdfsConfiguration(), fName, AppendTestUtil.BlockSize, (short)2, true
				, EnumSet.Of(HdfsDataOutputStream.SyncFlag.EndBlock));
		}

		/// <summary>
		/// The test calls
		/// <see cref="DoTheJob(Org.Apache.Hadoop.Conf.Configuration, string, long, short, bool, Sharpen.EnumSet{E})
		/// 	"/>
		/// while requiring the semantic of
		/// <see cref="Org.Apache.Hadoop.Hdfs.Client.HdfsDataOutputStream.SyncFlag.EndBlock"/
		/// 	>
		/// and
		/// <see cref="Org.Apache.Hadoop.Hdfs.Client.HdfsDataOutputStream.SyncFlag.UpdateLength
		/// 	"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void HSyncEndBlockAndUpdateLength()
		{
			DoTheJob(new HdfsConfiguration(), fName, AppendTestUtil.BlockSize, (short)2, true
				, EnumSet.Of(HdfsDataOutputStream.SyncFlag.EndBlock, HdfsDataOutputStream.SyncFlag
				.UpdateLength));
		}

		/// <summary>
		/// The test calls
		/// <see cref="DoTheJob(Org.Apache.Hadoop.Conf.Configuration, string, long, short, bool, Sharpen.EnumSet{E})
		/// 	"/>
		/// while requiring the semantic of
		/// <see cref="Org.Apache.Hadoop.Hdfs.Client.HdfsDataOutputStream.SyncFlag.UpdateLength
		/// 	"/>
		/// .
		/// Similar with
		/// <see cref="HFlush_02()"/>
		/// , it writes a file with a custom block
		/// size so the writes will be happening across block' boundaries
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void HSyncUpdateLength_02()
		{
			Configuration conf = new HdfsConfiguration();
			int customPerChecksumSize = 512;
			int customBlockSize = customPerChecksumSize * 3;
			// Modify defaul filesystem settings
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, customPerChecksumSize);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, customBlockSize);
			DoTheJob(conf, fName, customBlockSize, (short)2, true, EnumSet.Of(HdfsDataOutputStream.SyncFlag
				.UpdateLength));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void HSyncEndBlock_02()
		{
			Configuration conf = new HdfsConfiguration();
			int customPerChecksumSize = 512;
			int customBlockSize = customPerChecksumSize * 3;
			// Modify defaul filesystem settings
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, customPerChecksumSize);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, customBlockSize);
			DoTheJob(conf, fName, customBlockSize, (short)2, true, EnumSet.Of(HdfsDataOutputStream.SyncFlag
				.EndBlock));
		}

		/// <summary>
		/// The test calls
		/// <see cref="DoTheJob(Org.Apache.Hadoop.Conf.Configuration, string, long, short, bool, Sharpen.EnumSet{E})
		/// 	"/>
		/// while requiring the semantic of
		/// <see cref="Org.Apache.Hadoop.Hdfs.Client.HdfsDataOutputStream.SyncFlag.UpdateLength
		/// 	"/>
		/// .
		/// Similar with
		/// <see cref="HFlush_03()"/>
		/// , it writes a file with a custom block
		/// size so the writes will be happening across block's and checksum'
		/// boundaries.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void HSyncUpdateLength_03()
		{
			Configuration conf = new HdfsConfiguration();
			int customPerChecksumSize = 400;
			int customBlockSize = customPerChecksumSize * 3;
			// Modify defaul filesystem settings
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, customPerChecksumSize);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, customBlockSize);
			DoTheJob(conf, fName, customBlockSize, (short)2, true, EnumSet.Of(HdfsDataOutputStream.SyncFlag
				.UpdateLength));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void HSyncEndBlock_03()
		{
			Configuration conf = new HdfsConfiguration();
			int customPerChecksumSize = 400;
			int customBlockSize = customPerChecksumSize * 3;
			// Modify defaul filesystem settings
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, customPerChecksumSize);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, customBlockSize);
			DoTheJob(conf, fName, customBlockSize, (short)2, true, EnumSet.Of(HdfsDataOutputStream.SyncFlag
				.EndBlock));
		}

		/// <summary>
		/// The method starts new cluster with defined Configuration; creates a file
		/// with specified block_size and writes 10 equal sections in it; it also calls
		/// hflush/hsync after each write and throws an IOException in case of an error.
		/// </summary>
		/// <param name="conf">cluster configuration</param>
		/// <param name="fileName">of the file to be created and processed as required</param>
		/// <param name="block_size">value to be used for the file's creation</param>
		/// <param name="replicas">is the number of replicas</param>
		/// <param name="isSync">hsync or hflush</param>
		/// <param name="syncFlags">specify the semantic of the sync/flush</param>
		/// <exception cref="System.IO.IOException">in case of any errors</exception>
		public static void DoTheJob(Configuration conf, string fileName, long block_size, 
			short replicas, bool isSync, EnumSet<HdfsDataOutputStream.SyncFlag> syncFlags)
		{
			byte[] fileContent;
			int Sections = 10;
			fileContent = AppendTestUtil.InitBuffer(AppendTestUtil.FileSize);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(replicas).
				Build();
			// Make sure we work with DFS in order to utilize all its functionality
			DistributedFileSystem fileSystem = cluster.GetFileSystem();
			FSDataInputStream @is;
			try
			{
				Path path = new Path(fileName);
				string pathName = new Path(fileSystem.GetWorkingDirectory(), path).ToUri().GetPath
					();
				FSDataOutputStream stm = fileSystem.Create(path, false, 4096, replicas, block_size
					);
				System.Console.Out.WriteLine("Created file " + fileName);
				int tenth = AppendTestUtil.FileSize / Sections;
				int rounding = AppendTestUtil.FileSize - tenth * Sections;
				for (int i = 0; i < Sections; i++)
				{
					System.Console.Out.WriteLine("Writing " + (tenth * i) + " to " + (tenth * (i + 1)
						) + " section to file " + fileName);
					// write to the file
					stm.Write(fileContent, tenth * i, tenth);
					// Wait while hflush/hsync pushes all packets through built pipeline
					if (isSync)
					{
						((DFSOutputStream)stm.GetWrappedStream()).Hsync(syncFlags);
					}
					else
					{
						((DFSOutputStream)stm.GetWrappedStream()).Hflush();
					}
					// Check file length if updatelength is required
					if (isSync && syncFlags.Contains(HdfsDataOutputStream.SyncFlag.UpdateLength))
					{
						long currentFileLength = fileSystem.GetFileStatus(path).GetLen();
						NUnit.Framework.Assert.AreEqual("File size doesn't match for hsync/hflush with updating the length"
							, tenth * (i + 1), currentFileLength);
					}
					else
					{
						if (isSync && syncFlags.Contains(HdfsDataOutputStream.SyncFlag.EndBlock))
						{
							LocatedBlocks blocks = fileSystem.dfs.GetLocatedBlocks(pathName, 0);
							NUnit.Framework.Assert.AreEqual(i + 1, blocks.GetLocatedBlocks().Count);
						}
					}
					byte[] toRead = new byte[tenth];
					byte[] expected = new byte[tenth];
					System.Array.Copy(fileContent, tenth * i, expected, 0, tenth);
					// Open the same file for read. Need to create new reader after every write operation(!)
					@is = fileSystem.Open(path);
					@is.Seek(tenth * i);
					int readBytes = @is.Read(toRead, 0, tenth);
					System.Console.Out.WriteLine("Has read " + readBytes);
					NUnit.Framework.Assert.IsTrue("Should've get more bytes", (readBytes > 0) && (readBytes
						 <= tenth));
					@is.Close();
					CheckData(toRead, 0, readBytes, expected, "Partial verification");
				}
				System.Console.Out.WriteLine("Writing " + (tenth * Sections) + " to " + (tenth * 
					Sections + rounding) + " section to file " + fileName);
				stm.Write(fileContent, tenth * Sections, rounding);
				stm.Close();
				NUnit.Framework.Assert.AreEqual("File size doesn't match ", AppendTestUtil.FileSize
					, fileSystem.GetFileStatus(path).GetLen());
				AppendTestUtil.CheckFullFile(fileSystem, path, fileContent.Length, fileContent, "hflush()"
					);
			}
			finally
			{
				fileSystem.Close();
				cluster.Shutdown();
			}
		}

		internal static void CheckData(byte[] actual, int from, int len, byte[] expected, 
			string message)
		{
			for (int idx = 0; idx < len; idx++)
			{
				NUnit.Framework.Assert.AreEqual(message + " byte " + (from + idx) + " differs. expected "
					 + expected[from + idx] + " actual " + actual[idx], expected[from + idx], actual
					[idx]);
				actual[idx] = 0;
			}
		}

		/// <summary>
		/// This creates a slow writer and check to see
		/// if pipeline heartbeats work fine
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPipelineHeartbeat()
		{
			int DatanodeNum = 2;
			int fileLen = 6;
			Configuration conf = new HdfsConfiguration();
			int timeout = 2000;
			conf.SetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, timeout);
			Path p = new Path("/pipelineHeartbeat/foo");
			System.Console.Out.WriteLine("p=" + p);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(DatanodeNum
				).Build();
			try
			{
				DistributedFileSystem fs = cluster.GetFileSystem();
				byte[] fileContents = AppendTestUtil.InitBuffer(fileLen);
				// create a new file.
				FSDataOutputStream stm = AppendTestUtil.CreateFile(fs, p, DatanodeNum);
				stm.Write(fileContents, 0, 1);
				Sharpen.Thread.Sleep(timeout);
				stm.Hflush();
				System.Console.Out.WriteLine("Wrote 1 byte and hflush " + p);
				// write another byte
				Sharpen.Thread.Sleep(timeout);
				stm.Write(fileContents, 1, 1);
				stm.Hflush();
				stm.Write(fileContents, 2, 1);
				Sharpen.Thread.Sleep(timeout);
				stm.Hflush();
				stm.Write(fileContents, 3, 1);
				Sharpen.Thread.Sleep(timeout);
				stm.Write(fileContents, 4, 1);
				stm.Hflush();
				stm.Write(fileContents, 5, 1);
				Sharpen.Thread.Sleep(timeout);
				stm.Close();
				// verify that entire file is good
				AppendTestUtil.CheckFullFile(fs, p, fileLen, fileContents, "Failed to slowly write to a file"
					);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHFlushInterrupted()
		{
			int DatanodeNum = 2;
			int fileLen = 6;
			byte[] fileContents = AppendTestUtil.InitBuffer(fileLen);
			Configuration conf = new HdfsConfiguration();
			Path p = new Path("/hflush-interrupted");
			System.Console.Out.WriteLine("p=" + p);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(DatanodeNum
				).Build();
			try
			{
				DistributedFileSystem fs = cluster.GetFileSystem();
				// create a new file.
				FSDataOutputStream stm = AppendTestUtil.CreateFile(fs, p, DatanodeNum);
				stm.Write(fileContents, 0, 2);
				Sharpen.Thread.CurrentThread().Interrupt();
				try
				{
					stm.Hflush();
					// If we made it past the hflush(), then that means that the ack made it back
					// from the pipeline before we got to the wait() call. In that case we should
					// still have interrupted status.
					NUnit.Framework.Assert.IsTrue(Sharpen.Thread.Interrupted());
				}
				catch (ThreadInterruptedException)
				{
					System.Console.Out.WriteLine("Got expected exception during flush");
				}
				NUnit.Framework.Assert.IsFalse(Sharpen.Thread.Interrupted());
				// Try again to flush should succeed since we no longer have interrupt status
				stm.Hflush();
				// Write some more data and flush
				stm.Write(fileContents, 2, 2);
				stm.Hflush();
				// Write some data and close while interrupted
				stm.Write(fileContents, 4, 2);
				Sharpen.Thread.CurrentThread().Interrupt();
				try
				{
					stm.Close();
					// If we made it past the close(), then that means that the ack made it back
					// from the pipeline before we got to the wait() call. In that case we should
					// still have interrupted status.
					NUnit.Framework.Assert.IsTrue(Sharpen.Thread.Interrupted());
				}
				catch (ThreadInterruptedException)
				{
					System.Console.Out.WriteLine("Got expected exception during close");
					// If we got the exception, we shouldn't have interrupted status anymore.
					NUnit.Framework.Assert.IsFalse(Sharpen.Thread.Interrupted());
					// Now do a successful close.
					stm.Close();
				}
				// verify that entire file is good
				AppendTestUtil.CheckFullFile(fs, p, 4, fileContents, "Failed to deal with thread interruptions"
					, false);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		public TestHFlush()
		{
			{
				((Log4JLogger)DataNode.Log).GetLogger().SetLevel(Level.All);
				((Log4JLogger)DFSClient.Log).GetLogger().SetLevel(Level.All);
			}
		}
	}
}
