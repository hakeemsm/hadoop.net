using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class implements some of tests posted in HADOOP-2658.</summary>
	public class TestFileAppend3
	{
		internal const long BlockSize = 64 * 1024;

		internal const short Replication = 3;

		internal const int DatanodeNum = 5;

		private static Configuration conf;

		private static int buffersize;

		private static MiniDFSCluster cluster;

		private static DistributedFileSystem fs;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			AppendTestUtil.Log.Info("setUp()");
			conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, 512);
			buffersize = conf.GetInt(CommonConfigurationKeys.IoFileBufferSizeKey, 4096);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(DatanodeNum).Build();
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			AppendTestUtil.Log.Info("tearDown()");
			if (fs != null)
			{
				fs.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>TC1: Append on block boundary.</summary>
		/// <exception cref="System.IO.IOException">an exception might be thrown</exception>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTC1()
		{
			Path p = new Path("/TC1/foo");
			System.Console.Out.WriteLine("p=" + p);
			//a. Create file and write one block of data. Close file.
			int len1 = (int)BlockSize;
			{
				FSDataOutputStream @out = fs.Create(p, false, buffersize, Replication, BlockSize);
				AppendTestUtil.Write(@out, 0, len1);
				@out.Close();
			}
			//   Reopen file to append. Append half block of data. Close file.
			int len2 = (int)BlockSize / 2;
			{
				FSDataOutputStream @out = fs.Append(p);
				AppendTestUtil.Write(@out, len1, len2);
				@out.Close();
			}
			//b. Reopen file and read 1.5 blocks worth of data. Close file.
			AppendTestUtil.Check(fs, p, len1 + len2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTC1ForAppend2()
		{
			Path p = new Path("/TC1/foo2");
			//a. Create file and write one block of data. Close file.
			int len1 = (int)BlockSize;
			{
				FSDataOutputStream @out = fs.Create(p, false, buffersize, Replication, BlockSize);
				AppendTestUtil.Write(@out, 0, len1);
				@out.Close();
			}
			// Reopen file to append. Append half block of data. Close file.
			int len2 = (int)BlockSize / 2;
			{
				FSDataOutputStream @out = fs.Append(p, EnumSet.Of(CreateFlag.Append, CreateFlag.NewBlock
					), 4096, null);
				AppendTestUtil.Write(@out, len1, len2);
				@out.Close();
			}
			// b. Reopen file and read 1.5 blocks worth of data. Close file.
			AppendTestUtil.Check(fs, p, len1 + len2);
		}

		/// <summary>TC2: Append on non-block boundary.</summary>
		/// <exception cref="System.IO.IOException">an exception might be thrown</exception>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTC2()
		{
			Path p = new Path("/TC2/foo");
			System.Console.Out.WriteLine("p=" + p);
			//a. Create file with one and a half block of data. Close file.
			int len1 = (int)(BlockSize + BlockSize / 2);
			{
				FSDataOutputStream @out = fs.Create(p, false, buffersize, Replication, BlockSize);
				AppendTestUtil.Write(@out, 0, len1);
				@out.Close();
			}
			AppendTestUtil.Check(fs, p, len1);
			//   Reopen file to append quarter block of data. Close file.
			int len2 = (int)BlockSize / 4;
			{
				FSDataOutputStream @out = fs.Append(p);
				AppendTestUtil.Write(@out, len1, len2);
				@out.Close();
			}
			//b. Reopen file and read 1.75 blocks of data. Close file.
			AppendTestUtil.Check(fs, p, len1 + len2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTC2ForAppend2()
		{
			Path p = new Path("/TC2/foo2");
			//a. Create file with one and a half block of data. Close file.
			int len1 = (int)(BlockSize + BlockSize / 2);
			{
				FSDataOutputStream @out = fs.Create(p, false, buffersize, Replication, BlockSize);
				AppendTestUtil.Write(@out, 0, len1);
				@out.Close();
			}
			AppendTestUtil.Check(fs, p, len1);
			//   Reopen file to append quarter block of data. Close file.
			int len2 = (int)BlockSize / 4;
			{
				FSDataOutputStream @out = fs.Append(p, EnumSet.Of(CreateFlag.Append, CreateFlag.NewBlock
					), 4096, null);
				AppendTestUtil.Write(@out, len1, len2);
				@out.Close();
			}
			// b. Reopen file and read 1.75 blocks of data. Close file.
			AppendTestUtil.Check(fs, p, len1 + len2);
			IList<LocatedBlock> blocks = fs.GetClient().GetLocatedBlocks(p.ToString(), 0L).GetLocatedBlocks
				();
			NUnit.Framework.Assert.AreEqual(3, blocks.Count);
			NUnit.Framework.Assert.AreEqual(BlockSize, blocks[0].GetBlockSize());
			NUnit.Framework.Assert.AreEqual(BlockSize / 2, blocks[1].GetBlockSize());
			NUnit.Framework.Assert.AreEqual(BlockSize / 4, blocks[2].GetBlockSize());
		}

		/// <summary>TC5: Only one simultaneous append.</summary>
		/// <exception cref="System.IO.IOException">an exception might be thrown</exception>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTC5()
		{
			Path p = new Path("/TC5/foo");
			System.Console.Out.WriteLine("p=" + p);
			{
				//a. Create file on Machine M1. Write half block to it. Close file.
				FSDataOutputStream @out = fs.Create(p, false, buffersize, Replication, BlockSize);
				AppendTestUtil.Write(@out, 0, (int)(BlockSize / 2));
				@out.Close();
			}
			//b. Reopen file in "append" mode on Machine M1.
			FSDataOutputStream out_1 = fs.Append(p);
			//c. On Machine M2, reopen file in "append" mode. This should fail.
			try
			{
				AppendTestUtil.CreateHdfsWithDifferentUsername(conf).Append(p);
				NUnit.Framework.Assert.Fail("This should fail.");
			}
			catch (IOException ioe)
			{
				AppendTestUtil.Log.Info("GOOD: got an exception", ioe);
			}
			try
			{
				((DistributedFileSystem)AppendTestUtil.CreateHdfsWithDifferentUsername(conf)).Append
					(p, EnumSet.Of(CreateFlag.Append, CreateFlag.NewBlock), 4096, null);
				NUnit.Framework.Assert.Fail("This should fail.");
			}
			catch (IOException ioe)
			{
				AppendTestUtil.Log.Info("GOOD: got an exception", ioe);
			}
			//d. On Machine M1, close file.
			out_1.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTC5ForAppend2()
		{
			Path p = new Path("/TC5/foo2");
			{
				// a. Create file on Machine M1. Write half block to it. Close file.
				FSDataOutputStream @out = fs.Create(p, false, buffersize, Replication, BlockSize);
				AppendTestUtil.Write(@out, 0, (int)(BlockSize / 2));
				@out.Close();
			}
			// b. Reopen file in "append" mode on Machine M1.
			FSDataOutputStream out_1 = fs.Append(p, EnumSet.Of(CreateFlag.Append, CreateFlag.
				NewBlock), 4096, null);
			// c. On Machine M2, reopen file in "append" mode. This should fail.
			try
			{
				((DistributedFileSystem)AppendTestUtil.CreateHdfsWithDifferentUsername(conf)).Append
					(p, EnumSet.Of(CreateFlag.Append, CreateFlag.NewBlock), 4096, null);
				NUnit.Framework.Assert.Fail("This should fail.");
			}
			catch (IOException ioe)
			{
				AppendTestUtil.Log.Info("GOOD: got an exception", ioe);
			}
			try
			{
				AppendTestUtil.CreateHdfsWithDifferentUsername(conf).Append(p);
				NUnit.Framework.Assert.Fail("This should fail.");
			}
			catch (IOException ioe)
			{
				AppendTestUtil.Log.Info("GOOD: got an exception", ioe);
			}
			// d. On Machine M1, close file.
			out_1.Close();
		}

		/// <summary>TC7: Corrupted replicas are present.</summary>
		/// <exception cref="System.IO.IOException">an exception might be thrown</exception>
		/// <exception cref="System.Exception"/>
		private void TestTC7(bool appendToNewBlock)
		{
			short repl = 2;
			Path p = new Path("/TC7/foo" + (appendToNewBlock ? "0" : "1"));
			System.Console.Out.WriteLine("p=" + p);
			//a. Create file with replication factor of 2. Write half block of data. Close file.
			int len1 = (int)(BlockSize / 2);
			{
				FSDataOutputStream @out = fs.Create(p, false, buffersize, repl, BlockSize);
				AppendTestUtil.Write(@out, 0, len1);
				@out.Close();
			}
			DFSTestUtil.WaitReplication(fs, p, repl);
			//b. Log into one datanode that has one replica of this block.
			//   Find the block file on this datanode and truncate it to zero size.
			LocatedBlocks locatedblocks = fs.dfs.GetNamenode().GetBlockLocations(p.ToString()
				, 0L, len1);
			NUnit.Framework.Assert.AreEqual(1, locatedblocks.LocatedBlockCount());
			LocatedBlock lb = locatedblocks.Get(0);
			ExtendedBlock blk = lb.GetBlock();
			NUnit.Framework.Assert.AreEqual(len1, lb.GetBlockSize());
			DatanodeInfo[] datanodeinfos = lb.GetLocations();
			NUnit.Framework.Assert.AreEqual(repl, datanodeinfos.Length);
			DataNode dn = cluster.GetDataNode(datanodeinfos[0].GetIpcPort());
			FilePath f = DataNodeTestUtils.GetBlockFile(dn, blk.GetBlockPoolId(), blk.GetLocalBlock
				());
			RandomAccessFile raf = new RandomAccessFile(f, "rw");
			AppendTestUtil.Log.Info("dn=" + dn + ", blk=" + blk + " (length=" + blk.GetNumBytes
				() + ")");
			NUnit.Framework.Assert.AreEqual(len1, raf.Length());
			raf.SetLength(0);
			raf.Close();
			//c. Open file in "append mode".  Append a new block worth of data. Close file.
			int len2 = (int)BlockSize;
			{
				FSDataOutputStream @out = appendToNewBlock ? fs.Append(p, EnumSet.Of(CreateFlag.Append
					, CreateFlag.NewBlock), 4096, null) : fs.Append(p);
				AppendTestUtil.Write(@out, len1, len2);
				@out.Close();
			}
			//d. Reopen file and read two blocks worth of data.
			AppendTestUtil.Check(fs, p, len1 + len2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTC7()
		{
			TestTC7(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTC7ForAppend2()
		{
			TestTC7(true);
		}

		/// <summary>TC11: Racing rename</summary>
		/// <exception cref="System.Exception"/>
		private void TestTC11(bool appendToNewBlock)
		{
			Path p = new Path("/TC11/foo" + (appendToNewBlock ? "0" : "1"));
			System.Console.Out.WriteLine("p=" + p);
			//a. Create file and write one block of data. Close file.
			int len1 = (int)BlockSize;
			{
				FSDataOutputStream @out = fs.Create(p, false, buffersize, Replication, BlockSize);
				AppendTestUtil.Write(@out, 0, len1);
				@out.Close();
			}
			//b. Reopen file in "append" mode. Append half block of data.
			FSDataOutputStream out_1 = appendToNewBlock ? fs.Append(p, EnumSet.Of(CreateFlag.
				Append, CreateFlag.NewBlock), 4096, null) : fs.Append(p);
			int len2 = (int)BlockSize / 2;
			AppendTestUtil.Write(out_1, len1, len2);
			out_1.Hflush();
			//c. Rename file to file.new.
			Path pnew = new Path(p + ".new");
			NUnit.Framework.Assert.IsTrue(fs.Rename(p, pnew));
			//d. Close file handle that was opened in (b). 
			out_1.Close();
			//check block sizes
			long len = fs.GetFileStatus(pnew).GetLen();
			LocatedBlocks locatedblocks = fs.dfs.GetNamenode().GetBlockLocations(pnew.ToString
				(), 0L, len);
			int numblock = locatedblocks.LocatedBlockCount();
			for (int i = 0; i < numblock; i++)
			{
				LocatedBlock lb = locatedblocks.Get(i);
				ExtendedBlock blk = lb.GetBlock();
				long size = lb.GetBlockSize();
				if (i < numblock - 1)
				{
					NUnit.Framework.Assert.AreEqual(BlockSize, size);
				}
				foreach (DatanodeInfo datanodeinfo in lb.GetLocations())
				{
					DataNode dn = cluster.GetDataNode(datanodeinfo.GetIpcPort());
					Block metainfo = DataNodeTestUtils.GetFSDataset(dn).GetStoredBlock(blk.GetBlockPoolId
						(), blk.GetBlockId());
					NUnit.Framework.Assert.AreEqual(size, metainfo.GetNumBytes());
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTC11()
		{
			TestTC11(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTC11ForAppend2()
		{
			TestTC11(true);
		}

		/// <summary>TC12: Append to partial CRC chunk</summary>
		/// <exception cref="System.Exception"/>
		private void TestTC12(bool appendToNewBlock)
		{
			Path p = new Path("/TC12/foo" + (appendToNewBlock ? "0" : "1"));
			System.Console.Out.WriteLine("p=" + p);
			//a. Create file with a block size of 64KB
			//   and a default io.bytes.per.checksum of 512 bytes.
			//   Write 25687 bytes of data. Close file.
			int len1 = 25687;
			{
				FSDataOutputStream @out = fs.Create(p, false, buffersize, Replication, BlockSize);
				AppendTestUtil.Write(@out, 0, len1);
				@out.Close();
			}
			//b. Reopen file in "append" mode. Append another 5877 bytes of data. Close file.
			int len2 = 5877;
			{
				FSDataOutputStream @out = appendToNewBlock ? fs.Append(p, EnumSet.Of(CreateFlag.Append
					, CreateFlag.NewBlock), 4096, null) : fs.Append(p);
				AppendTestUtil.Write(@out, len1, len2);
				@out.Close();
			}
			//c. Reopen file and read 25687+5877 bytes of data from file. Close file.
			AppendTestUtil.Check(fs, p, len1 + len2);
			if (appendToNewBlock)
			{
				LocatedBlocks blks = fs.dfs.GetLocatedBlocks(p.ToString(), 0);
				NUnit.Framework.Assert.AreEqual(2, blks.GetLocatedBlocks().Count);
				NUnit.Framework.Assert.AreEqual(len1, blks.GetLocatedBlocks()[0].GetBlockSize());
				NUnit.Framework.Assert.AreEqual(len2, blks.GetLocatedBlocks()[1].GetBlockSize());
				AppendTestUtil.Check(fs, p, 0, len1);
				AppendTestUtil.Check(fs, p, len1, len2);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTC12()
		{
			TestTC12(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTC12ForAppend2()
		{
			TestTC12(true);
		}

		/// <summary>
		/// Append to a partial CRC chunk and the first write does not fill up the
		/// partial CRC trunk
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void TestAppendToPartialChunk(bool appendToNewBlock)
		{
			Path p = new Path("/partialChunk/foo" + (appendToNewBlock ? "0" : "1"));
			int fileLen = 513;
			System.Console.Out.WriteLine("p=" + p);
			byte[] fileContents = AppendTestUtil.InitBuffer(fileLen);
			// create a new file.
			FSDataOutputStream stm = AppendTestUtil.CreateFile(fs, p, 1);
			// create 1 byte file
			stm.Write(fileContents, 0, 1);
			stm.Close();
			System.Console.Out.WriteLine("Wrote 1 byte and closed the file " + p);
			// append to file
			stm = appendToNewBlock ? fs.Append(p, EnumSet.Of(CreateFlag.Append, CreateFlag.NewBlock
				), 4096, null) : fs.Append(p);
			// Append to a partial CRC trunk
			stm.Write(fileContents, 1, 1);
			stm.Hflush();
			// The partial CRC trunk is not full yet and close the file
			stm.Close();
			System.Console.Out.WriteLine("Append 1 byte and closed the file " + p);
			// write the remainder of the file
			stm = appendToNewBlock ? fs.Append(p, EnumSet.Of(CreateFlag.Append, CreateFlag.NewBlock
				), 4096, null) : fs.Append(p);
			// ensure getPos is set to reflect existing size of the file
			NUnit.Framework.Assert.AreEqual(2, stm.GetPos());
			// append to a partial CRC trunk
			stm.Write(fileContents, 2, 1);
			// The partial chunk is not full yet, force to send a packet to DN
			stm.Hflush();
			System.Console.Out.WriteLine("Append and flush 1 byte");
			// The partial chunk is not full yet, force to send another packet to DN
			stm.Write(fileContents, 3, 2);
			stm.Hflush();
			System.Console.Out.WriteLine("Append and flush 2 byte");
			// fill up the partial chunk and close the file
			stm.Write(fileContents, 5, fileLen - 5);
			stm.Close();
			System.Console.Out.WriteLine("Flush 508 byte and closed the file " + p);
			// verify that entire file is good
			AppendTestUtil.CheckFullFile(fs, p, fileLen, fileContents, "Failed to append to a partial chunk"
				);
		}

		// Do small appends.
		/// <exception cref="System.IO.IOException"/>
		internal virtual void DoSmallAppends(Path file, DistributedFileSystem fs, int iterations
			)
		{
			for (int i = 0; i < iterations; i++)
			{
				FSDataOutputStream stm;
				try
				{
					stm = fs.Append(file);
				}
				catch (IOException)
				{
					// If another thread is already appending, skip this time.
					continue;
				}
				// Failure in write or close will be terminal.
				AppendTestUtil.Write(stm, 0, 123);
				stm.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSmallAppendRace()
		{
			Path file = new Path("/testSmallAppendRace");
			string fName = file.ToUri().GetPath();
			// Create the file and write a small amount of data.
			FSDataOutputStream stm = fs.Create(file);
			AppendTestUtil.Write(stm, 0, 123);
			stm.Close();
			// Introduce a delay between getFileInfo and calling append() against NN.
			DFSClient client = DFSClientAdapter.GetDFSClient(fs);
			DFSClient spyClient = Org.Mockito.Mockito.Spy(client);
			Org.Mockito.Mockito.When(spyClient.GetFileInfo(fName)).ThenAnswer(new _Answer_548
				(client, fName));
			DFSClientAdapter.SetDFSClient(fs, spyClient);
			// Create two threads for doing appends to the same file.
			Sharpen.Thread worker1 = new _Thread_564(this, file);
			Sharpen.Thread worker2 = new _Thread_574(this, file);
			worker1.Start();
			worker2.Start();
			// append will fail when the file size crosses the checksum chunk boundary,
			// if append was called with a stale file stat.
			DoSmallAppends(file, fs, 20);
		}

		private sealed class _Answer_548 : Answer<HdfsFileStatus>
		{
			public _Answer_548(DFSClient client, string fName)
			{
				this.client = client;
				this.fName = fName;
			}

			public HdfsFileStatus Answer(InvocationOnMock invocation)
			{
				try
				{
					HdfsFileStatus stat = client.GetFileInfo(fName);
					Sharpen.Thread.Sleep(100);
					return stat;
				}
				catch (Exception)
				{
					return null;
				}
			}

			private readonly DFSClient client;

			private readonly string fName;
		}

		private sealed class _Thread_564 : Sharpen.Thread
		{
			public _Thread_564(TestFileAppend3 _enclosing, Path file)
			{
				this._enclosing = _enclosing;
				this.file = file;
			}

			public override void Run()
			{
				try
				{
					this._enclosing.DoSmallAppends(file, TestFileAppend3.fs, 20);
				}
				catch (IOException)
				{
				}
			}

			private readonly TestFileAppend3 _enclosing;

			private readonly Path file;
		}

		private sealed class _Thread_574 : Sharpen.Thread
		{
			public _Thread_574(TestFileAppend3 _enclosing, Path file)
			{
				this._enclosing = _enclosing;
				this.file = file;
			}

			public override void Run()
			{
				try
				{
					this._enclosing.DoSmallAppends(file, TestFileAppend3.fs, 20);
				}
				catch (IOException)
				{
				}
			}

			private readonly TestFileAppend3 _enclosing;

			private readonly Path file;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAppendToPartialChunk()
		{
			TestAppendToPartialChunk(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAppendToPartialChunkforAppend2()
		{
			TestAppendToPartialChunk(true);
		}

		public TestFileAppend3()
		{
			{
				DFSTestUtil.SetNameNodeLogLevel(Level.All);
				GenericTestUtils.SetLogLevel(DataNode.Log, Level.All);
				GenericTestUtils.SetLogLevel(DFSClient.Log, Level.All);
				GenericTestUtils.SetLogLevel(InterDatanodeProtocol.Log, Level.All);
			}
		}
	}
}
