using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests the building blocks that are needed to
	/// support HDFS appends.
	/// </summary>
	public class TestFileAppend
	{
		internal readonly bool simulatedStorage = false;

		private static byte[] fileContents = null;

		//
		// writes to file but does not close it
		//
		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FSDataOutputStream stm)
		{
			byte[] buffer = AppendTestUtil.InitBuffer(AppendTestUtil.FileSize);
			stm.Write(buffer);
		}

		//
		// verify that the data written to the full blocks are sane
		// 
		/// <exception cref="System.IO.IOException"/>
		private void CheckFile(FileSystem fileSys, Path name, int repl)
		{
			bool done = false;
			// wait till all full blocks are confirmed by the datanodes.
			while (!done)
			{
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
				done = true;
				BlockLocation[] locations = fileSys.GetFileBlockLocations(fileSys.GetFileStatus(name
					), 0, AppendTestUtil.FileSize);
				if (locations.Length < AppendTestUtil.NumBlocks)
				{
					System.Console.Out.WriteLine("Number of blocks found " + locations.Length);
					done = false;
					continue;
				}
				for (int idx = 0; idx < AppendTestUtil.NumBlocks; idx++)
				{
					if (locations[idx].GetHosts().Length < repl)
					{
						System.Console.Out.WriteLine("Block index " + idx + " not yet replciated.");
						done = false;
						break;
					}
				}
			}
			byte[] expected = new byte[AppendTestUtil.NumBlocks * AppendTestUtil.BlockSize];
			System.Array.Copy(fileContents, 0, expected, 0, expected.Length);
			// do a sanity check. Read the file
			// do not check file status since the file is not yet closed.
			AppendTestUtil.CheckFullFile(fileSys, name, AppendTestUtil.NumBlocks * AppendTestUtil
				.BlockSize, expected, "Read 1", false);
		}

		/// <summary>Test that copy on write for blocks works correctly</summary>
		/// <exception cref="System.IO.IOException">an exception might be thrown</exception>
		[NUnit.Framework.Test]
		public virtual void TestCopyOnWrite()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			IPEndPoint addr = new IPEndPoint("localhost", cluster.GetNameNodePort());
			DFSClient client = new DFSClient(addr, conf);
			try
			{
				// create a new file, write to it and close it.
				//
				Path file1 = new Path("/filestatus.dat");
				FSDataOutputStream stm = AppendTestUtil.CreateFile(fs, file1, 1);
				WriteFile(stm);
				stm.Close();
				// Get a handle to the datanode
				DataNode[] dn = cluster.ListDataNodes();
				NUnit.Framework.Assert.IsTrue("There should be only one datanode but found " + dn
					.Length, dn.Length == 1);
				LocatedBlocks locations = client.GetNamenode().GetBlockLocations(file1.ToString()
					, 0, long.MaxValue);
				IList<LocatedBlock> blocks = locations.GetLocatedBlocks();
				//
				// Create hard links for a few of the blocks
				//
				for (int i = 0; i < blocks.Count; i = i + 2)
				{
					ExtendedBlock b = blocks[i].GetBlock();
					FilePath f = DataNodeTestUtils.GetFile(dn[0], b.GetBlockPoolId(), b.GetLocalBlock
						().GetBlockId());
					FilePath link = new FilePath(f.ToString() + ".link");
					System.Console.Out.WriteLine("Creating hardlink for File " + f + " to " + link);
					HardLink.CreateHardLink(f, link);
				}
				//
				// Detach all blocks. This should remove hardlinks (if any)
				//
				for (int i_1 = 0; i_1 < blocks.Count; i_1++)
				{
					ExtendedBlock b = blocks[i_1].GetBlock();
					System.Console.Out.WriteLine("testCopyOnWrite detaching block " + b);
					NUnit.Framework.Assert.IsTrue("Detaching block " + b + " should have returned true"
						, DataNodeTestUtils.UnlinkBlock(dn[0], b, 1));
				}
				// Since the blocks were already detached earlier, these calls should
				// return false
				//
				for (int i_2 = 0; i_2 < blocks.Count; i_2++)
				{
					ExtendedBlock b = blocks[i_2].GetBlock();
					System.Console.Out.WriteLine("testCopyOnWrite detaching block " + b);
					NUnit.Framework.Assert.IsTrue("Detaching block " + b + " should have returned false"
						, !DataNodeTestUtils.UnlinkBlock(dn[0], b, 1));
				}
			}
			finally
			{
				client.Close();
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Test a simple flush on a simple HDFS file.</summary>
		/// <exception cref="System.IO.IOException">an exception might be thrown</exception>
		[NUnit.Framework.Test]
		public virtual void TestSimpleFlush()
		{
			Configuration conf = new HdfsConfiguration();
			fileContents = AppendTestUtil.InitBuffer(AppendTestUtil.FileSize);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				// create a new file.
				Path file1 = new Path("/simpleFlush.dat");
				FSDataOutputStream stm = AppendTestUtil.CreateFile(fs, file1, 1);
				System.Console.Out.WriteLine("Created file simpleFlush.dat");
				// write to file
				int mid = AppendTestUtil.FileSize / 2;
				stm.Write(fileContents, 0, mid);
				stm.Hflush();
				System.Console.Out.WriteLine("Wrote and Flushed first part of file.");
				// write the remainder of the file
				stm.Write(fileContents, mid, AppendTestUtil.FileSize - mid);
				System.Console.Out.WriteLine("Written second part of file");
				stm.Hflush();
				stm.Hflush();
				System.Console.Out.WriteLine("Wrote and Flushed second part of file.");
				// verify that full blocks are sane
				CheckFile(fs, file1, 1);
				stm.Close();
				System.Console.Out.WriteLine("Closed file.");
				// verify that entire file is good
				AppendTestUtil.CheckFullFile(fs, file1, AppendTestUtil.FileSize, fileContents, "Read 2"
					);
			}
			catch (IOException e)
			{
				System.Console.Out.WriteLine("Exception :" + e);
				throw;
			}
			catch (Exception e)
			{
				System.Console.Out.WriteLine("Throwable :" + e);
				Sharpen.Runtime.PrintStackTrace(e);
				throw new IOException("Throwable : " + e);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Test that file data can be flushed.</summary>
		/// <exception cref="System.IO.IOException">an exception might be thrown</exception>
		[NUnit.Framework.Test]
		public virtual void TestComplexFlush()
		{
			Configuration conf = new HdfsConfiguration();
			fileContents = AppendTestUtil.InitBuffer(AppendTestUtil.FileSize);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				// create a new file.
				Path file1 = new Path("/complexFlush.dat");
				FSDataOutputStream stm = AppendTestUtil.CreateFile(fs, file1, 1);
				System.Console.Out.WriteLine("Created file complexFlush.dat");
				int start = 0;
				for (start = 0; (start + 29) < AppendTestUtil.FileSize; )
				{
					stm.Write(fileContents, start, 29);
					stm.Hflush();
					start += 29;
				}
				stm.Write(fileContents, start, AppendTestUtil.FileSize - start);
				// need to make sure we completely write out all full blocks before
				// the checkFile() call (see FSOutputSummer#flush)
				stm.Flush();
				// verify that full blocks are sane
				CheckFile(fs, file1, 1);
				stm.Close();
				// verify that entire file is good
				AppendTestUtil.CheckFullFile(fs, file1, AppendTestUtil.FileSize, fileContents, "Read 2"
					);
			}
			catch (IOException e)
			{
				System.Console.Out.WriteLine("Exception :" + e);
				throw;
			}
			catch (Exception e)
			{
				System.Console.Out.WriteLine("Throwable :" + e);
				Sharpen.Runtime.PrintStackTrace(e);
				throw new IOException("Throwable : " + e);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>FileNotFoundException is expected for appending to a non-exisiting file</summary>
		/// <exception cref="System.IO.FileNotFoundException">as the result</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFileNotFound()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				Path file1 = new Path("/nonexistingfile.dat");
				fs.Append(file1);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Test two consecutive appends on a file with a full block.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppendTwice()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs1 = cluster.GetFileSystem();
			FileSystem fs2 = AppendTestUtil.CreateHdfsWithDifferentUsername(conf);
			try
			{
				Path p = new Path("/testAppendTwice/foo");
				int len = 1 << 16;
				byte[] fileContents = AppendTestUtil.InitBuffer(len);
				{
					// create a new file with a full block.
					FSDataOutputStream @out = fs2.Create(p, true, 4096, (short)1, len);
					@out.Write(fileContents, 0, len);
					@out.Close();
				}
				//1st append does not add any data so that the last block remains full
				//and the last block in INodeFileUnderConstruction is a BlockInfo
				//but not BlockInfoUnderConstruction. 
				fs2.Append(p);
				//2nd append should get AlreadyBeingCreatedException
				fs1.Append(p);
				NUnit.Framework.Assert.Fail();
			}
			catch (RemoteException re)
			{
				AppendTestUtil.Log.Info("Got an exception:", re);
				NUnit.Framework.Assert.AreEqual(typeof(AlreadyBeingCreatedException).FullName, re
					.GetClassName());
			}
			finally
			{
				fs2.Close();
				fs1.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Test two consecutive appends on a file with a full block.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppend2Twice()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			DistributedFileSystem fs1 = cluster.GetFileSystem();
			FileSystem fs2 = AppendTestUtil.CreateHdfsWithDifferentUsername(conf);
			try
			{
				Path p = new Path("/testAppendTwice/foo");
				int len = 1 << 16;
				byte[] fileContents = AppendTestUtil.InitBuffer(len);
				{
					// create a new file with a full block.
					FSDataOutputStream @out = fs2.Create(p, true, 4096, (short)1, len);
					@out.Write(fileContents, 0, len);
					@out.Close();
				}
				//1st append does not add any data so that the last block remains full
				//and the last block in INodeFileUnderConstruction is a BlockInfo
				//but not BlockInfoUnderConstruction.
				((DistributedFileSystem)fs2).Append(p, EnumSet.Of(CreateFlag.Append, CreateFlag.NewBlock
					), 4096, null);
				// 2nd append should get AlreadyBeingCreatedException
				fs1.Append(p);
				NUnit.Framework.Assert.Fail();
			}
			catch (RemoteException re)
			{
				AppendTestUtil.Log.Info("Got an exception:", re);
				NUnit.Framework.Assert.AreEqual(typeof(AlreadyBeingCreatedException).FullName, re
					.GetClassName());
			}
			finally
			{
				fs2.Close();
				fs1.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Tests appending after soft-limit expires.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppendAfterSoftLimit()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, 1);
			conf.SetBoolean(DFSConfigKeys.DfsSupportAppendKey, true);
			//Set small soft-limit for lease
			long softLimit = 1L;
			long hardLimit = 9999999L;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.SetLeasePeriod(softLimit, hardLimit);
			cluster.WaitActive();
			FileSystem fs = cluster.GetFileSystem();
			FileSystem fs2 = new DistributedFileSystem();
			fs2.Initialize(fs.GetUri(), conf);
			Path testPath = new Path("/testAppendAfterSoftLimit");
			byte[] fileContents = AppendTestUtil.InitBuffer(32);
			// create a new file without closing
			FSDataOutputStream @out = fs.Create(testPath);
			@out.Write(fileContents);
			//Wait for > soft-limit
			Sharpen.Thread.Sleep(250);
			try
			{
				FSDataOutputStream appendStream2 = fs2.Append(testPath);
				appendStream2.Write(fileContents);
				appendStream2.Close();
				NUnit.Framework.Assert.AreEqual(fileContents.Length, fs.GetFileStatus(testPath).GetLen
					());
			}
			finally
			{
				fs.Close();
				fs2.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Tests appending after soft-limit expires.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppend2AfterSoftLimit()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, 1);
			//Set small soft-limit for lease
			long softLimit = 1L;
			long hardLimit = 9999999L;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.SetLeasePeriod(softLimit, hardLimit);
			cluster.WaitActive();
			DistributedFileSystem fs = cluster.GetFileSystem();
			DistributedFileSystem fs2 = new DistributedFileSystem();
			fs2.Initialize(fs.GetUri(), conf);
			Path testPath = new Path("/testAppendAfterSoftLimit");
			byte[] fileContents = AppendTestUtil.InitBuffer(32);
			// create a new file without closing
			FSDataOutputStream @out = fs.Create(testPath);
			@out.Write(fileContents);
			//Wait for > soft-limit
			Sharpen.Thread.Sleep(250);
			try
			{
				FSDataOutputStream appendStream2 = fs2.Append(testPath, EnumSet.Of(CreateFlag.Append
					, CreateFlag.NewBlock), 4096, null);
				appendStream2.Write(fileContents);
				appendStream2.Close();
				NUnit.Framework.Assert.AreEqual(fileContents.Length, fs.GetFileStatus(testPath).GetLen
					());
				// make sure we now have 1 block since the first writer was revoked
				LocatedBlocks blks = fs.GetClient().GetLocatedBlocks(testPath.ToString(), 0L);
				NUnit.Framework.Assert.AreEqual(1, blks.GetLocatedBlocks().Count);
				foreach (LocatedBlock blk in blks.GetLocatedBlocks())
				{
					NUnit.Framework.Assert.AreEqual(fileContents.Length, blk.GetBlockSize());
				}
			}
			finally
			{
				fs.Close();
				fs2.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Old replica of the block should not be accepted as valid for append/read
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailedAppendBlockRejection()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set("dfs.client.block.write.replace-datanode-on-failure.enable", "false");
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			DistributedFileSystem fs = null;
			try
			{
				fs = cluster.GetFileSystem();
				Path path = new Path("/test");
				FSDataOutputStream @out = fs.Create(path);
				@out.WriteBytes("hello\n");
				@out.Close();
				// stop one datanode
				MiniDFSCluster.DataNodeProperties dnProp = cluster.StopDataNode(0);
				string dnAddress = dnProp.datanode.GetXferAddress().ToString();
				if (dnAddress.StartsWith("/"))
				{
					dnAddress = Sharpen.Runtime.Substring(dnAddress, 1);
				}
				// append again to bump genstamps
				for (int i = 0; i < 2; i++)
				{
					@out = fs.Append(path);
					@out.WriteBytes("helloagain\n");
					@out.Close();
				}
				// re-open and make the block state as underconstruction
				@out = fs.Append(path);
				cluster.RestartDataNode(dnProp, true);
				// wait till the block report comes
				Sharpen.Thread.Sleep(2000);
				// check the block locations, this should not contain restarted datanode
				BlockLocation[] locations = fs.GetFileBlockLocations(path, 0, long.MaxValue);
				string[] names = locations[0].GetNames();
				foreach (string node in names)
				{
					if (node.Equals(dnAddress))
					{
						NUnit.Framework.Assert.Fail("Failed append should not be present in latest block locations."
							);
					}
				}
				@out.Close();
			}
			finally
			{
				IOUtils.CloseStream(fs);
				cluster.Shutdown();
			}
		}

		/// <summary>Old replica of the block should not be accepted as valid for append/read
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultiAppend2()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set("dfs.client.block.write.replace-datanode-on-failure.enable", "false");
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			DistributedFileSystem fs = null;
			string hello = "hello\n";
			try
			{
				fs = cluster.GetFileSystem();
				Path path = new Path("/test");
				FSDataOutputStream @out = fs.Create(path);
				@out.WriteBytes(hello);
				@out.Close();
				// stop one datanode
				MiniDFSCluster.DataNodeProperties dnProp = cluster.StopDataNode(0);
				string dnAddress = dnProp.datanode.GetXferAddress().ToString();
				if (dnAddress.StartsWith("/"))
				{
					dnAddress = Sharpen.Runtime.Substring(dnAddress, 1);
				}
				// append again to bump genstamps
				for (int i = 0; i < 2; i++)
				{
					@out = fs.Append(path, EnumSet.Of(CreateFlag.Append, CreateFlag.NewBlock), 4096, 
						null);
					@out.WriteBytes(hello);
					@out.Close();
				}
				// re-open and make the block state as underconstruction
				@out = fs.Append(path, EnumSet.Of(CreateFlag.Append, CreateFlag.NewBlock), 4096, 
					null);
				cluster.RestartDataNode(dnProp, true);
				// wait till the block report comes
				Sharpen.Thread.Sleep(2000);
				@out.WriteBytes(hello);
				@out.Close();
				// check the block locations
				LocatedBlocks blocks = fs.GetClient().GetLocatedBlocks(path.ToString(), 0L);
				// since we append the file 3 time, we should be 4 blocks
				NUnit.Framework.Assert.AreEqual(4, blocks.GetLocatedBlocks().Count);
				foreach (LocatedBlock block in blocks.GetLocatedBlocks())
				{
					NUnit.Framework.Assert.AreEqual(hello.Length, block.GetBlockSize());
				}
				StringBuilder sb = new StringBuilder();
				for (int i_1 = 0; i_1 < 4; i_1++)
				{
					sb.Append(hello);
				}
				byte[] content = Sharpen.Runtime.GetBytesForString(sb.ToString());
				AppendTestUtil.CheckFullFile(fs, path, content.Length, content, "Read /test");
				// restart namenode to make sure the editlog can be properly applied
				cluster.RestartNameNode(true);
				cluster.WaitActive();
				AppendTestUtil.CheckFullFile(fs, path, content.Length, content, "Read /test");
				blocks = fs.GetClient().GetLocatedBlocks(path.ToString(), 0L);
				// since we append the file 3 time, we should be 4 blocks
				NUnit.Framework.Assert.AreEqual(4, blocks.GetLocatedBlocks().Count);
				foreach (LocatedBlock block_1 in blocks.GetLocatedBlocks())
				{
					NUnit.Framework.Assert.AreEqual(hello.Length, block_1.GetBlockSize());
				}
			}
			finally
			{
				IOUtils.CloseStream(fs);
				cluster.Shutdown();
			}
		}
	}
}
