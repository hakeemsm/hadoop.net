using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests the building blocks that are needed to
	/// support HDFS appends.
	/// </summary>
	public class TestFileAppend2
	{
		internal const int numBlocks = 5;

		internal readonly bool simulatedStorage = false;

		private byte[] fileContents = null;

		internal readonly int numDatanodes = 6;

		internal readonly int numberOfFiles = 50;

		internal readonly int numThreads = 10;

		internal readonly int numAppendsPerThread = 20;

		internal TestFileAppend2.Workload[] workload = null;

		internal readonly AList<Path> testFiles = new AList<Path>();

		internal static volatile bool globalStatus = true;

		/// <summary>Creates one file, writes a few bytes to it and then closed it.</summary>
		/// <remarks>
		/// Creates one file, writes a few bytes to it and then closed it.
		/// Reopens the same file for appending, write all blocks and then close.
		/// Verify that all data exists in file.
		/// </remarks>
		/// <exception cref="System.IO.IOException">an exception might be thrown</exception>
		[NUnit.Framework.Test]
		public virtual void TestSimpleAppend()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsDatanodeHandlerCountKey, 50);
			fileContents = AppendTestUtil.InitBuffer(AppendTestUtil.FileSize);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				{
					// test appending to a file.
					// create a new file.
					Path file1 = new Path("/simpleAppend.dat");
					FSDataOutputStream stm = AppendTestUtil.CreateFile(fs, file1, 1);
					System.Console.Out.WriteLine("Created file simpleAppend.dat");
					// write to file
					int mid = 186;
					// io.bytes.per.checksum bytes
					System.Console.Out.WriteLine("Writing " + mid + " bytes to file " + file1);
					stm.Write(fileContents, 0, mid);
					stm.Close();
					System.Console.Out.WriteLine("Wrote and Closed first part of file.");
					// write to file
					int mid2 = 607;
					// io.bytes.per.checksum bytes
					System.Console.Out.WriteLine("Writing " + mid + " bytes to file " + file1);
					stm = fs.Append(file1);
					stm.Write(fileContents, mid, mid2 - mid);
					stm.Close();
					System.Console.Out.WriteLine("Wrote and Closed second part of file.");
					// write the remainder of the file
					stm = fs.Append(file1);
					// ensure getPos is set to reflect existing size of the file
					NUnit.Framework.Assert.IsTrue(stm.GetPos() > 0);
					System.Console.Out.WriteLine("Writing " + (AppendTestUtil.FileSize - mid2) + " bytes to file "
						 + file1);
					stm.Write(fileContents, mid2, AppendTestUtil.FileSize - mid2);
					System.Console.Out.WriteLine("Written second part of file");
					stm.Close();
					System.Console.Out.WriteLine("Wrote and Closed second part of file.");
					// verify that entire file is good
					AppendTestUtil.CheckFullFile(fs, file1, AppendTestUtil.FileSize, fileContents, "Read 2"
						);
				}
				{
					// test appending to an non-existing file.
					FSDataOutputStream @out = null;
					try
					{
						@out = fs.Append(new Path("/non-existing.dat"));
						NUnit.Framework.Assert.Fail("Expected to have FileNotFoundException");
					}
					catch (FileNotFoundException fnfe)
					{
						System.Console.Out.WriteLine("Good: got " + fnfe);
						Sharpen.Runtime.PrintStackTrace(fnfe, System.Console.Out);
					}
					finally
					{
						IOUtils.CloseStream(@out);
					}
				}
				{
					// test append permission.
					//set root to all writable 
					Path root = new Path("/");
					fs.SetPermission(root, new FsPermission((short)0x1ff));
					fs.Close();
					// login as a different user
					UserGroupInformation superuser = UserGroupInformation.GetCurrentUser();
					string username = "testappenduser";
					string group = "testappendgroup";
					NUnit.Framework.Assert.IsFalse(superuser.GetShortUserName().Equals(username));
					NUnit.Framework.Assert.IsFalse(Arrays.AsList(superuser.GetGroupNames()).Contains(
						group));
					UserGroupInformation appenduser = UserGroupInformation.CreateUserForTesting(username
						, new string[] { group });
					fs = DFSTestUtil.GetFileSystemAs(appenduser, conf);
					// create a file
					Path dir = new Path(root, GetType().Name);
					Path foo = new Path(dir, "foo.dat");
					FSDataOutputStream @out = null;
					int offset = 0;
					try
					{
						@out = fs.Create(foo);
						int len = 10 + AppendTestUtil.NextInt(100);
						@out.Write(fileContents, offset, len);
						offset += len;
					}
					finally
					{
						IOUtils.CloseStream(@out);
					}
					// change dir and foo to minimal permissions.
					fs.SetPermission(dir, new FsPermission((short)0x40));
					fs.SetPermission(foo, new FsPermission((short)0x80));
					// try append, should success
					@out = null;
					try
					{
						@out = fs.Append(foo);
						int len = 10 + AppendTestUtil.NextInt(100);
						@out.Write(fileContents, offset, len);
						offset += len;
					}
					finally
					{
						IOUtils.CloseStream(@out);
					}
					// change dir and foo to all but no write on foo.
					fs.SetPermission(foo, new FsPermission((short)0x17f));
					fs.SetPermission(dir, new FsPermission((short)0x1ff));
					// try append, should fail
					@out = null;
					try
					{
						@out = fs.Append(foo);
						NUnit.Framework.Assert.Fail("Expected to have AccessControlException");
					}
					catch (AccessControlException ace)
					{
						System.Console.Out.WriteLine("Good: got " + ace);
						Sharpen.Runtime.PrintStackTrace(ace, System.Console.Out);
					}
					finally
					{
						IOUtils.CloseStream(@out);
					}
				}
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

		/// <summary>Creates one file, writes a few bytes to it and then closed it.</summary>
		/// <remarks>
		/// Creates one file, writes a few bytes to it and then closed it.
		/// Reopens the same file for appending using append2 API, write all blocks and
		/// then close. Verify that all data exists in file.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleAppend2()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsDatanodeHandlerCountKey, 50);
			fileContents = AppendTestUtil.InitBuffer(AppendTestUtil.FileSize);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			DistributedFileSystem fs = cluster.GetFileSystem();
			try
			{
				{
					// test appending to a file.
					// create a new file.
					Path file1 = new Path("/simpleAppend.dat");
					FSDataOutputStream stm = AppendTestUtil.CreateFile(fs, file1, 1);
					System.Console.Out.WriteLine("Created file simpleAppend.dat");
					// write to file
					int mid = 186;
					// io.bytes.per.checksum bytes
					System.Console.Out.WriteLine("Writing " + mid + " bytes to file " + file1);
					stm.Write(fileContents, 0, mid);
					stm.Close();
					System.Console.Out.WriteLine("Wrote and Closed first part of file.");
					// write to file
					int mid2 = 607;
					// io.bytes.per.checksum bytes
					System.Console.Out.WriteLine("Writing " + mid + " bytes to file " + file1);
					stm = fs.Append(file1, EnumSet.Of(CreateFlag.Append, CreateFlag.NewBlock), 4096, 
						null);
					stm.Write(fileContents, mid, mid2 - mid);
					stm.Close();
					System.Console.Out.WriteLine("Wrote and Closed second part of file.");
					// write the remainder of the file
					stm = fs.Append(file1, EnumSet.Of(CreateFlag.Append, CreateFlag.NewBlock), 4096, 
						null);
					// ensure getPos is set to reflect existing size of the file
					NUnit.Framework.Assert.IsTrue(stm.GetPos() > 0);
					System.Console.Out.WriteLine("Writing " + (AppendTestUtil.FileSize - mid2) + " bytes to file "
						 + file1);
					stm.Write(fileContents, mid2, AppendTestUtil.FileSize - mid2);
					System.Console.Out.WriteLine("Written second part of file");
					stm.Close();
					System.Console.Out.WriteLine("Wrote and Closed second part of file.");
					// verify that entire file is good
					AppendTestUtil.CheckFullFile(fs, file1, AppendTestUtil.FileSize, fileContents, "Read 2"
						);
					// also make sure there three different blocks for the file
					IList<LocatedBlock> blocks = fs.GetClient().GetLocatedBlocks(file1.ToString(), 0L
						).GetLocatedBlocks();
					NUnit.Framework.Assert.AreEqual(12, blocks.Count);
					// the block size is 1024
					NUnit.Framework.Assert.AreEqual(mid, blocks[0].GetBlockSize());
					NUnit.Framework.Assert.AreEqual(mid2 - mid, blocks[1].GetBlockSize());
					for (int i = 2; i < 11; i++)
					{
						NUnit.Framework.Assert.AreEqual(AppendTestUtil.BlockSize, blocks[i].GetBlockSize(
							));
					}
					NUnit.Framework.Assert.AreEqual((AppendTestUtil.FileSize - mid2) % AppendTestUtil
						.BlockSize, blocks[11].GetBlockSize());
				}
				{
					// test appending to an non-existing file.
					FSDataOutputStream @out = null;
					try
					{
						@out = fs.Append(new Path("/non-existing.dat"), EnumSet.Of(CreateFlag.Append, CreateFlag
							.NewBlock), 4096, null);
						NUnit.Framework.Assert.Fail("Expected to have FileNotFoundException");
					}
					catch (FileNotFoundException fnfe)
					{
						System.Console.Out.WriteLine("Good: got " + fnfe);
						Sharpen.Runtime.PrintStackTrace(fnfe, System.Console.Out);
					}
					finally
					{
						IOUtils.CloseStream(@out);
					}
				}
				{
					// test append permission.
					// set root to all writable
					Path root = new Path("/");
					fs.SetPermission(root, new FsPermission((short)0x1ff));
					fs.Close();
					// login as a different user
					UserGroupInformation superuser = UserGroupInformation.GetCurrentUser();
					string username = "testappenduser";
					string group = "testappendgroup";
					NUnit.Framework.Assert.IsFalse(superuser.GetShortUserName().Equals(username));
					NUnit.Framework.Assert.IsFalse(Arrays.AsList(superuser.GetGroupNames()).Contains(
						group));
					UserGroupInformation appenduser = UserGroupInformation.CreateUserForTesting(username
						, new string[] { group });
					fs = (DistributedFileSystem)DFSTestUtil.GetFileSystemAs(appenduser, conf);
					// create a file
					Path dir = new Path(root, GetType().Name);
					Path foo = new Path(dir, "foo.dat");
					FSDataOutputStream @out = null;
					int offset = 0;
					try
					{
						@out = fs.Create(foo);
						int len = 10 + AppendTestUtil.NextInt(100);
						@out.Write(fileContents, offset, len);
						offset += len;
					}
					finally
					{
						IOUtils.CloseStream(@out);
					}
					// change dir and foo to minimal permissions.
					fs.SetPermission(dir, new FsPermission((short)0x40));
					fs.SetPermission(foo, new FsPermission((short)0x80));
					// try append, should success
					@out = null;
					try
					{
						@out = fs.Append(foo, EnumSet.Of(CreateFlag.Append, CreateFlag.NewBlock), 4096, null
							);
						int len = 10 + AppendTestUtil.NextInt(100);
						@out.Write(fileContents, offset, len);
						offset += len;
					}
					finally
					{
						IOUtils.CloseStream(@out);
					}
					// change dir and foo to all but no write on foo.
					fs.SetPermission(foo, new FsPermission((short)0x17f));
					fs.SetPermission(dir, new FsPermission((short)0x1ff));
					// try append, should fail
					@out = null;
					try
					{
						@out = fs.Append(foo, EnumSet.Of(CreateFlag.Append, CreateFlag.NewBlock), 4096, null
							);
						NUnit.Framework.Assert.Fail("Expected to have AccessControlException");
					}
					catch (AccessControlException ace)
					{
						System.Console.Out.WriteLine("Good: got " + ace);
						Sharpen.Runtime.PrintStackTrace(ace, System.Console.Out);
					}
					finally
					{
						IOUtils.CloseStream(@out);
					}
				}
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		internal class Workload : Sharpen.Thread
		{
			private readonly int id;

			private readonly MiniDFSCluster cluster;

			private readonly bool appendToNewBlock;

			internal Workload(TestFileAppend2 _enclosing, MiniDFSCluster cluster, int threadIndex
				, bool append2)
			{
				this._enclosing = _enclosing;
				//
				// an object that does a bunch of appends to files
				//
				this.id = threadIndex;
				this.cluster = cluster;
				this.appendToNewBlock = append2;
			}

			// create a bunch of files. Write to them and then verify.
			public override void Run()
			{
				System.Console.Out.WriteLine("Workload " + this.id + " starting... ");
				for (int i = 0; i < this._enclosing.numAppendsPerThread; i++)
				{
					// pick a file at random and remove it from pool
					Path testfile;
					lock (this._enclosing.testFiles)
					{
						if (this._enclosing.testFiles.Count == 0)
						{
							System.Console.Out.WriteLine("Completed write to almost all files.");
							return;
						}
						int index = AppendTestUtil.NextInt(this._enclosing.testFiles.Count);
						testfile = this._enclosing.testFiles.Remove(index);
					}
					long len = 0;
					int sizeToAppend = 0;
					try
					{
						DistributedFileSystem fs = this.cluster.GetFileSystem();
						// add a random number of bytes to file
						len = fs.GetFileStatus(testfile).GetLen();
						// if file is already full, then pick another file
						if (len >= AppendTestUtil.FileSize)
						{
							System.Console.Out.WriteLine("File " + testfile + " is full.");
							continue;
						}
						// do small size appends so that we can trigger multiple
						// appends to the same file.
						//
						int left = (int)(AppendTestUtil.FileSize - len) / 3;
						if (left <= 0)
						{
							left = 1;
						}
						sizeToAppend = AppendTestUtil.NextInt(left);
						System.Console.Out.WriteLine("Workload thread " + this.id + " appending " + sizeToAppend
							 + " bytes " + " to file " + testfile + " of size " + len);
						FSDataOutputStream stm = this.appendToNewBlock ? fs.Append(testfile, EnumSet.Of(CreateFlag
							.Append, CreateFlag.NewBlock), 4096, null) : fs.Append(testfile);
						stm.Write(this._enclosing.fileContents, (int)len, sizeToAppend);
						stm.Close();
						// wait for the file size to be reflected in the namenode metadata
						while (fs.GetFileStatus(testfile).GetLen() != (len + sizeToAppend))
						{
							try
							{
								System.Console.Out.WriteLine("Workload thread " + this.id + " file " + testfile +
									 " size " + fs.GetFileStatus(testfile).GetLen() + " expected size " + (len + sizeToAppend
									) + " waiting for namenode metadata update.");
								Sharpen.Thread.Sleep(5000);
							}
							catch (Exception)
							{
							}
						}
						NUnit.Framework.Assert.IsTrue("File " + testfile + " size is " + fs.GetFileStatus
							(testfile).GetLen() + " but expected " + (len + sizeToAppend), fs.GetFileStatus(
							testfile).GetLen() == (len + sizeToAppend));
						AppendTestUtil.CheckFullFile(fs, testfile, (int)(len + sizeToAppend), this._enclosing
							.fileContents, "Read 2");
					}
					catch (Exception e)
					{
						TestFileAppend2.globalStatus = false;
						if (e.ToString() != null)
						{
							System.Console.Out.WriteLine("Workload exception " + this.id + " testfile " + testfile
								 + " " + e);
							Sharpen.Runtime.PrintStackTrace(e);
						}
						NUnit.Framework.Assert.IsTrue("Workload exception " + this.id + " testfile " + testfile
							 + " expected size " + (len + sizeToAppend), false);
					}
					// Add testfile back to the pool of files.
					lock (this._enclosing.testFiles)
					{
						this._enclosing.testFiles.AddItem(testfile);
					}
				}
			}

			private readonly TestFileAppend2 _enclosing;
		}

		/// <summary>Test that appends to files at random offsets.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void TestComplexAppend(bool appendToNewBlock)
		{
			fileContents = AppendTestUtil.InitBuffer(AppendTestUtil.FileSize);
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 2000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 2);
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, 2);
			conf.SetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, 30000);
			conf.SetInt(DFSConfigKeys.DfsDatanodeSocketWriteTimeoutKey, 30000);
			conf.SetInt(DFSConfigKeys.DfsDatanodeHandlerCountKey, 50);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes
				).Build();
			cluster.WaitActive();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				// create a bunch of test files with random replication factors.
				// Insert them into a linked list.
				//
				for (int i = 0; i < numberOfFiles; i++)
				{
					int replication = AppendTestUtil.NextInt(numDatanodes - 2) + 1;
					Path testFile = new Path("/" + i + ".dat");
					FSDataOutputStream stm = AppendTestUtil.CreateFile(fs, testFile, replication);
					stm.Close();
					testFiles.AddItem(testFile);
				}
				// Create threads and make them run workload concurrently.
				workload = new TestFileAppend2.Workload[numThreads];
				for (int i_1 = 0; i_1 < numThreads; i_1++)
				{
					workload[i_1] = new TestFileAppend2.Workload(this, cluster, i_1, appendToNewBlock
						);
					workload[i_1].Start();
				}
				// wait for all transactions to get over
				for (int i_2 = 0; i_2 < numThreads; i_2++)
				{
					try
					{
						System.Console.Out.WriteLine("Waiting for thread " + i_2 + " to complete...");
						workload[i_2].Join();
						System.Console.Out.WriteLine("Waiting for thread " + i_2 + " complete.");
					}
					catch (Exception)
					{
						i_2--;
					}
				}
			}
			finally
			{
				// retry
				fs.Close();
				cluster.Shutdown();
			}
			// If any of the worker thread failed in their job, indicate that
			// this test failed.
			//
			NUnit.Framework.Assert.IsTrue("testComplexAppend Worker encountered exceptions.", 
				globalStatus);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestComplexAppend()
		{
			TestComplexAppend(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestComplexAppend2()
		{
			TestComplexAppend(true);
		}

		/// <summary>
		/// Make sure when the block length after appending is less than 512 bytes, the
		/// checksum re-calculation and overwrite are performed correctly.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppendLessThanChecksumChunk()
		{
			byte[] buf = new byte[1024];
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).NumDataNodes
				(1).Build();
			cluster.WaitActive();
			try
			{
				using (DistributedFileSystem fs = cluster.GetFileSystem())
				{
					int len1 = 200;
					int len2 = 300;
					Path p = new Path("/foo");
					FSDataOutputStream @out = fs.Create(p);
					@out.Write(buf, 0, len1);
					@out.Close();
					@out = fs.Append(p);
					@out.Write(buf, 0, len2);
					// flush but leave open
					@out.Hflush();
					// read data to verify the replica's content and checksum are correct
					FSDataInputStream @in = fs.Open(p);
					int length = @in.Read(0, buf, 0, len1 + len2);
					NUnit.Framework.Assert.IsTrue(length > 0);
					@in.Close();
					@out.Close();
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		public TestFileAppend2()
		{
			{
				DFSTestUtil.SetNameNodeLogLevel(Level.All);
				GenericTestUtils.SetLogLevel(DataNode.Log, Level.All);
				GenericTestUtils.SetLogLevel(DFSClient.Log, Level.All);
			}
		}
	}
}
