using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests that a file need not be closed before its
	/// data can be read by another client.
	/// </summary>
	public class TestDatanodeDeath
	{
		internal const int blockSize = 8192;

		internal const int numBlocks = 2;

		internal const int fileSize = numBlocks * blockSize + 1;

		internal const int numDatanodes = 15;

		internal const short replication = 3;

		internal readonly int numberOfFiles = 3;

		internal readonly int numThreads = 5;

		internal TestDatanodeDeath.Workload[] workload = null;

		internal class Workload : Sharpen.Thread
		{
			private readonly short replication;

			private readonly int numberOfFiles;

			private readonly int id;

			private readonly FileSystem fs;

			private long stamp;

			private readonly long myseed;

			internal Workload(long myseed, FileSystem fs, int threadIndex, int numberOfFiles, 
				short replication, long stamp)
			{
				//
				// an object that does a bunch of transactions
				//
				this.myseed = myseed;
				id = threadIndex;
				this.fs = fs;
				this.numberOfFiles = numberOfFiles;
				this.replication = replication;
				this.stamp = stamp;
			}

			// create a bunch of files. Write to them and then verify.
			public override void Run()
			{
				System.Console.Out.WriteLine("Workload starting ");
				for (int i = 0; i < numberOfFiles; i++)
				{
					Path filename = new Path(id + "." + i);
					try
					{
						System.Console.Out.WriteLine("Workload processing file " + filename);
						FSDataOutputStream stm = CreateFile(fs, filename, replication);
						DFSOutputStream dfstream = (DFSOutputStream)(stm.GetWrappedStream());
						dfstream.SetArtificialSlowdown(1000);
						WriteFile(stm, myseed);
						stm.Close();
						CheckFile(fs, filename, replication, numBlocks, fileSize, myseed);
					}
					catch (Exception e)
					{
						System.Console.Out.WriteLine("Workload exception " + e);
						NUnit.Framework.Assert.IsTrue(e.ToString(), false);
					}
					// increment the stamp to indicate that another file is done.
					lock (this)
					{
						stamp++;
					}
				}
			}

			public virtual void ResetStamp()
			{
				lock (this)
				{
					this.stamp = 0;
				}
			}

			public virtual long GetStamp()
			{
				lock (this)
				{
					return stamp;
				}
			}
		}

		//
		// creates a file and returns a descriptor for writing to it.
		//
		/// <exception cref="System.IO.IOException"/>
		private static FSDataOutputStream CreateFile(FileSystem fileSys, Path name, short
			 repl)
		{
			// create and write a file that contains three blocks of data
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), repl, blockSize);
			return stm;
		}

		//
		// writes to file
		//
		/// <exception cref="System.IO.IOException"/>
		private static void WriteFile(FSDataOutputStream stm, long seed)
		{
			byte[] buffer = AppendTestUtil.RandomBytes(seed, fileSize);
			int mid = fileSize / 2;
			stm.Write(buffer, 0, mid);
			stm.Write(buffer, mid, fileSize - mid);
		}

		//
		// verify that the data written are sane
		// 
		/// <exception cref="System.IO.IOException"/>
		private static void CheckFile(FileSystem fileSys, Path name, int repl, int numblocks
			, int filesize, long seed)
		{
			bool done = false;
			int attempt = 0;
			long len = fileSys.GetFileStatus(name).GetLen();
			NUnit.Framework.Assert.IsTrue(name + " should be of size " + filesize + " but found to be of size "
				 + len, len == filesize);
			// wait till all full blocks are confirmed by the datanodes.
			while (!done)
			{
				attempt++;
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
				done = true;
				BlockLocation[] locations = fileSys.GetFileBlockLocations(fileSys.GetFileStatus(name
					), 0, filesize);
				if (locations.Length < numblocks)
				{
					if (attempt > 100)
					{
						System.Console.Out.WriteLine("File " + name + " has only " + locations.Length + " blocks, "
							 + " but is expected to have " + numblocks + " blocks.");
					}
					done = false;
					continue;
				}
				for (int idx = 0; idx < locations.Length; idx++)
				{
					if (locations[idx].GetHosts().Length < repl)
					{
						if (attempt > 100)
						{
							System.Console.Out.WriteLine("File " + name + " has " + locations.Length + " blocks: "
								 + " The " + idx + " block has only " + locations[idx].GetHosts().Length + " replicas but is expected to have "
								 + repl + " replicas.");
						}
						done = false;
						break;
					}
				}
			}
			FSDataInputStream stm = fileSys.Open(name);
			byte[] expected = AppendTestUtil.RandomBytes(seed, fileSize);
			// do a sanity check. Read the file
			byte[] actual = new byte[filesize];
			stm.ReadFully(0, actual);
			CheckData(actual, 0, expected, "Read 1");
		}

		private static void CheckData(byte[] actual, int from, byte[] expected, string message
			)
		{
			for (int idx = 0; idx < actual.Length; idx++)
			{
				NUnit.Framework.Assert.AreEqual(message + " byte " + (from + idx) + " differs. expected "
					 + expected[from + idx] + " actual " + actual[idx], actual[idx], expected[from +
					 idx]);
				actual[idx] = 0;
			}
		}

		/// <summary>A class that kills one datanode and recreates a new one.</summary>
		/// <remarks>
		/// A class that kills one datanode and recreates a new one. It waits to
		/// ensure that that all workers have finished at least one file since the
		/// last kill of a datanode. This guarantees that all three replicas of
		/// a block do not get killed (otherwise the file will be corrupt and the
		/// test will fail).
		/// </remarks>
		internal class Modify : Sharpen.Thread
		{
			internal volatile bool running;

			internal readonly MiniDFSCluster cluster;

			internal readonly Configuration conf;

			internal Modify(TestDatanodeDeath _enclosing, Configuration conf, MiniDFSCluster 
				cluster)
			{
				this._enclosing = _enclosing;
				this.running = true;
				this.cluster = cluster;
				this.conf = conf;
			}

			public override void Run()
			{
				while (this.running)
				{
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
						continue;
					}
					// check if all threads have a new stamp. 
					// If so, then all workers have finished at least one file
					// since the last stamp.
					bool loop = false;
					for (int i = 0; i < this._enclosing.numThreads; i++)
					{
						if (this._enclosing.workload[i].GetStamp() == 0)
						{
							loop = true;
							break;
						}
					}
					if (loop)
					{
						continue;
					}
					// Now it is guaranteed that there will be at least one valid
					// replica of a file.
					for (int i_1 = 0; i_1 < TestDatanodeDeath.replication - 1; i_1++)
					{
						// pick a random datanode to shutdown
						int victim = AppendTestUtil.NextInt(TestDatanodeDeath.numDatanodes);
						try
						{
							System.Console.Out.WriteLine("Stopping datanode " + victim);
							this.cluster.RestartDataNode(victim);
						}
						catch (IOException e)
						{
							// cluster.startDataNodes(conf, 1, true, null, null);
							System.Console.Out.WriteLine("TestDatanodeDeath Modify exception " + e);
							NUnit.Framework.Assert.IsTrue("TestDatanodeDeath Modify exception " + e, false);
							this.running = false;
						}
					}
					// set a new stamp for all workers
					for (int i_2 = 0; i_2 < this._enclosing.numThreads; i_2++)
					{
						this._enclosing.workload[i_2].ResetStamp();
					}
				}
			}

			// Make the thread exit.
			internal virtual void Close()
			{
				this.running = false;
				this.Interrupt();
			}

			private readonly TestDatanodeDeath _enclosing;
		}

		/// <summary>
		/// Test that writing to files is good even when datanodes in the pipeline
		/// dies.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void ComplexTest()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 2000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 2);
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, 2);
			conf.SetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, 5000);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes
				).Build();
			cluster.WaitActive();
			FileSystem fs = cluster.GetFileSystem();
			TestDatanodeDeath.Modify modThread = null;
			try
			{
				// Create threads and make them run workload concurrently.
				workload = new TestDatanodeDeath.Workload[numThreads];
				for (int i = 0; i < numThreads; i++)
				{
					workload[i] = new TestDatanodeDeath.Workload(AppendTestUtil.NextLong(), fs, i, numberOfFiles
						, replication, 0);
					workload[i].Start();
				}
				// Create a thread that kills existing datanodes and creates new ones.
				modThread = new TestDatanodeDeath.Modify(this, conf, cluster);
				modThread.Start();
				// wait for all transactions to get over
				for (int i_1 = 0; i_1 < numThreads; i_1++)
				{
					try
					{
						System.Console.Out.WriteLine("Waiting for thread " + i_1 + " to complete...");
						workload[i_1].Join();
						// if most of the threads are done, then stop restarting datanodes.
						if (i_1 >= numThreads / 2)
						{
							modThread.Close();
						}
					}
					catch (Exception)
					{
						i_1--;
					}
				}
			}
			finally
			{
				// retry
				if (modThread != null)
				{
					modThread.Close();
					try
					{
						modThread.Join();
					}
					catch (Exception)
					{
					}
				}
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Write to one file, then kill one datanode in the pipeline and then
		/// close the file.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void SimpleTest(int datanodeToKill)
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 2000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, 2);
			conf.SetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, 5000);
			int myMaxNodes = 5;
			System.Console.Out.WriteLine("SimpleTest starting with DataNode to Kill " + datanodeToKill
				);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(myMaxNodes
				).Build();
			cluster.WaitActive();
			FileSystem fs = cluster.GetFileSystem();
			short repl = 3;
			Path filename = new Path("simpletest.dat");
			try
			{
				// create a file and write one block of data
				System.Console.Out.WriteLine("SimpleTest creating file " + filename);
				FSDataOutputStream stm = CreateFile(fs, filename, repl);
				DFSOutputStream dfstream = (DFSOutputStream)(stm.GetWrappedStream());
				// these are test settings
				dfstream.SetChunksPerPacket(5);
				dfstream.SetArtificialSlowdown(3000);
				long myseed = AppendTestUtil.NextLong();
				byte[] buffer = AppendTestUtil.RandomBytes(myseed, fileSize);
				int mid = fileSize / 4;
				stm.Write(buffer, 0, mid);
				DatanodeInfo[] targets = dfstream.GetPipeline();
				int count = 5;
				while (count-- > 0 && targets == null)
				{
					try
					{
						System.Console.Out.WriteLine("SimpleTest: Waiting for pipeline to be created.");
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
					}
					targets = dfstream.GetPipeline();
				}
				if (targets == null)
				{
					int victim = AppendTestUtil.NextInt(myMaxNodes);
					System.Console.Out.WriteLine("SimpleTest stopping datanode random " + victim);
					cluster.StopDataNode(victim);
				}
				else
				{
					int victim = datanodeToKill;
					System.Console.Out.WriteLine("SimpleTest stopping datanode " + targets[victim]);
					cluster.StopDataNode(targets[victim].GetXferAddr());
				}
				System.Console.Out.WriteLine("SimpleTest stopping datanode complete");
				// write some more data to file, close and verify
				stm.Write(buffer, mid, fileSize - mid);
				stm.Close();
				CheckFile(fs, filename, repl, numBlocks, fileSize, myseed);
			}
			catch (Exception e)
			{
				System.Console.Out.WriteLine("Simple Workload exception " + e);
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.IsTrue(e.ToString(), false);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSimple0()
		{
			SimpleTest(0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSimple1()
		{
			SimpleTest(1);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSimple2()
		{
			SimpleTest(2);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestComplex()
		{
			ComplexTest();
		}

		public TestDatanodeDeath()
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
