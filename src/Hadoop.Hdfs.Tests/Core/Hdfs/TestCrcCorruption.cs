using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>A JUnit test for corrupted file handling.</summary>
	/// <remarks>
	/// A JUnit test for corrupted file handling.
	/// This test creates a bunch of files/directories with replication
	/// factor of 2. Then verifies that a client can automatically
	/// access the remaining valid replica inspite of the following
	/// types of simulated errors:
	/// 1. Delete meta file on one replica
	/// 2. Truncates meta file on one replica
	/// 3. Corrupts the meta file header on one replica
	/// 4. Corrupts any random offset and portion of the meta file
	/// 5. Swaps two meta files, i.e the format of the meta files
	/// are valid but their CRCs do not match with their corresponding
	/// data blocks
	/// The above tests are run for varied values of dfs.bytes-per-checksum
	/// and dfs.blocksize. It tests for the case when the meta file is
	/// multiple blocks.
	/// Another portion of the test is commented out till HADOOP-1557
	/// is addressed:
	/// 1. Create file with 2 replica, corrupt the meta file of replica,
	/// decrease replication factor from 2 to 1. Validate that the
	/// remaining replica is the good one.
	/// 2. Create file with 2 replica, corrupt the meta file of one replica,
	/// increase replication factor of file to 3. verify that the new
	/// replica was created from the non-corrupted replica.
	/// </remarks>
	public class TestCrcCorruption
	{
		private DFSClientFaultInjector faultInjector;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			faultInjector = Org.Mockito.Mockito.Mock<DFSClientFaultInjector>();
			DFSClientFaultInjector.instance = faultInjector;
		}

		/// <summary>
		/// Test case for data corruption during data transmission for
		/// create/write.
		/// </summary>
		/// <remarks>
		/// Test case for data corruption during data transmission for
		/// create/write. To recover from corruption while writing, at
		/// least two replicas are needed.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestCorruptionDuringWrt()
		{
			Configuration conf = new HdfsConfiguration();
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(10).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				Path file = new Path("/test_corruption_file");
				FSDataOutputStream @out = fs.Create(file, true, 8192, (short)3, (long)(128 * 1024
					 * 1024));
				byte[] data = new byte[65536];
				for (int i = 0; i < 65536; i++)
				{
					data[i] = unchecked((byte)(i % 256));
				}
				for (int i_1 = 0; i_1 < 5; i_1++)
				{
					@out.Write(data, 0, 65535);
				}
				@out.Hflush();
				// corrupt the packet once
				Org.Mockito.Mockito.When(faultInjector.CorruptPacket()).ThenReturn(true, false);
				Org.Mockito.Mockito.When(faultInjector.UncorruptPacket()).ThenReturn(true, false);
				for (int i_2 = 0; i_2 < 5; i_2++)
				{
					@out.Write(data, 0, 65535);
				}
				@out.Close();
				// read should succeed
				FSDataInputStream @in = fs.Open(file);
				for (int c; (c = @in.Read()) != -1; )
				{
				}
				@in.Close();
				// test the retry limit
				@out = fs.Create(file, true, 8192, (short)3, (long)(128 * 1024 * 1024));
				// corrupt the packet once and never fix it.
				Org.Mockito.Mockito.When(faultInjector.CorruptPacket()).ThenReturn(true, false);
				Org.Mockito.Mockito.When(faultInjector.UncorruptPacket()).ThenReturn(false);
				// the client should give up pipeline reconstruction after retries.
				try
				{
					for (int i_3 = 0; i_3 < 5; i_3++)
					{
						@out.Write(data, 0, 65535);
					}
					@out.Close();
					NUnit.Framework.Assert.Fail("Write did not fail");
				}
				catch (IOException ioe)
				{
					// we should get an ioe
					DFSClient.Log.Info("Got expected exception", ioe);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				Org.Mockito.Mockito.When(faultInjector.CorruptPacket()).ThenReturn(false);
				Org.Mockito.Mockito.When(faultInjector.UncorruptPacket()).ThenReturn(false);
			}
		}

		/// <summary>check if DFS can handle corrupted CRC blocks</summary>
		/// <exception cref="System.Exception"/>
		private void Thistest(Configuration conf, DFSTestUtil util)
		{
			MiniDFSCluster cluster = null;
			int numDataNodes = 2;
			short replFactor = 2;
			Random random = new Random();
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				util.CreateFiles(fs, "/srcdat", replFactor);
				util.WaitReplication(fs, "/srcdat", (short)2);
				// Now deliberately remove/truncate meta blocks from the first
				// directory of the first datanode. The complete absense of a meta
				// file disallows this Datanode to send data to another datanode.
				// However, a client is alowed access to this block.
				//
				FilePath storageDir = cluster.GetInstanceStorageDir(0, 1);
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				FilePath data_dir = MiniDFSCluster.GetFinalizedDir(storageDir, bpid);
				NUnit.Framework.Assert.IsTrue("data directory does not exist", data_dir.Exists());
				FilePath[] blocks = data_dir.ListFiles();
				NUnit.Framework.Assert.IsTrue("Blocks do not exist in data-dir", (blocks != null)
					 && (blocks.Length > 0));
				int num = 0;
				for (int idx = 0; idx < blocks.Length; idx++)
				{
					if (blocks[idx].GetName().StartsWith(Block.BlockFilePrefix) && blocks[idx].GetName
						().EndsWith(".meta"))
					{
						num++;
						if (num % 3 == 0)
						{
							//
							// remove .meta file
							//
							System.Console.Out.WriteLine("Deliberately removing file " + blocks[idx].GetName(
								));
							NUnit.Framework.Assert.IsTrue("Cannot remove file.", blocks[idx].Delete());
						}
						else
						{
							if (num % 3 == 1)
							{
								//
								// shorten .meta file
								//
								RandomAccessFile file = new RandomAccessFile(blocks[idx], "rw");
								FileChannel channel = file.GetChannel();
								int newsize = random.Next((int)channel.Size() / 2);
								System.Console.Out.WriteLine("Deliberately truncating file " + blocks[idx].GetName
									() + " to size " + newsize + " bytes.");
								channel.Truncate(newsize);
								file.Close();
							}
							else
							{
								//
								// corrupt a few bytes of the metafile
								//
								RandomAccessFile file = new RandomAccessFile(blocks[idx], "rw");
								FileChannel channel = file.GetChannel();
								long position = 0;
								//
								// The very first time, corrupt the meta header at offset 0
								//
								if (num != 2)
								{
									position = (long)random.Next((int)channel.Size());
								}
								int length = random.Next((int)(channel.Size() - position + 1));
								byte[] buffer = new byte[length];
								random.NextBytes(buffer);
								channel.Write(ByteBuffer.Wrap(buffer), position);
								System.Console.Out.WriteLine("Deliberately corrupting file " + blocks[idx].GetName
									() + " at offset " + position + " length " + length);
								file.Close();
							}
						}
					}
				}
				//
				// Now deliberately corrupt all meta blocks from the second
				// directory of the first datanode
				//
				storageDir = cluster.GetInstanceStorageDir(0, 1);
				data_dir = MiniDFSCluster.GetFinalizedDir(storageDir, bpid);
				NUnit.Framework.Assert.IsTrue("data directory does not exist", data_dir.Exists());
				blocks = data_dir.ListFiles();
				NUnit.Framework.Assert.IsTrue("Blocks do not exist in data-dir", (blocks != null)
					 && (blocks.Length > 0));
				int count = 0;
				FilePath previous = null;
				for (int idx_1 = 0; idx_1 < blocks.Length; idx_1++)
				{
					if (blocks[idx_1].GetName().StartsWith("blk_") && blocks[idx_1].GetName().EndsWith
						(".meta"))
					{
						//
						// Move the previous metafile into the current one.
						//
						count++;
						if (count % 2 == 0)
						{
							System.Console.Out.WriteLine("Deliberately insertimg bad crc into files " + blocks
								[idx_1].GetName() + " " + previous.GetName());
							NUnit.Framework.Assert.IsTrue("Cannot remove file.", blocks[idx_1].Delete());
							NUnit.Framework.Assert.IsTrue("Cannot corrupt meta file.", previous.RenameTo(blocks
								[idx_1]));
							NUnit.Framework.Assert.IsTrue("Cannot recreate empty meta file.", previous.CreateNewFile
								());
							previous = null;
						}
						else
						{
							previous = blocks[idx_1];
						}
					}
				}
				//
				// Only one replica is possibly corrupted. The other replica should still
				// be good. Verify.
				//
				NUnit.Framework.Assert.IsTrue("Corrupted replicas not handled properly.", util.CheckFiles
					(fs, "/srcdat"));
				System.Console.Out.WriteLine("All File still have a valid replica");
				//
				// set replication factor back to 1. This causes only one replica of
				// of each block to remain in HDFS. The check is to make sure that 
				// the corrupted replica generated above is the one that gets deleted.
				// This test is currently disabled until HADOOP-1557 is solved.
				//
				util.SetReplication(fs, "/srcdat", (short)1);
				//util.waitReplication(fs, "/srcdat", (short)1);
				//System.out.println("All Files done with removing replicas");
				//assertTrue("Excess replicas deleted. Corrupted replicas found.",
				//           util.checkFiles(fs, "/srcdat"));
				System.Console.Out.WriteLine("The excess-corrupted-replica test is disabled " + " pending HADOOP-1557"
					);
				util.Cleanup(fs, "/srcdat");
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCrcCorruption()
		{
			//
			// default parameters
			//
			System.Console.Out.WriteLine("TestCrcCorruption with default parameters");
			Configuration conf1 = new HdfsConfiguration();
			conf1.SetInt(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 3 * 1000);
			DFSTestUtil util1 = new DFSTestUtil.Builder().SetName("TestCrcCorruption").SetNumFiles
				(40).Build();
			Thistest(conf1, util1);
			//
			// specific parameters
			//
			System.Console.Out.WriteLine("TestCrcCorruption with specific parameters");
			Configuration conf2 = new HdfsConfiguration();
			conf2.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, 17);
			conf2.SetInt(DFSConfigKeys.DfsBlockSizeKey, 34);
			DFSTestUtil util2 = new DFSTestUtil.Builder().SetName("TestCrcCorruption").SetNumFiles
				(40).SetMaxSize(400).Build();
			Thistest(conf2, util2);
		}

		/// <summary>
		/// Make a single-DN cluster, corrupt a block, and make sure
		/// there's no infinite loop, but rather it eventually
		/// reports the exception to the client.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestEntirelyCorruptFileOneNode()
		{
			// 5 min timeout
			DoTestEntirelyCorruptFile(1);
		}

		/// <summary>
		/// Same thing with multiple datanodes - in history, this has
		/// behaved differently than the above.
		/// </summary>
		/// <remarks>
		/// Same thing with multiple datanodes - in history, this has
		/// behaved differently than the above.
		/// This test usually completes in around 15 seconds - if it
		/// times out, this suggests that the client is retrying
		/// indefinitely.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestEntirelyCorruptFileThreeNodes()
		{
			// 5 min timeout
			DoTestEntirelyCorruptFile(3);
		}

		/// <exception cref="System.Exception"/>
		private void DoTestEntirelyCorruptFile(int numDataNodes)
		{
			long fileSize = 4096;
			Path file = new Path("/testFile");
			short replFactor = (short)numDataNodes;
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, numDataNodes);
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes
				).Build();
			try
			{
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, file, fileSize, replFactor, 12345L);
				/*seed*/
				DFSTestUtil.WaitReplication(fs, file, replFactor);
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, file);
				int blockFilesCorrupted = cluster.CorruptBlockOnDataNodes(block);
				NUnit.Framework.Assert.AreEqual("All replicas not corrupted", replFactor, blockFilesCorrupted
					);
				try
				{
					IOUtils.CopyBytes(fs.Open(file), new IOUtils.NullOutputStream(), conf, true);
					NUnit.Framework.Assert.Fail("Didn't get exception");
				}
				catch (IOException ioe)
				{
					DFSClient.Log.Info("Got expected exception", ioe);
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
