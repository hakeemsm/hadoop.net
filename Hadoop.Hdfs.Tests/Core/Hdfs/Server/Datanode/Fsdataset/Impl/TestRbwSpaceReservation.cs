using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Com.Google.Common.Base;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>
	/// Ensure that the DN reserves disk space equivalent to a full block for
	/// replica being written (RBW).
	/// </summary>
	public class TestRbwSpaceReservation
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestRbwSpaceReservation
			));

		private const int DuRefreshIntervalMsec = 500;

		private const int StoragesPerDatanode = 1;

		private const int BlockSize = 1024 * 1024;

		private const int SmallBlockSize = 1024;

		protected internal MiniDFSCluster cluster;

		private Configuration conf;

		private DistributedFileSystem fs = null;

		private DFSClient client = null;

		internal FsVolumeImpl singletonVolume = null;

		private static Random rand = new Random();

		private void InitConfig(int blockSize)
		{
			conf = new HdfsConfiguration();
			// Refresh disk usage information frequently.
			conf.SetInt(FsDuIntervalKey, DuRefreshIntervalMsec);
			conf.SetLong(DfsBlockSizeKey, blockSize);
			// Disable the scanner
			conf.SetInt(DfsDatanodeScanPeriodHoursKey, -1);
		}

		static TestRbwSpaceReservation()
		{
			((Log4JLogger)FsDatasetImpl.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)DataNode.Log).GetLogger().SetLevel(Level.All);
		}

		/// <param name="blockSize"/>
		/// <param name="perVolumeCapacity">
		/// limit the capacity of each volume to the given
		/// value. If negative, then don't limit.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		private void StartCluster(int blockSize, int numDatanodes, long perVolumeCapacity
			)
		{
			InitConfig(blockSize);
			cluster = new MiniDFSCluster.Builder(conf).StoragesPerDatanode(StoragesPerDatanode
				).NumDataNodes(numDatanodes).Build();
			fs = cluster.GetFileSystem();
			client = fs.GetClient();
			cluster.WaitActive();
			if (perVolumeCapacity >= 0)
			{
				foreach (DataNode dn in cluster.GetDataNodes())
				{
					foreach (FsVolumeSpi volume in dn.GetFSDataset().GetVolumes())
					{
						((FsVolumeImpl)volume).SetCapacityForTesting(perVolumeCapacity);
					}
				}
			}
			if (numDatanodes == 1)
			{
				IList<FsVolumeSpi> volumes = cluster.GetDataNodes()[0].GetFSDataset().GetVolumes(
					);
				Assert.AssertThat(volumes.Count, IS.Is(1));
				singletonVolume = ((FsVolumeImpl)volumes[0]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutdownCluster()
		{
			if (client != null)
			{
				client.Close();
				client = null;
			}
			if (fs != null)
			{
				fs.Close();
				fs = null;
			}
			if (cluster != null)
			{
				cluster.Shutdown();
				cluster = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void CreateFileAndTestSpaceReservation(string fileNamePrefix, int fileBlockSize
			)
		{
			// Enough for 1 block + meta files + some delta.
			long configuredCapacity = fileBlockSize * 2 - 1;
			StartCluster(BlockSize, 1, configuredCapacity);
			FSDataOutputStream @out = null;
			Path path = new Path("/" + fileNamePrefix + ".dat");
			try
			{
				@out = fs.Create(path, false, 4096, (short)1, fileBlockSize);
				byte[] buffer = new byte[rand.Next(fileBlockSize / 4)];
				@out.Write(buffer);
				@out.Hsync();
				int bytesWritten = buffer.Length;
				// Check that space was reserved for a full block minus the bytesWritten.
				Assert.AssertThat(singletonVolume.GetReservedForRbw(), IS.Is((long)fileBlockSize 
					- bytesWritten));
				@out.Close();
				@out = null;
				// Check that the reserved space has been released since we closed the
				// file.
				Assert.AssertThat(singletonVolume.GetReservedForRbw(), IS.Is(0L));
				// Reopen the file for appends and write 1 more byte.
				@out = fs.Append(path);
				@out.Write(buffer);
				@out.Hsync();
				bytesWritten += buffer.Length;
				// Check that space was again reserved for a full block minus the
				// bytesWritten so far.
				Assert.AssertThat(singletonVolume.GetReservedForRbw(), IS.Is((long)fileBlockSize 
					- bytesWritten));
				// Write once again and again verify the available space. This ensures
				// that the reserved space is progressively adjusted to account for bytes
				// written to disk.
				@out.Write(buffer);
				@out.Hsync();
				bytesWritten += buffer.Length;
				Assert.AssertThat(singletonVolume.GetReservedForRbw(), IS.Is((long)fileBlockSize 
					- bytesWritten));
			}
			finally
			{
				if (@out != null)
				{
					@out.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestWithDefaultBlockSize()
		{
			CreateFileAndTestSpaceReservation(GenericTestUtils.GetMethodName(), BlockSize);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestWithNonDefaultBlockSize()
		{
			// Same test as previous one, but with a non-default block size.
			CreateFileAndTestSpaceReservation(GenericTestUtils.GetMethodName(), BlockSize * 2
				);
		}

		[Rule]
		public ExpectedException thrown = ExpectedException.None();

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWithLimitedSpace()
		{
			// Cluster with just enough space for a full block + meta.
			StartCluster(BlockSize, 1, 2 * BlockSize - 1);
			string methodName = GenericTestUtils.GetMethodName();
			Path file1 = new Path("/" + methodName + ".01.dat");
			Path file2 = new Path("/" + methodName + ".02.dat");
			// Create two files.
			FSDataOutputStream os1 = null;
			FSDataOutputStream os2 = null;
			try
			{
				os1 = fs.Create(file1);
				os2 = fs.Create(file2);
				// Write one byte to the first file.
				byte[] data = new byte[1];
				os1.Write(data);
				os1.Hsync();
				// Try to write one byte to the second file.
				// The block allocation must fail.
				thrown.Expect(typeof(RemoteException));
				os2.Write(data);
				os2.Hsync();
			}
			finally
			{
				if (os1 != null)
				{
					os1.Close();
				}
			}
		}

		// os2.close() will fail as no block was allocated.
		/// <summary>
		/// Ensure that reserved space is released when the client goes away
		/// unexpectedly.
		/// </summary>
		/// <remarks>
		/// Ensure that reserved space is released when the client goes away
		/// unexpectedly.
		/// The verification is done for each replica in the write pipeline.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		public virtual void TestSpaceReleasedOnUnexpectedEof()
		{
			short replication = 3;
			StartCluster(BlockSize, replication, -1);
			string methodName = GenericTestUtils.GetMethodName();
			Path file = new Path("/" + methodName + ".01.dat");
			// Write 1 byte to the file and kill the writer.
			FSDataOutputStream os = fs.Create(file, replication);
			os.Write(new byte[1]);
			os.Hsync();
			DFSTestUtil.AbortStream((DFSOutputStream)os.GetWrappedStream());
			// Ensure all space reserved for the replica was released on each
			// DataNode.
			foreach (DataNode dn in cluster.GetDataNodes())
			{
				FsVolumeImpl volume = (FsVolumeImpl)dn.GetFSDataset().GetVolumes()[0];
				GenericTestUtils.WaitFor(new _Supplier_276(volume), 500, int.MaxValue);
			}
		}

		private sealed class _Supplier_276 : Supplier<bool>
		{
			public _Supplier_276(FsVolumeImpl volume)
			{
				this.volume = volume;
			}

			public bool Get()
			{
				return (volume.GetReservedForRbw() == 0);
			}

			private readonly FsVolumeImpl volume;
		}

		// Wait until the test times out.
		/// <exception cref="System.Exception"/>
		public virtual void TestRBWFileCreationError()
		{
			short replication = 1;
			StartCluster(BlockSize, replication, -1);
			FsVolumeImpl fsVolumeImpl = (FsVolumeImpl)cluster.GetDataNodes()[0].GetFSDataset(
				).GetVolumes()[0];
			string methodName = GenericTestUtils.GetMethodName();
			Path file = new Path("/" + methodName + ".01.dat");
			// Mock BlockPoolSlice so that RBW file creation gives IOExcception
			BlockPoolSlice blockPoolSlice = Org.Mockito.Mockito.Mock<BlockPoolSlice>();
			Org.Mockito.Mockito.When(blockPoolSlice.CreateRbwFile((Block)Org.Mockito.Mockito.
				Any())).ThenThrow(new IOException("Synthetic IO Exception Throgh MOCK"));
			FieldInfo field = Sharpen.Runtime.GetDeclaredField(typeof(FsVolumeImpl), "bpSlices"
				);
			IDictionary<string, BlockPoolSlice> bpSlices = (IDictionary<string, BlockPoolSlice
				>)field.GetValue(fsVolumeImpl);
			bpSlices[fsVolumeImpl.GetBlockPoolList()[0]] = blockPoolSlice;
			try
			{
				// Write 1 byte to the file
				FSDataOutputStream os = fs.Create(file, replication);
				os.Write(new byte[1]);
				os.Hsync();
				os.Close();
				NUnit.Framework.Assert.Fail("Expecting IOException file creation failure");
			}
			catch (IOException)
			{
			}
			// Exception can be ignored (expected)
			// Ensure RBW space reserved is released
			NUnit.Framework.Assert.IsTrue("Expected ZERO but got " + fsVolumeImpl.GetReservedForRbw
				(), fsVolumeImpl.GetReservedForRbw() == 0);
		}

		/// <summary>Stress test to ensure we are not leaking reserved space.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void StressTest()
		{
			int numWriters = 5;
			StartCluster(SmallBlockSize, 1, SmallBlockSize * numWriters * 10);
			TestRbwSpaceReservation.Writer[] writers = new TestRbwSpaceReservation.Writer[numWriters
				];
			// Start a few writers and let them run for a while.
			for (int i = 0; i < numWriters; ++i)
			{
				writers[i] = new TestRbwSpaceReservation.Writer(client, SmallBlockSize);
				writers[i].Start();
			}
			Sharpen.Thread.Sleep(60000);
			// Stop the writers.
			foreach (TestRbwSpaceReservation.Writer w in writers)
			{
				w.StopWriter();
			}
			int filesCreated = 0;
			int numFailures = 0;
			foreach (TestRbwSpaceReservation.Writer w_1 in writers)
			{
				w_1.Join();
				filesCreated += w_1.GetFilesCreated();
				numFailures += w_1.GetNumFailures();
			}
			Log.Info("Stress test created " + filesCreated + " files and hit " + numFailures 
				+ " failures");
			// Check no space was leaked.
			Assert.AssertThat(singletonVolume.GetReservedForRbw(), IS.Is(0L));
		}

		private class Writer : Daemon
		{
			private volatile bool keepRunning;

			private readonly DFSClient localClient;

			private int filesCreated = 0;

			private int numFailures = 0;

			internal byte[] data;

			/// <exception cref="System.IO.IOException"/>
			internal Writer(DFSClient client, int blockSize)
			{
				localClient = client;
				keepRunning = true;
				filesCreated = 0;
				numFailures = 0;
				// At least some of the files should span a block boundary.
				data = new byte[blockSize * 2];
			}

			public override void Run()
			{
				while (keepRunning)
				{
					OutputStream os = null;
					try
					{
						string filename = "/file-" + rand.NextLong();
						os = localClient.Create(filename, false);
						os.Write(data, 0, rand.Next(data.Length));
						IOUtils.CloseQuietly(os);
						os = null;
						localClient.Delete(filename, false);
						Sharpen.Thread.Sleep(50);
						// Sleep for a bit to avoid killing the system.
						++filesCreated;
					}
					catch (IOException)
					{
						// Just ignore the exception and keep going.
						++numFailures;
					}
					catch (Exception)
					{
						return;
					}
					finally
					{
						if (os != null)
						{
							IOUtils.CloseQuietly(os);
						}
					}
				}
			}

			public virtual void StopWriter()
			{
				keepRunning = false;
			}

			public virtual int GetFilesCreated()
			{
				return filesCreated;
			}

			public virtual int GetNumFailures()
			{
				return numFailures;
			}
		}
	}
}
