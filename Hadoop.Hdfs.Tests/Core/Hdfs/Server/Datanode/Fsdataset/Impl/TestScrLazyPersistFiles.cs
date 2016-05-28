using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Hamcrest;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	public class TestScrLazyPersistFiles : LazyPersistTestCase
	{
		[BeforeClass]
		public static void Init()
		{
			DomainSocket.DisableBindPathValidation();
		}

		[SetUp]
		public virtual void Before()
		{
			Assume.AssumeThat(NativeCodeLoader.IsNativeCodeLoaded() && !Path.Windows, CoreMatchers.EqualTo
				(true));
			Assume.AssumeThat(DomainSocket.GetLoadingFailureReason(), CoreMatchers.EqualTo(null
				));
		}

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		/// <summary>
		/// Read in-memory block with Short Circuit Read
		/// Note: the test uses faked RAM_DISK from physical disk.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRamDiskShortCircuitRead()
		{
			StartUpCluster(ReplFactor, new StorageType[] { StorageType.RamDisk, StorageType.Default
				 }, 2 * BlockSize - 1, true);
			// 1 replica + delta, SCR read
			string MethodName = GenericTestUtils.GetMethodName();
			int Seed = unchecked((int)(0xFADED));
			Path path = new Path("/" + MethodName + ".dat");
			MakeRandomTestFile(path, BlockSize, true, Seed);
			EnsureFileReplicasOnStorageType(path, StorageType.RamDisk);
			// Sleep for a short time to allow the lazy writer thread to do its job
			Sharpen.Thread.Sleep(3 * LazyWriterIntervalSec * 1000);
			//assertThat(verifyReadRandomFile(path, BLOCK_SIZE, SEED), is(true));
			FSDataInputStream fis = fs.Open(path);
			// Verify SCR read counters
			try
			{
				fis = fs.Open(path);
				byte[] buf = new byte[BufferLength];
				fis.Read(0, buf, 0, BufferLength);
				HdfsDataInputStream dfsis = (HdfsDataInputStream)fis;
				NUnit.Framework.Assert.AreEqual(BufferLength, dfsis.GetReadStatistics().GetTotalBytesRead
					());
				NUnit.Framework.Assert.AreEqual(BufferLength, dfsis.GetReadStatistics().GetTotalShortCircuitBytesRead
					());
			}
			finally
			{
				fis.Close();
				fis = null;
			}
		}

		/// <summary>
		/// Eviction of lazy persisted blocks with Short Circuit Read handle open
		/// Note: the test uses faked RAM_DISK from physical disk.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRamDiskEvictionWithShortCircuitReadHandle()
		{
			StartUpCluster(ReplFactor, new StorageType[] { StorageType.RamDisk, StorageType.Default
				 }, (6 * BlockSize - 1), true);
			// 5 replica + delta, SCR.
			string MethodName = GenericTestUtils.GetMethodName();
			Path path1 = new Path("/" + MethodName + ".01.dat");
			Path path2 = new Path("/" + MethodName + ".02.dat");
			int Seed = unchecked((int)(0xFADED));
			MakeRandomTestFile(path1, BlockSize, true, Seed);
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			// Sleep for a short time to allow the lazy writer thread to do its job.
			// However the block replica should not be evicted from RAM_DISK yet.
			Sharpen.Thread.Sleep(3 * LazyWriterIntervalSec * 1000);
			// No eviction should happen as the free ratio is below the threshold
			FSDataInputStream fis = fs.Open(path1);
			try
			{
				// Keep and open read handle to path1 while creating path2
				byte[] buf = new byte[BufferLength];
				fis.Read(0, buf, 0, BufferLength);
				// Create the 2nd file that will trigger RAM_DISK eviction.
				MakeTestFile(path2, BlockSize * 2, true);
				EnsureFileReplicasOnStorageType(path2, StorageType.RamDisk);
				// Ensure path1 is still readable from the open SCR handle.
				fis.Read(fis.GetPos(), buf, 0, BufferLength);
				HdfsDataInputStream dfsis = (HdfsDataInputStream)fis;
				NUnit.Framework.Assert.AreEqual(2 * BufferLength, dfsis.GetReadStatistics().GetTotalBytesRead
					());
				NUnit.Framework.Assert.AreEqual(2 * BufferLength, dfsis.GetReadStatistics().GetTotalShortCircuitBytesRead
					());
			}
			finally
			{
				IOUtils.CloseQuietly(fis);
			}
			// After the open handle is closed, path1 should be evicted to DISK.
			TriggerBlockReport();
			EnsureFileReplicasOnStorageType(path1, StorageType.Default);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShortCircuitReadAfterEviction()
		{
			Assume.AssumeThat(DomainSocket.GetLoadingFailureReason(), CoreMatchers.EqualTo(null
				));
			StartUpCluster(true, 1 + EvictionLowWatermark, true, false);
			DoShortCircuitReadAfterEvictionTest();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLegacyShortCircuitReadAfterEviction()
		{
			StartUpCluster(true, 1 + EvictionLowWatermark, true, true);
			DoShortCircuitReadAfterEvictionTest();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void DoShortCircuitReadAfterEvictionTest()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path path1 = new Path("/" + MethodName + ".01.dat");
			Path path2 = new Path("/" + MethodName + ".02.dat");
			int Seed = unchecked((int)(0xFADED));
			MakeRandomTestFile(path1, BlockSize, true, Seed);
			// Verify short-circuit read from RAM_DISK.
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			FilePath metaFile = cluster.GetBlockMetadataFile(0, DFSTestUtil.GetFirstBlock(fs, 
				path1));
			NUnit.Framework.Assert.IsTrue(metaFile.Length() <= BlockMetadataHeader.GetHeaderSize
				());
			NUnit.Framework.Assert.IsTrue(VerifyReadRandomFile(path1, BlockSize, Seed));
			// Sleep for a short time to allow the lazy writer thread to do its job.
			Sharpen.Thread.Sleep(3 * LazyWriterIntervalSec * 1000);
			// Verify short-circuit read from RAM_DISK once again.
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			metaFile = cluster.GetBlockMetadataFile(0, DFSTestUtil.GetFirstBlock(fs, path1));
			NUnit.Framework.Assert.IsTrue(metaFile.Length() <= BlockMetadataHeader.GetHeaderSize
				());
			NUnit.Framework.Assert.IsTrue(VerifyReadRandomFile(path1, BlockSize, Seed));
			// Create another file with a replica on RAM_DISK, which evicts the first.
			MakeRandomTestFile(path2, BlockSize, true, Seed);
			Sharpen.Thread.Sleep(3 * LazyWriterIntervalSec * 1000);
			TriggerBlockReport();
			// Verify short-circuit read still works from DEFAULT storage.  This time,
			// we'll have a checksum written during lazy persistence.
			EnsureFileReplicasOnStorageType(path1, StorageType.Default);
			metaFile = cluster.GetBlockMetadataFile(0, DFSTestUtil.GetFirstBlock(fs, path1));
			NUnit.Framework.Assert.IsTrue(metaFile.Length() > BlockMetadataHeader.GetHeaderSize
				());
			NUnit.Framework.Assert.IsTrue(VerifyReadRandomFile(path1, BlockSize, Seed));
			// In the implementation of legacy short-circuit reads, any failure is
			// trapped silently, reverts back to a remote read, and also disables all
			// subsequent legacy short-circuit reads in the ClientContext.  If the test
			// uses legacy, then assert that it didn't get disabled.
			ClientContext clientContext = client.GetClientContext();
			if (clientContext.GetUseLegacyBlockReaderLocal())
			{
				NUnit.Framework.Assert.IsFalse(clientContext.GetDisableLegacyBlockReaderLocal());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShortCircuitReadBlockFileCorruption()
		{
			Assume.AssumeThat(DomainSocket.GetLoadingFailureReason(), CoreMatchers.EqualTo(null
				));
			StartUpCluster(true, 1 + EvictionLowWatermark, true, false);
			DoShortCircuitReadBlockFileCorruptionTest();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLegacyShortCircuitReadBlockFileCorruption()
		{
			StartUpCluster(true, 1 + EvictionLowWatermark, true, true);
			DoShortCircuitReadBlockFileCorruptionTest();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void DoShortCircuitReadBlockFileCorruptionTest()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path path1 = new Path("/" + MethodName + ".01.dat");
			Path path2 = new Path("/" + MethodName + ".02.dat");
			int Seed = unchecked((int)(0xFADED));
			MakeRandomTestFile(path1, BlockSize, true, Seed);
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			// Create another file with a replica on RAM_DISK, which evicts the first.
			MakeRandomTestFile(path2, BlockSize, true, Seed);
			// Sleep for a short time to allow the lazy writer thread to do its job.
			Sharpen.Thread.Sleep(3 * LazyWriterIntervalSec * 1000);
			TriggerBlockReport();
			// Corrupt the lazy-persisted block file, and verify that checksum
			// verification catches it.
			EnsureFileReplicasOnStorageType(path1, StorageType.Default);
			cluster.CorruptReplica(0, DFSTestUtil.GetFirstBlock(fs, path1));
			exception.Expect(typeof(ChecksumException));
			DFSTestUtil.ReadFileBuffer(fs, path1);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShortCircuitReadMetaFileCorruption()
		{
			Assume.AssumeThat(DomainSocket.GetLoadingFailureReason(), CoreMatchers.EqualTo(null
				));
			StartUpCluster(true, 1 + EvictionLowWatermark, true, false);
			DoShortCircuitReadMetaFileCorruptionTest();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLegacyShortCircuitReadMetaFileCorruption()
		{
			StartUpCluster(true, 1 + EvictionLowWatermark, true, true);
			DoShortCircuitReadMetaFileCorruptionTest();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void DoShortCircuitReadMetaFileCorruptionTest()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path path1 = new Path("/" + MethodName + ".01.dat");
			Path path2 = new Path("/" + MethodName + ".02.dat");
			int Seed = unchecked((int)(0xFADED));
			MakeRandomTestFile(path1, BlockSize, true, Seed);
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			// Create another file with a replica on RAM_DISK, which evicts the first.
			MakeRandomTestFile(path2, BlockSize, true, Seed);
			// Sleep for a short time to allow the lazy writer thread to do its job.
			Sharpen.Thread.Sleep(3 * LazyWriterIntervalSec * 1000);
			TriggerBlockReport();
			// Corrupt the lazy-persisted checksum file, and verify that checksum
			// verification catches it.
			EnsureFileReplicasOnStorageType(path1, StorageType.Default);
			FilePath metaFile = cluster.GetBlockMetadataFile(0, DFSTestUtil.GetFirstBlock(fs, 
				path1));
			MiniDFSCluster.CorruptBlock(metaFile);
			exception.Expect(typeof(ChecksumException));
			DFSTestUtil.ReadFileBuffer(fs, path1);
		}
	}
}
