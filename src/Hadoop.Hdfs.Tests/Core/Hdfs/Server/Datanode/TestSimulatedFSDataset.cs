using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>this class tests the methods of the  SimulatedFSDataset.</summary>
	public class TestSimulatedFSDataset
	{
		internal Configuration conf = null;

		internal const string bpid = "BP-TEST";

		internal const int Numblocks = 20;

		internal const int BlockLengthMultiplier = 79;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new HdfsConfiguration();
			SimulatedFSDataset.SetFactory(conf);
		}

		internal virtual long BlockIdToLen(long blkid)
		{
			return blkid * BlockLengthMultiplier;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual int AddSomeBlocks(SimulatedFSDataset fsdataset, int startingBlockId
			)
		{
			int bytesAdded = 0;
			for (int i = startingBlockId; i < startingBlockId + Numblocks; ++i)
			{
				ExtendedBlock b = new ExtendedBlock(bpid, i, 0, 0);
				// we pass expected len as zero, - fsdataset should use the sizeof actual
				// data written
				ReplicaInPipelineInterface bInfo = fsdataset.CreateRbw(StorageType.Default, b, false
					).GetReplica();
				ReplicaOutputStreams @out = bInfo.CreateStreams(true, DataChecksum.NewDataChecksum
					(DataChecksum.Type.Crc32, 512));
				try
				{
					OutputStream dataOut = @out.GetDataOut();
					NUnit.Framework.Assert.AreEqual(0, fsdataset.GetLength(b));
					for (int j = 1; j <= BlockIdToLen(i); ++j)
					{
						dataOut.Write(j);
						NUnit.Framework.Assert.AreEqual(j, bInfo.GetBytesOnDisk());
						// correct length even as we write
						bytesAdded++;
					}
				}
				finally
				{
					@out.Close();
				}
				b.SetNumBytes(BlockIdToLen(i));
				fsdataset.FinalizeBlock(b);
				NUnit.Framework.Assert.AreEqual(BlockIdToLen(i), fsdataset.GetLength(b));
			}
			return bytesAdded;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual int AddSomeBlocks(SimulatedFSDataset fsdataset)
		{
			return AddSomeBlocks(fsdataset, 1);
		}

		[NUnit.Framework.Test]
		public virtual void TestFSDatasetFactory()
		{
			Configuration conf = new Configuration();
			FsDatasetSpi.Factory<object> f = FsDatasetSpi.Factory.GetFactory(conf);
			NUnit.Framework.Assert.AreEqual(typeof(FsDatasetFactory), f.GetType());
			NUnit.Framework.Assert.IsFalse(f.IsSimulated());
			SimulatedFSDataset.SetFactory(conf);
			FsDatasetSpi.Factory<object> s = FsDatasetSpi.Factory.GetFactory(conf);
			NUnit.Framework.Assert.AreEqual(typeof(SimulatedFSDataset.Factory), s.GetType());
			NUnit.Framework.Assert.IsTrue(s.IsSimulated());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetMetaData()
		{
			SimulatedFSDataset fsdataset = GetSimulatedFSDataset();
			ExtendedBlock b = new ExtendedBlock(bpid, 1, 5, 0);
			try
			{
				NUnit.Framework.Assert.IsTrue(fsdataset.GetMetaDataInputStream(b) == null);
				NUnit.Framework.Assert.IsTrue("Expected an IO exception", false);
			}
			catch (IOException)
			{
			}
			// ok - as expected
			AddSomeBlocks(fsdataset);
			// Only need to add one but ....
			b = new ExtendedBlock(bpid, 1, 0, 0);
			InputStream metaInput = fsdataset.GetMetaDataInputStream(b);
			DataInputStream metaDataInput = new DataInputStream(metaInput);
			short version = metaDataInput.ReadShort();
			NUnit.Framework.Assert.AreEqual(BlockMetadataHeader.Version, version);
			DataChecksum checksum = DataChecksum.NewDataChecksum(metaDataInput);
			NUnit.Framework.Assert.AreEqual(DataChecksum.Type.Null, checksum.GetChecksumType(
				));
			NUnit.Framework.Assert.AreEqual(0, checksum.GetChecksumSize());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStorageUsage()
		{
			SimulatedFSDataset fsdataset = GetSimulatedFSDataset();
			NUnit.Framework.Assert.AreEqual(fsdataset.GetDfsUsed(), 0);
			NUnit.Framework.Assert.AreEqual(fsdataset.GetRemaining(), fsdataset.GetCapacity()
				);
			int bytesAdded = AddSomeBlocks(fsdataset);
			NUnit.Framework.Assert.AreEqual(bytesAdded, fsdataset.GetDfsUsed());
			NUnit.Framework.Assert.AreEqual(fsdataset.GetCapacity() - bytesAdded, fsdataset.GetRemaining
				());
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckBlockDataAndSize(SimulatedFSDataset fsdataset, ExtendedBlock
			 b, long expectedLen)
		{
			InputStream input = fsdataset.GetBlockInputStream(b);
			long lengthRead = 0;
			int data;
			while ((data = input.Read()) != -1)
			{
				NUnit.Framework.Assert.AreEqual(SimulatedFSDataset.DefaultDatabyte, data);
				lengthRead++;
			}
			NUnit.Framework.Assert.AreEqual(expectedLen, lengthRead);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteRead()
		{
			SimulatedFSDataset fsdataset = GetSimulatedFSDataset();
			AddSomeBlocks(fsdataset);
			for (int i = 1; i <= Numblocks; ++i)
			{
				ExtendedBlock b = new ExtendedBlock(bpid, i, 0, 0);
				NUnit.Framework.Assert.IsTrue(fsdataset.IsValidBlock(b));
				NUnit.Framework.Assert.AreEqual(BlockIdToLen(i), fsdataset.GetLength(b));
				CheckBlockDataAndSize(fsdataset, b, BlockIdToLen(i));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetBlockReport()
		{
			SimulatedFSDataset fsdataset = GetSimulatedFSDataset();
			BlockListAsLongs blockReport = fsdataset.GetBlockReport(bpid);
			NUnit.Framework.Assert.AreEqual(0, blockReport.GetNumberOfBlocks());
			AddSomeBlocks(fsdataset);
			blockReport = fsdataset.GetBlockReport(bpid);
			NUnit.Framework.Assert.AreEqual(Numblocks, blockReport.GetNumberOfBlocks());
			foreach (Block b in blockReport)
			{
				NUnit.Framework.Assert.IsNotNull(b);
				NUnit.Framework.Assert.AreEqual(BlockIdToLen(b.GetBlockId()), b.GetNumBytes());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInjectionEmpty()
		{
			SimulatedFSDataset fsdataset = GetSimulatedFSDataset();
			BlockListAsLongs blockReport = fsdataset.GetBlockReport(bpid);
			NUnit.Framework.Assert.AreEqual(0, blockReport.GetNumberOfBlocks());
			int bytesAdded = AddSomeBlocks(fsdataset);
			blockReport = fsdataset.GetBlockReport(bpid);
			NUnit.Framework.Assert.AreEqual(Numblocks, blockReport.GetNumberOfBlocks());
			foreach (Block b in blockReport)
			{
				NUnit.Framework.Assert.IsNotNull(b);
				NUnit.Framework.Assert.AreEqual(BlockIdToLen(b.GetBlockId()), b.GetNumBytes());
			}
			// Inject blocks into an empty fsdataset
			//  - injecting the blocks we got above.
			SimulatedFSDataset sfsdataset = GetSimulatedFSDataset();
			sfsdataset.InjectBlocks(bpid, blockReport);
			blockReport = sfsdataset.GetBlockReport(bpid);
			NUnit.Framework.Assert.AreEqual(Numblocks, blockReport.GetNumberOfBlocks());
			foreach (Block b_1 in blockReport)
			{
				NUnit.Framework.Assert.IsNotNull(b_1);
				NUnit.Framework.Assert.AreEqual(BlockIdToLen(b_1.GetBlockId()), b_1.GetNumBytes()
					);
				NUnit.Framework.Assert.AreEqual(BlockIdToLen(b_1.GetBlockId()), sfsdataset.GetLength
					(new ExtendedBlock(bpid, b_1)));
			}
			NUnit.Framework.Assert.AreEqual(bytesAdded, sfsdataset.GetDfsUsed());
			NUnit.Framework.Assert.AreEqual(sfsdataset.GetCapacity() - bytesAdded, sfsdataset
				.GetRemaining());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInjectionNonEmpty()
		{
			SimulatedFSDataset fsdataset = GetSimulatedFSDataset();
			BlockListAsLongs blockReport = fsdataset.GetBlockReport(bpid);
			NUnit.Framework.Assert.AreEqual(0, blockReport.GetNumberOfBlocks());
			int bytesAdded = AddSomeBlocks(fsdataset);
			blockReport = fsdataset.GetBlockReport(bpid);
			NUnit.Framework.Assert.AreEqual(Numblocks, blockReport.GetNumberOfBlocks());
			foreach (Block b in blockReport)
			{
				NUnit.Framework.Assert.IsNotNull(b);
				NUnit.Framework.Assert.AreEqual(BlockIdToLen(b.GetBlockId()), b.GetNumBytes());
			}
			fsdataset = null;
			// Inject blocks into an non-empty fsdataset
			//  - injecting the blocks we got above.
			SimulatedFSDataset sfsdataset = GetSimulatedFSDataset();
			// Add come blocks whose block ids do not conflict with
			// the ones we are going to inject.
			bytesAdded += AddSomeBlocks(sfsdataset, Numblocks + 1);
			sfsdataset.GetBlockReport(bpid);
			NUnit.Framework.Assert.AreEqual(Numblocks, blockReport.GetNumberOfBlocks());
			sfsdataset.GetBlockReport(bpid);
			NUnit.Framework.Assert.AreEqual(Numblocks, blockReport.GetNumberOfBlocks());
			sfsdataset.InjectBlocks(bpid, blockReport);
			blockReport = sfsdataset.GetBlockReport(bpid);
			NUnit.Framework.Assert.AreEqual(Numblocks * 2, blockReport.GetNumberOfBlocks());
			foreach (Block b_1 in blockReport)
			{
				NUnit.Framework.Assert.IsNotNull(b_1);
				NUnit.Framework.Assert.AreEqual(BlockIdToLen(b_1.GetBlockId()), b_1.GetNumBytes()
					);
				NUnit.Framework.Assert.AreEqual(BlockIdToLen(b_1.GetBlockId()), sfsdataset.GetLength
					(new ExtendedBlock(bpid, b_1)));
			}
			NUnit.Framework.Assert.AreEqual(bytesAdded, sfsdataset.GetDfsUsed());
			NUnit.Framework.Assert.AreEqual(sfsdataset.GetCapacity() - bytesAdded, sfsdataset
				.GetRemaining());
			// Now test that the dataset cannot be created if it does not have sufficient cap
			conf.SetLong(SimulatedFSDataset.ConfigPropertyCapacity, 10);
			try
			{
				sfsdataset = GetSimulatedFSDataset();
				sfsdataset.AddBlockPool(bpid, conf);
				sfsdataset.InjectBlocks(bpid, blockReport);
				NUnit.Framework.Assert.IsTrue("Expected an IO exception", false);
			}
			catch (IOException)
			{
			}
		}

		// ok - as expected
		public virtual void CheckInvalidBlock(ExtendedBlock b)
		{
			SimulatedFSDataset fsdataset = GetSimulatedFSDataset();
			NUnit.Framework.Assert.IsFalse(fsdataset.IsValidBlock(b));
			try
			{
				fsdataset.GetLength(b);
				NUnit.Framework.Assert.IsTrue("Expected an IO exception", false);
			}
			catch (IOException)
			{
			}
			// ok - as expected
			try
			{
				fsdataset.GetBlockInputStream(b);
				NUnit.Framework.Assert.IsTrue("Expected an IO exception", false);
			}
			catch (IOException)
			{
			}
			// ok - as expected
			try
			{
				fsdataset.FinalizeBlock(b);
				NUnit.Framework.Assert.IsTrue("Expected an IO exception", false);
			}
			catch (IOException)
			{
			}
		}

		// ok - as expected
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInValidBlocks()
		{
			SimulatedFSDataset fsdataset = GetSimulatedFSDataset();
			ExtendedBlock b = new ExtendedBlock(bpid, 1, 5, 0);
			CheckInvalidBlock(b);
			// Now check invlaid after adding some blocks
			AddSomeBlocks(fsdataset);
			b = new ExtendedBlock(bpid, Numblocks + 99, 5, 0);
			CheckInvalidBlock(b);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidate()
		{
			SimulatedFSDataset fsdataset = GetSimulatedFSDataset();
			int bytesAdded = AddSomeBlocks(fsdataset);
			Block[] deleteBlocks = new Block[2];
			deleteBlocks[0] = new Block(1, 0, 0);
			deleteBlocks[1] = new Block(2, 0, 0);
			fsdataset.Invalidate(bpid, deleteBlocks);
			CheckInvalidBlock(new ExtendedBlock(bpid, deleteBlocks[0]));
			CheckInvalidBlock(new ExtendedBlock(bpid, deleteBlocks[1]));
			long sizeDeleted = BlockIdToLen(1) + BlockIdToLen(2);
			NUnit.Framework.Assert.AreEqual(bytesAdded - sizeDeleted, fsdataset.GetDfsUsed());
			NUnit.Framework.Assert.AreEqual(fsdataset.GetCapacity() - bytesAdded + sizeDeleted
				, fsdataset.GetRemaining());
			// Now make sure the rest of the blocks are valid
			for (int i = 3; i <= Numblocks; ++i)
			{
				Block b = new Block(i, 0, 0);
				NUnit.Framework.Assert.IsTrue(fsdataset.IsValidBlock(new ExtendedBlock(bpid, b)));
			}
		}

		private SimulatedFSDataset GetSimulatedFSDataset()
		{
			SimulatedFSDataset fsdataset = new SimulatedFSDataset(null, conf);
			fsdataset.AddBlockPool(bpid, conf);
			return fsdataset;
		}
	}
}
