using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestClientBlockVerification
	{
		internal static BlockReaderTestUtil util = null;

		internal static readonly Path TestFile = new Path("/test.file");

		internal const int FileSizeK = 256;

		internal static LocatedBlock testBlock = null;

		static TestClientBlockVerification()
		{
			((Log4JLogger)RemoteBlockReader2.Log).GetLogger().SetLevel(Level.All);
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupCluster()
		{
			int ReplicationFactor = 1;
			util = new BlockReaderTestUtil(ReplicationFactor);
			util.WriteFile(TestFile, FileSizeK);
			IList<LocatedBlock> blkList = util.GetFileBlocks(TestFile, FileSizeK);
			testBlock = blkList[0];
		}

		// Use the first block to test
		/// <summary>Verify that if we read an entire block, we send CHECKSUM_OK</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockVerification()
		{
			RemoteBlockReader2 reader = (RemoteBlockReader2)Org.Mockito.Mockito.Spy(util.GetBlockReader
				(testBlock, 0, FileSizeK * 1024));
			util.ReadAndCheckEOS(reader, FileSizeK * 1024, true);
			Org.Mockito.Mockito.Verify(reader).SendReadResult(DataTransferProtos.Status.ChecksumOk
				);
			reader.Close();
		}

		/// <summary>Test that if we do an incomplete read, we don't call CHECKSUM_OK</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIncompleteRead()
		{
			RemoteBlockReader2 reader = (RemoteBlockReader2)Org.Mockito.Mockito.Spy(util.GetBlockReader
				(testBlock, 0, FileSizeK * 1024));
			util.ReadAndCheckEOS(reader, FileSizeK / 2 * 1024, false);
			// We asked the blockreader for the whole file, and only read
			// half of it, so no CHECKSUM_OK
			Org.Mockito.Mockito.Verify(reader, Org.Mockito.Mockito.Never()).SendReadResult(DataTransferProtos.Status
				.ChecksumOk);
			reader.Close();
		}

		/// <summary>
		/// Test that if we ask for a half block, and read it all, we *do
		/// send CHECKSUM_OK.
		/// </summary>
		/// <remarks>
		/// Test that if we ask for a half block, and read it all, we *do
		/// send CHECKSUM_OK. The DN takes care of knowing whether it was
		/// the whole block or not.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCompletePartialRead()
		{
			// Ask for half the file
			RemoteBlockReader2 reader = (RemoteBlockReader2)Org.Mockito.Mockito.Spy(util.GetBlockReader
				(testBlock, 0, FileSizeK * 1024 / 2));
			// And read half the file
			util.ReadAndCheckEOS(reader, FileSizeK * 1024 / 2, true);
			Org.Mockito.Mockito.Verify(reader).SendReadResult(DataTransferProtos.Status.ChecksumOk
				);
			reader.Close();
		}

		/// <summary>
		/// Test various unaligned reads to make sure that we properly
		/// account even when we don't start or end on a checksum boundary
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUnalignedReads()
		{
			int[] startOffsets = new int[] { 0, 3, 129 };
			int[] lengths = new int[] { 30, 300, 512, 513, 1025 };
			foreach (int startOffset in startOffsets)
			{
				foreach (int length in lengths)
				{
					DFSClient.Log.Info("Testing startOffset = " + startOffset + " and " + " len=" + length
						);
					RemoteBlockReader2 reader = (RemoteBlockReader2)Org.Mockito.Mockito.Spy(util.GetBlockReader
						(testBlock, startOffset, length));
					util.ReadAndCheckEOS(reader, length, true);
					Org.Mockito.Mockito.Verify(reader).SendReadResult(DataTransferProtos.Status.ChecksumOk
						);
					reader.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TeardownCluster()
		{
			util.Shutdown();
		}
	}
}
