using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Test;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// Test to verify that the DFSClient passes the expected block length to
	/// the DataNode via DataTransferProtocol.
	/// </summary>
	public class TestWriteBlockGetsBlockLengthHint
	{
		internal const long DefaultBlockLength = 1024;

		internal const long ExpectedBlockLength = DefaultBlockLength * 2;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void BlockLengthHintIsPropagated()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			Configuration conf = new HdfsConfiguration();
			TestWriteBlockGetsBlockLengthHint.FsDatasetChecker.SetFactory(conf);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, DefaultBlockLength);
			conf.SetInt(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, -1);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				cluster.WaitActive();
				// FsDatasetChecker#createRbw asserts during block creation if the test
				// fails.
				DFSTestUtil.CreateFile(cluster.GetFileSystem(), path, 4096, ExpectedBlockLength, 
					ExpectedBlockLength, (short)1, unchecked((int)(0x1BAD5EED)));
			}
			finally
			{
				// Buffer size.
				cluster.Shutdown();
			}
		}

		internal class FsDatasetChecker : SimulatedFSDataset
		{
			internal class Factory : FsDatasetSpi.Factory<SimulatedFSDataset>
			{
				/// <exception cref="System.IO.IOException"/>
				public override SimulatedFSDataset NewInstance(DataNode datanode, DataStorage storage
					, Configuration conf)
				{
					return new TestWriteBlockGetsBlockLengthHint.FsDatasetChecker(storage, conf);
				}

				public override bool IsSimulated()
				{
					return true;
				}
			}

			public static void SetFactory(Configuration conf)
			{
				conf.Set(DFSConfigKeys.DfsDatanodeFsdatasetFactoryKey, typeof(TestWriteBlockGetsBlockLengthHint.FsDatasetChecker.Factory
					).FullName);
			}

			public FsDatasetChecker(DataStorage storage, Configuration conf)
				: base(storage, conf)
			{
			}

			/// <summary>
			/// Override createRbw to verify that the block length that is passed
			/// is correct.
			/// </summary>
			/// <remarks>
			/// Override createRbw to verify that the block length that is passed
			/// is correct. This requires both DFSOutputStream and BlockReceiver to
			/// correctly propagate the hint to FsDatasetSpi.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public override ReplicaHandler CreateRbw(StorageType storageType, ExtendedBlock b
				, bool allowLazyPersist)
			{
				lock (this)
				{
					Assert.AssertThat(b.GetLocalBlock().GetNumBytes(), IS.Is(ExpectedBlockLength));
					return base.CreateRbw(storageType, b, allowLazyPersist);
				}
			}
		}
	}
}
