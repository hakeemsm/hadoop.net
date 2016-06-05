using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Hamcrest.Core;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Tests that the DataNode respects
	/// <see cref="Org.Apache.Hadoop.Hdfs.DFSConfigKeys.DfsBlockreportSplitThresholdKey"/
	/// 	>
	/// </summary>
	public class TestDnRespectsBlockReportSplitThreshold
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestStorageReport));

		private const int BlockSize = 1024;

		private const short ReplFactor = 1;

		private const long seed = unchecked((int)(0xFEEDFACE));

		private const int BlocksInFile = 5;

		private static Configuration conf;

		private MiniDFSCluster cluster;

		private DistributedFileSystem fs;

		internal static string bpid;

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartUpCluster(long splitThreshold)
		{
			conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockreportSplitThresholdKey, splitThreshold);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(ReplFactor).Build();
			fs = cluster.GetFileSystem();
			bpid = cluster.GetNamesystem().GetBlockPoolId();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutDownCluster()
		{
			if (cluster != null)
			{
				fs.Close();
				cluster.Shutdown();
				cluster = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateFile(string filenamePrefix, int blockCount)
		{
			Path path = new Path("/" + filenamePrefix + ".dat");
			DFSTestUtil.CreateFile(fs, path, BlockSize, blockCount * BlockSize, BlockSize, ReplFactor
				, seed);
		}

		private void VerifyCapturedArguments(ArgumentCaptor<StorageBlockReport[]> captor, 
			int expectedReportsPerCall, int expectedTotalBlockCount)
		{
			IList<StorageBlockReport[]> listOfReports = captor.GetAllValues();
			int numBlocksReported = 0;
			foreach (StorageBlockReport[] reports in listOfReports)
			{
				Assert.AssertThat(reports.Length, IS.Is(expectedReportsPerCall));
				foreach (StorageBlockReport report in reports)
				{
					BlockListAsLongs blockList = report.GetBlocks();
					numBlocksReported += blockList.GetNumberOfBlocks();
				}
			}
			System.Diagnostics.Debug.Assert((numBlocksReported >= expectedTotalBlockCount));
		}

		/// <summary>
		/// Test that if splitThreshold is zero, then we always get a separate
		/// call per storage.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestAlwaysSplit()
		{
			StartUpCluster(0);
			NameNode nn = cluster.GetNameNode();
			DataNode dn = cluster.GetDataNodes()[0];
			// Create a file with a few blocks.
			CreateFile(GenericTestUtils.GetMethodName(), BlocksInFile);
			// Insert a spy object for the NN RPC.
			DatanodeProtocolClientSideTranslatorPB nnSpy = DataNodeTestUtils.SpyOnBposToNN(dn
				, nn);
			// Trigger a block report so there is an interaction with the spy
			// object.
			DataNodeTestUtils.TriggerBlockReport(dn);
			ArgumentCaptor<StorageBlockReport[]> captor = ArgumentCaptor.ForClass<StorageBlockReport
				[]>();
			Org.Mockito.Mockito.Verify(nnSpy, Org.Mockito.Mockito.Times(cluster.GetStoragesPerDatanode
				())).BlockReport(Matchers.Any<DatanodeRegistration>(), Matchers.AnyString(), captor
				.Capture(), Org.Mockito.Mockito.AnyObject<BlockReportContext>());
			VerifyCapturedArguments(captor, 1, BlocksInFile);
		}

		/// <summary>
		/// Tests the behavior when the count of blocks is exactly one less than
		/// the threshold.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestCornerCaseUnderThreshold()
		{
			StartUpCluster(BlocksInFile + 1);
			NameNode nn = cluster.GetNameNode();
			DataNode dn = cluster.GetDataNodes()[0];
			// Create a file with a few blocks.
			CreateFile(GenericTestUtils.GetMethodName(), BlocksInFile);
			// Insert a spy object for the NN RPC.
			DatanodeProtocolClientSideTranslatorPB nnSpy = DataNodeTestUtils.SpyOnBposToNN(dn
				, nn);
			// Trigger a block report so there is an interaction with the spy
			// object.
			DataNodeTestUtils.TriggerBlockReport(dn);
			ArgumentCaptor<StorageBlockReport[]> captor = ArgumentCaptor.ForClass<StorageBlockReport
				[]>();
			Org.Mockito.Mockito.Verify(nnSpy, Org.Mockito.Mockito.Times(1)).BlockReport(Matchers.Any
				<DatanodeRegistration>(), Matchers.AnyString(), captor.Capture(), Org.Mockito.Mockito
				.AnyObject<BlockReportContext>());
			VerifyCapturedArguments(captor, cluster.GetStoragesPerDatanode(), BlocksInFile);
		}

		/// <summary>
		/// Tests the behavior when the count of blocks is exactly equal to the
		/// threshold.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestCornerCaseAtThreshold()
		{
			StartUpCluster(BlocksInFile);
			NameNode nn = cluster.GetNameNode();
			DataNode dn = cluster.GetDataNodes()[0];
			// Create a file with a few blocks.
			CreateFile(GenericTestUtils.GetMethodName(), BlocksInFile);
			// Insert a spy object for the NN RPC.
			DatanodeProtocolClientSideTranslatorPB nnSpy = DataNodeTestUtils.SpyOnBposToNN(dn
				, nn);
			// Trigger a block report so there is an interaction with the spy
			// object.
			DataNodeTestUtils.TriggerBlockReport(dn);
			ArgumentCaptor<StorageBlockReport[]> captor = ArgumentCaptor.ForClass<StorageBlockReport
				[]>();
			Org.Mockito.Mockito.Verify(nnSpy, Org.Mockito.Mockito.Times(cluster.GetStoragesPerDatanode
				())).BlockReport(Matchers.Any<DatanodeRegistration>(), Matchers.AnyString(), captor
				.Capture(), Org.Mockito.Mockito.AnyObject<BlockReportContext>());
			VerifyCapturedArguments(captor, 1, BlocksInFile);
		}
	}
}
