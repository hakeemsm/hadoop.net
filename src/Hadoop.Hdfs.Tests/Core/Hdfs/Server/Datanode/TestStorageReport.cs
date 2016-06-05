using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Hamcrest.Core;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestStorageReport
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestStorageReport));

		private const short ReplFactor = 1;

		private static readonly StorageType storageType = StorageType.Ssd;

		private static Configuration conf;

		private MiniDFSCluster cluster;

		private DistributedFileSystem fs;

		internal static string bpid;

		// pick non-default.
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void StartUpCluster()
		{
			conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(ReplFactor).StorageTypes(
				new StorageType[] { storageType, storageType }).Build();
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

		/// <summary>
		/// Ensure that storage type and storage state are propagated
		/// in Storage Reports.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStorageReportHasStorageTypeAndState()
		{
			// Make sure we are not testing with the default type, that would not
			// be a very good test.
			NUnit.Framework.Assert.AreNotSame(storageType, StorageType.Default);
			NameNode nn = cluster.GetNameNode();
			DataNode dn = cluster.GetDataNodes()[0];
			// Insert a spy object for the NN RPC.
			DatanodeProtocolClientSideTranslatorPB nnSpy = DataNodeTestUtils.SpyOnBposToNN(dn
				, nn);
			// Trigger a heartbeat so there is an interaction with the spy
			// object.
			DataNodeTestUtils.TriggerHeartbeat(dn);
			// Verify that the callback passed in the expected parameters.
			ArgumentCaptor<StorageReport[]> captor = ArgumentCaptor.ForClass<StorageReport[]>
				();
			Org.Mockito.Mockito.Verify(nnSpy).SendHeartbeat(Matchers.Any<DatanodeRegistration
				>(), captor.Capture(), Matchers.AnyLong(), Matchers.AnyLong(), Matchers.AnyInt()
				, Matchers.AnyInt(), Matchers.AnyInt(), Org.Mockito.Mockito.Any<VolumeFailureSummary
				>());
			StorageReport[] reports = captor.GetValue();
			foreach (StorageReport report in reports)
			{
				Assert.AssertThat(report.GetStorage().GetStorageType(), IS.Is(storageType));
				Assert.AssertThat(report.GetStorage().GetState(), IS.Is(DatanodeStorage.State.Normal
					));
			}
		}
	}
}
