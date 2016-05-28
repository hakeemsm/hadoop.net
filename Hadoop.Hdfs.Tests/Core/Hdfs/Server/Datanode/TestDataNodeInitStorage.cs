using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Test to verify that the DataNode Uuid is correctly initialized before
	/// FsDataSet initialization.
	/// </summary>
	public class TestDataNodeInitStorage
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.TestDataNodeInitStorage
			));

		private class SimulatedFsDatasetVerifier : SimulatedFSDataset
		{
			internal class Factory : FsDatasetSpi.Factory<SimulatedFSDataset>
			{
				/// <exception cref="System.IO.IOException"/>
				public override SimulatedFSDataset NewInstance(DataNode datanode, DataStorage storage
					, Configuration conf)
				{
					return new TestDataNodeInitStorage.SimulatedFsDatasetVerifier(storage, conf);
				}

				public override bool IsSimulated()
				{
					return true;
				}
			}

			public static void SetFactory(Configuration conf)
			{
				conf.Set(DFSConfigKeys.DfsDatanodeFsdatasetFactoryKey, typeof(TestDataNodeInitStorage.SimulatedFsDatasetVerifier.Factory
					).FullName);
			}

			public SimulatedFsDatasetVerifier(DataStorage storage, Configuration conf)
				: base(storage, conf)
			{
				// This constructor does the actual verification by ensuring that
				// the DatanodeUuid is initialized.
				Log.Info("Assigned DatanodeUuid is " + storage.GetDatanodeUuid());
				System.Diagnostics.Debug.Assert((storage.GetDatanodeUuid() != null));
				System.Diagnostics.Debug.Assert((storage.GetDatanodeUuid().Length != 0));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDataNodeInitStorage()
		{
			// Create configuration to use SimulatedFsDatasetVerifier#Factory.
			Configuration conf = new HdfsConfiguration();
			TestDataNodeInitStorage.SimulatedFsDatasetVerifier.SetFactory(conf);
			// Start a cluster so that SimulatedFsDatasetVerifier constructor is
			// invoked.
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			cluster.Shutdown();
		}
	}
}
