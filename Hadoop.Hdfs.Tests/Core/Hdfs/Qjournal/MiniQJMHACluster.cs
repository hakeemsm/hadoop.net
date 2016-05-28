using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal
{
	public class MiniQJMHACluster
	{
		private MiniDFSCluster cluster;

		private MiniJournalCluster journalCluster;

		private readonly Configuration conf;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Qjournal.MiniQJMHACluster
			));

		public const string Nameservice = "ns1";

		private const string Nn1 = "nn1";

		private const string Nn2 = "nn2";

		private static readonly Random Random = new Random();

		private int basePort = 10000;

		public class Builder
		{
			private readonly Configuration conf;

			private HdfsServerConstants.StartupOption startOpt = null;

			private readonly MiniDFSCluster.Builder dfsBuilder;

			public Builder(Configuration conf)
			{
				this.conf = conf;
				// most QJMHACluster tests don't need DataNodes, so we'll make
				// this the default
				this.dfsBuilder = new MiniDFSCluster.Builder(conf).NumDataNodes(0);
			}

			public virtual MiniDFSCluster.Builder GetDfsBuilder()
			{
				return dfsBuilder;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual MiniQJMHACluster Build()
			{
				return new MiniQJMHACluster(this);
			}

			public virtual void StartupOption(HdfsServerConstants.StartupOption startOpt)
			{
				this.startOpt = startOpt;
			}
		}

		public static MiniDFSNNTopology CreateDefaultTopology(int basePort)
		{
			return new MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf(Nameservice
				).AddNN(new MiniDFSNNTopology.NNConf("nn1").SetIpcPort(basePort).SetHttpPort(basePort
				 + 1)).AddNN(new MiniDFSNNTopology.NNConf("nn2").SetIpcPort(basePort + 2).SetHttpPort
				(basePort + 3)));
		}

		/// <exception cref="System.IO.IOException"/>
		private MiniQJMHACluster(MiniQJMHACluster.Builder builder)
		{
			this.conf = builder.conf;
			int retryCount = 0;
			while (true)
			{
				try
				{
					basePort = 10000 + Random.Next(1000) * 4;
					// start 3 journal nodes
					journalCluster = new MiniJournalCluster.Builder(conf).Format(true).Build();
					URI journalURI = journalCluster.GetQuorumJournalURI(Nameservice);
					// start cluster with 2 NameNodes
					MiniDFSNNTopology topology = CreateDefaultTopology(basePort);
					InitHAConf(journalURI, builder.conf);
					// First start up the NNs just to format the namespace. The MinIDFSCluster
					// has no way to just format the NameNodes without also starting them.
					cluster = builder.dfsBuilder.NnTopology(topology).ManageNameDfsSharedDirs(false).
						Build();
					cluster.WaitActive();
					cluster.ShutdownNameNodes();
					// initialize the journal nodes
					Configuration confNN0 = cluster.GetConfiguration(0);
					NameNode.InitializeSharedEdits(confNN0, true);
					cluster.GetNameNodeInfos()[0].SetStartOpt(builder.startOpt);
					cluster.GetNameNodeInfos()[1].SetStartOpt(builder.startOpt);
					// restart the cluster
					cluster.RestartNameNodes();
					++retryCount;
					break;
				}
				catch (BindException)
				{
					Log.Info("MiniQJMHACluster port conflicts, retried " + retryCount + " times");
				}
			}
		}

		private Configuration InitHAConf(URI journalURI, Configuration conf)
		{
			conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, journalURI.ToString());
			string address1 = "127.0.0.1:" + basePort;
			string address2 = "127.0.0.1:" + (basePort + 2);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, Nameservice
				, Nn1), address1);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, Nameservice
				, Nn2), address2);
			conf.Set(DFSConfigKeys.DfsNameservices, Nameservice);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, Nameservice
				), Nn1 + "," + Nn2);
			conf.Set(DFSConfigKeys.DfsClientFailoverProxyProviderKeyPrefix + "." + Nameservice
				, typeof(ConfiguredFailoverProxyProvider).FullName);
			conf.Set("fs.defaultFS", "hdfs://" + Nameservice);
			return conf;
		}

		public virtual MiniDFSCluster GetDfsCluster()
		{
			return cluster;
		}

		public virtual MiniJournalCluster GetJournalCluster()
		{
			return journalCluster;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Shutdown()
		{
			cluster.Shutdown();
			journalCluster.Shutdown();
		}
	}
}
