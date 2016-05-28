using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class is used to specify the setup of namenodes when instantiating
	/// a MiniDFSCluster.
	/// </summary>
	/// <remarks>
	/// This class is used to specify the setup of namenodes when instantiating
	/// a MiniDFSCluster. It consists of a set of nameservices, each of which
	/// may have one or more namenodes (in the case of HA)
	/// </remarks>
	public class MiniDFSNNTopology
	{
		private readonly IList<MiniDFSNNTopology.NSConf> nameservices = Lists.NewArrayList
			();

		private bool federation;

		public MiniDFSNNTopology()
		{
		}

		/// <summary>Set up a simple non-federated non-HA NN.</summary>
		public static Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology SimpleSingleNN(int nameNodePort
			, int nameNodeHttpPort)
		{
			return new Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf
				(null).AddNN(new MiniDFSNNTopology.NNConf(null).SetHttpPort(nameNodeHttpPort).SetIpcPort
				(nameNodePort)));
		}

		/// <summary>Set up an HA topology with a single HA nameservice.</summary>
		public static Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology SimpleHATopology()
		{
			return new Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf
				("minidfs-ns").AddNN(new MiniDFSNNTopology.NNConf("nn1")).AddNN(new MiniDFSNNTopology.NNConf
				("nn2")));
		}

		/// <summary>
		/// Set up federated cluster with the given number of nameservices, each
		/// of which has only a single NameNode.
		/// </summary>
		public static Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology SimpleFederatedTopology(int
			 numNameservices)
		{
			Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology topology = new Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology
				();
			for (int i = 1; i <= numNameservices; i++)
			{
				topology.AddNameservice(new MiniDFSNNTopology.NSConf("ns" + i).AddNN(new MiniDFSNNTopology.NNConf
					(null)));
			}
			topology.SetFederation(true);
			return topology;
		}

		/// <summary>
		/// Set up federated cluster with the given nameservices, each
		/// of which has only a single NameNode.
		/// </summary>
		public static Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology SimpleFederatedTopology(string
			 nameservicesIds)
		{
			Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology topology = new Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology
				();
			string[] nsIds = nameservicesIds.Split(",");
			foreach (string nsId in nsIds)
			{
				topology.AddNameservice(new MiniDFSNNTopology.NSConf(nsId).AddNN(new MiniDFSNNTopology.NNConf
					(null)));
			}
			topology.SetFederation(true);
			return topology;
		}

		/// <summary>
		/// Set up federated cluster with the given number of nameservices, each
		/// of which has two NameNodes.
		/// </summary>
		public static Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology SimpleHAFederatedTopology(
			int numNameservices)
		{
			Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology topology = new Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology
				();
			for (int i = 0; i < numNameservices; i++)
			{
				topology.AddNameservice(new MiniDFSNNTopology.NSConf("ns" + i).AddNN(new MiniDFSNNTopology.NNConf
					("nn0")).AddNN(new MiniDFSNNTopology.NNConf("nn1")));
			}
			topology.SetFederation(true);
			return topology;
		}

		public virtual Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology SetFederation(bool federation
			)
		{
			this.federation = federation;
			return this;
		}

		public virtual Org.Apache.Hadoop.Hdfs.MiniDFSNNTopology AddNameservice(MiniDFSNNTopology.NSConf
			 nameservice)
		{
			Preconditions.CheckArgument(!nameservice.GetNNs().IsEmpty(), "Must have at least one NN in a nameservice"
				);
			this.nameservices.AddItem(nameservice);
			return this;
		}

		public virtual int CountNameNodes()
		{
			int count = 0;
			foreach (MiniDFSNNTopology.NSConf ns in nameservices)
			{
				count += ns.nns.Count;
			}
			return count;
		}

		public virtual MiniDFSNNTopology.NNConf GetOnlyNameNode()
		{
			Preconditions.CheckState(CountNameNodes() == 1, "must have exactly one NN!");
			return nameservices[0].GetNNs()[0];
		}

		public virtual bool IsFederated()
		{
			return nameservices.Count > 1 || federation;
		}

		/// <returns>
		/// true if at least one of the nameservices
		/// in the topology has HA enabled.
		/// </returns>
		public virtual bool IsHA()
		{
			foreach (MiniDFSNNTopology.NSConf ns in nameservices)
			{
				if (ns.GetNNs().Count > 1)
				{
					return true;
				}
			}
			return false;
		}

		/// <returns>
		/// true if all of the NNs in the cluster have their HTTP
		/// port specified to be non-ephemeral.
		/// </returns>
		public virtual bool AllHttpPortsSpecified()
		{
			foreach (MiniDFSNNTopology.NSConf ns in nameservices)
			{
				foreach (MiniDFSNNTopology.NNConf nn in ns.GetNNs())
				{
					if (nn.GetHttpPort() == 0)
					{
						return false;
					}
				}
			}
			return true;
		}

		/// <returns>
		/// true if all of the NNs in the cluster have their IPC
		/// port specified to be non-ephemeral.
		/// </returns>
		public virtual bool AllIpcPortsSpecified()
		{
			foreach (MiniDFSNNTopology.NSConf ns in nameservices)
			{
				foreach (MiniDFSNNTopology.NNConf nn in ns.GetNNs())
				{
					if (nn.GetIpcPort() == 0)
					{
						return false;
					}
				}
			}
			return true;
		}

		public virtual IList<MiniDFSNNTopology.NSConf> GetNameservices()
		{
			return nameservices;
		}

		public class NSConf
		{
			private readonly string id;

			private readonly IList<MiniDFSNNTopology.NNConf> nns = Lists.NewArrayList();

			public NSConf(string id)
			{
				this.id = id;
			}

			public virtual MiniDFSNNTopology.NSConf AddNN(MiniDFSNNTopology.NNConf nn)
			{
				this.nns.AddItem(nn);
				return this;
			}

			public virtual string GetId()
			{
				return id;
			}

			public virtual IList<MiniDFSNNTopology.NNConf> GetNNs()
			{
				return nns;
			}
		}

		public class NNConf
		{
			private readonly string nnId;

			private int httpPort;

			private int ipcPort;

			private string clusterId;

			public NNConf(string nnId)
			{
				this.nnId = nnId;
			}

			internal virtual string GetNnId()
			{
				return nnId;
			}

			internal virtual int GetIpcPort()
			{
				return ipcPort;
			}

			internal virtual int GetHttpPort()
			{
				return httpPort;
			}

			internal virtual string GetClusterId()
			{
				return clusterId;
			}

			public virtual MiniDFSNNTopology.NNConf SetHttpPort(int httpPort)
			{
				this.httpPort = httpPort;
				return this;
			}

			public virtual MiniDFSNNTopology.NNConf SetIpcPort(int ipcPort)
			{
				this.ipcPort = ipcPort;
				return this;
			}

			public virtual MiniDFSNNTopology.NNConf SetClusterId(string clusterId)
			{
				this.clusterId = clusterId;
				return this;
			}
		}
	}
}
