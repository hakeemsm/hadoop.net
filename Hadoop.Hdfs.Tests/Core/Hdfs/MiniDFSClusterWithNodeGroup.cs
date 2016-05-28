using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class MiniDFSClusterWithNodeGroup : MiniDFSCluster
	{
		private static string[] NodeGroups = null;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.MiniDFSClusterWithNodeGroup
			));

		/// <exception cref="System.IO.IOException"/>
		public MiniDFSClusterWithNodeGroup(MiniDFSCluster.Builder builder)
			: base(builder)
		{
		}

		public static void SetNodeGroups(string[] nodeGroups)
		{
			NodeGroups = nodeGroups;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartDataNodes(Configuration conf, int numDataNodes, StorageType
			[][] storageTypes, bool manageDfsDirs, HdfsServerConstants.StartupOption operation
			, string[] racks, string[] nodeGroups, string[] hosts, long[][] storageCapacities
			, long[] simulatedCapacities, bool setupHostsFile, bool checkDataNodeAddrConfig, 
			bool checkDataNodeHostConfig)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(storageCapacities == null || simulatedCapacities 
					== null);
				System.Diagnostics.Debug.Assert(storageTypes == null || storageTypes.Length == numDataNodes
					);
				System.Diagnostics.Debug.Assert(storageCapacities == null || storageCapacities.Length
					 == numDataNodes);
				if (operation == HdfsServerConstants.StartupOption.Recover)
				{
					return;
				}
				if (checkDataNodeHostConfig)
				{
					conf.SetIfUnset(DFSConfigKeys.DfsDatanodeHostNameKey, "127.0.0.1");
				}
				else
				{
					conf.Set(DFSConfigKeys.DfsDatanodeHostNameKey, "127.0.0.1");
				}
				conf.Set(DFSConfigKeys.DfsDatanodeHostNameKey, "127.0.0.1");
				int curDatanodesNum = dataNodes.Count;
				// for mincluster's the default initialDelay for BRs is 0
				if (conf.Get(DFSConfigKeys.DfsBlockreportInitialDelayKey) == null)
				{
					conf.SetLong(DFSConfigKeys.DfsBlockreportInitialDelayKey, 0);
				}
				// If minicluster's name node is null assume that the conf has been
				// set with the right address:port of the name node.
				//
				if (racks != null && numDataNodes > racks.Length)
				{
					throw new ArgumentException("The length of racks [" + racks.Length + "] is less than the number of datanodes ["
						 + numDataNodes + "].");
				}
				if (nodeGroups != null && numDataNodes > nodeGroups.Length)
				{
					throw new ArgumentException("The length of nodeGroups [" + nodeGroups.Length + "] is less than the number of datanodes ["
						 + numDataNodes + "].");
				}
				if (hosts != null && numDataNodes > hosts.Length)
				{
					throw new ArgumentException("The length of hosts [" + hosts.Length + "] is less than the number of datanodes ["
						 + numDataNodes + "].");
				}
				//Generate some hostnames if required
				if (racks != null && hosts == null)
				{
					hosts = new string[numDataNodes];
					for (int i = curDatanodesNum; i < curDatanodesNum + numDataNodes; i++)
					{
						hosts[i - curDatanodesNum] = "host" + i + ".foo.com";
					}
				}
				if (simulatedCapacities != null && numDataNodes > simulatedCapacities.Length)
				{
					throw new ArgumentException("The length of simulatedCapacities [" + simulatedCapacities
						.Length + "] is less than the number of datanodes [" + numDataNodes + "].");
				}
				string[] dnArgs = (operation == null || operation != HdfsServerConstants.StartupOption
					.Rollback) ? null : new string[] { operation.GetName() };
				DataNode[] dns = new DataNode[numDataNodes];
				for (int i_1 = curDatanodesNum; i_1 < curDatanodesNum + numDataNodes; i_1++)
				{
					Configuration dnConf = new HdfsConfiguration(conf);
					// Set up datanode address
					SetupDatanodeAddress(dnConf, setupHostsFile, checkDataNodeAddrConfig);
					if (manageDfsDirs)
					{
						string dirs = MakeDataNodeDirs(i_1, storageTypes == null ? null : storageTypes[i_1
							]);
						dnConf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, dirs);
						conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, dirs);
					}
					if (simulatedCapacities != null)
					{
						SimulatedFSDataset.SetFactory(dnConf);
						dnConf.SetLong(SimulatedFSDataset.ConfigPropertyCapacity, simulatedCapacities[i_1
							 - curDatanodesNum]);
					}
					Log.Info("Starting DataNode " + i_1 + " with " + DFSConfigKeys.DfsDatanodeDataDirKey
						 + ": " + dnConf.Get(DFSConfigKeys.DfsDatanodeDataDirKey));
					if (hosts != null)
					{
						dnConf.Set(DFSConfigKeys.DfsDatanodeHostNameKey, hosts[i_1 - curDatanodesNum]);
						Log.Info("Starting DataNode " + i_1 + " with hostname set to: " + dnConf.Get(DFSConfigKeys
							.DfsDatanodeHostNameKey));
					}
					if (racks != null)
					{
						string name = hosts[i_1 - curDatanodesNum];
						if (nodeGroups == null)
						{
							Log.Info("Adding node with hostname : " + name + " to rack " + racks[i_1 - curDatanodesNum
								]);
							StaticMapping.AddNodeToRack(name, racks[i_1 - curDatanodesNum]);
						}
						else
						{
							Log.Info("Adding node with hostname : " + name + " to serverGroup " + nodeGroups[
								i_1 - curDatanodesNum] + " and rack " + racks[i_1 - curDatanodesNum]);
							StaticMapping.AddNodeToRack(name, racks[i_1 - curDatanodesNum] + nodeGroups[i_1 -
								 curDatanodesNum]);
						}
					}
					Configuration newconf = new HdfsConfiguration(dnConf);
					// save config
					if (hosts != null)
					{
						NetUtils.AddStaticResolution(hosts[i_1 - curDatanodesNum], "localhost");
					}
					SecureDataNodeStarter.SecureResources secureResources = null;
					if (UserGroupInformation.IsSecurityEnabled())
					{
						try
						{
							secureResources = SecureDataNodeStarter.GetSecureResources(dnConf);
						}
						catch (Exception ex)
						{
							Sharpen.Runtime.PrintStackTrace(ex);
						}
					}
					DataNode dn = DataNode.InstantiateDataNode(dnArgs, dnConf, secureResources);
					if (dn == null)
					{
						throw new IOException("Cannot start DataNode in " + dnConf.Get(DFSConfigKeys.DfsDatanodeDataDirKey
							));
					}
					//since the HDFS does things based on IP:port, we need to add the mapping
					//for IP:port to rackId
					string ipAddr = dn.GetXferAddress().Address.GetHostAddress();
					if (racks != null)
					{
						int port = dn.GetXferAddress().Port;
						if (nodeGroups == null)
						{
							Log.Info("Adding node with IP:port : " + ipAddr + ":" + port + " to rack " + racks
								[i_1 - curDatanodesNum]);
							StaticMapping.AddNodeToRack(ipAddr + ":" + port, racks[i_1 - curDatanodesNum]);
						}
						else
						{
							Log.Info("Adding node with IP:port : " + ipAddr + ":" + port + " to nodeGroup " +
								 nodeGroups[i_1 - curDatanodesNum] + " and rack " + racks[i_1 - curDatanodesNum]
								);
							StaticMapping.AddNodeToRack(ipAddr + ":" + port, racks[i_1 - curDatanodesNum] + nodeGroups
								[i_1 - curDatanodesNum]);
						}
					}
					dn.RunDatanodeDaemon();
					dataNodes.AddItem(new MiniDFSCluster.DataNodeProperties(this, dn, newconf, dnArgs
						, secureResources, dn.GetIpcPort()));
					dns[i_1 - curDatanodesNum] = dn;
				}
				curDatanodesNum += numDataNodes;
				this.numDataNodes += numDataNodes;
				WaitActive();
				if (storageCapacities != null)
				{
					for (int i = curDatanodesNum; i_1 < curDatanodesNum + numDataNodes; ++i_1)
					{
						IList<FsVolumeSpi> volumes = dns[i_1].GetFSDataset().GetVolumes();
						System.Diagnostics.Debug.Assert(volumes.Count == storagesPerDatanode);
						for (int j = 0; j < volumes.Count; ++j)
						{
							FsVolumeImpl volume = (FsVolumeImpl)volumes[j];
							volume.SetCapacityForTesting(storageCapacities[i_1][j]);
						}
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartDataNodes(Configuration conf, int numDataNodes, bool manageDfsDirs
			, HdfsServerConstants.StartupOption operation, string[] racks, string[] nodeGroups
			, string[] hosts, long[] simulatedCapacities, bool setupHostsFile)
		{
			lock (this)
			{
				StartDataNodes(conf, numDataNodes, null, manageDfsDirs, operation, racks, nodeGroups
					, hosts, null, simulatedCapacities, setupHostsFile, false, false);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartDataNodes(Configuration conf, int numDataNodes, bool manageDfsDirs
			, HdfsServerConstants.StartupOption operation, string[] racks, long[] simulatedCapacities
			, string[] nodeGroups)
		{
			StartDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, nodeGroups, null
				, simulatedCapacities, false);
		}

		// This is for initialize from parent class.
		/// <exception cref="System.IO.IOException"/>
		public override void StartDataNodes(Configuration conf, int numDataNodes, StorageType
			[][] storageTypes, bool manageDfsDirs, HdfsServerConstants.StartupOption operation
			, string[] racks, string[] hosts, long[][] storageCapacities, long[] simulatedCapacities
			, bool setupHostsFile, bool checkDataNodeAddrConfig, bool checkDataNodeHostConfig
			, Configuration[] dnConfOverlays)
		{
			lock (this)
			{
				StartDataNodes(conf, numDataNodes, storageTypes, manageDfsDirs, operation, racks, 
					NodeGroups, hosts, storageCapacities, simulatedCapacities, setupHostsFile, checkDataNodeAddrConfig
					, checkDataNodeHostConfig);
			}
		}
	}
}
