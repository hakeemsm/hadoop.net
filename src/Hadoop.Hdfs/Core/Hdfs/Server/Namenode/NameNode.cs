using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Javax.Management;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Tracing;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// NameNode serves as both directory namespace manager and
	/// "inode table" for the Hadoop DFS.
	/// </summary>
	/// <remarks>
	/// NameNode serves as both directory namespace manager and
	/// "inode table" for the Hadoop DFS.  There is a single NameNode
	/// running in any DFS deployment.  (Well, except when there
	/// is a second backup/failover NameNode, or when using federated NameNodes.)
	/// The NameNode controls two critical tables:
	/// 1)  filename-&gt;blocksequence (namespace)
	/// 2)  block-&gt;machinelist ("inodes")
	/// The first table is stored on disk and is very precious.
	/// The second table is rebuilt every time the NameNode comes up.
	/// 'NameNode' refers to both this class as well as the 'NameNode server'.
	/// The 'FSNamesystem' class actually performs most of the filesystem
	/// management.  The majority of the 'NameNode' class itself is concerned
	/// with exposing the IPC interface and the HTTP server to the outside world,
	/// plus some configuration management.
	/// NameNode implements the
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol"/>
	/// interface, which
	/// allows clients to ask for DFS services.
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol"/>
	/// is not designed for
	/// direct use by authors of DFS client code.  End-users should instead use the
	/// <see cref="Org.Apache.Hadoop.FS.FileSystem"/>
	/// class.
	/// NameNode also implements the
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeProtocol"/>
	/// interface,
	/// used by DataNodes that actually store DFS data blocks.  These
	/// methods are invoked repeatedly and automatically by all the
	/// DataNodes in a DFS deployment.
	/// NameNode also implements the
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.NamenodeProtocol"/>
	/// interface,
	/// used by secondary namenodes or rebalancing processes to get partial
	/// NameNode state, for example partial blocksMap etc.
	/// </remarks>
	public class NameNode : NameNodeStatusMXBean
	{
		static NameNode()
		{
			HdfsConfiguration.Init();
		}

		/// <summary>Categories of operations supported by the namenode.</summary>
		public enum OperationCategory
		{
			Unchecked,
			Read,
			Write,
			Checkpoint,
			Journal
		}

		/// <summary>
		/// HDFS configuration can have three types of parameters:
		/// <ol>
		/// <li>Parameters that are common for all the name services in the cluster.</li>
		/// <li>Parameters that are specific to a name service.
		/// </summary>
		/// <remarks>
		/// HDFS configuration can have three types of parameters:
		/// <ol>
		/// <li>Parameters that are common for all the name services in the cluster.</li>
		/// <li>Parameters that are specific to a name service. These keys are suffixed
		/// with nameserviceId in the configuration. For example,
		/// "dfs.namenode.rpc-address.nameservice1".</li>
		/// <li>Parameters that are specific to a single name node. These keys are suffixed
		/// with nameserviceId and namenodeId in the configuration. for example,
		/// "dfs.namenode.rpc-address.nameservice1.namenode1"</li>
		/// </ol>
		/// In the latter cases, operators may specify the configuration without
		/// any suffix, with a nameservice suffix, or with a nameservice and namenode
		/// suffix. The more specific suffix will take precedence.
		/// These keys are specific to a given namenode, and thus may be configured
		/// globally, for a nameservice, or for a specific namenode within a nameservice.
		/// </remarks>
		public static readonly string[] NamenodeSpecificKeys = new string[] { DFSConfigKeys
			.DfsNamenodeRpcAddressKey, DFSConfigKeys.DfsNamenodeRpcBindHostKey, DFSConfigKeys
			.DfsNamenodeNameDirKey, DFSConfigKeys.DfsNamenodeEditsDirKey, DFSConfigKeys.DfsNamenodeSharedEditsDirKey
			, DFSConfigKeys.DfsNamenodeCheckpointDirKey, DFSConfigKeys.DfsNamenodeCheckpointEditsDirKey
			, DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, DFSConfigKeys.DfsNamenodeServiceRpcBindHostKey
			, DFSConfigKeys.DfsNamenodeHttpAddressKey, DFSConfigKeys.DfsNamenodeHttpsAddressKey
			, DFSConfigKeys.DfsNamenodeHttpBindHostKey, DFSConfigKeys.DfsNamenodeHttpsBindHostKey
			, DFSConfigKeys.DfsNamenodeKeytabFileKey, DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey
			, DFSConfigKeys.DfsNamenodeSecondaryHttpsAddressKey, DFSConfigKeys.DfsSecondaryNamenodeKeytabFileKey
			, DFSConfigKeys.DfsNamenodeBackupAddressKey, DFSConfigKeys.DfsNamenodeBackupHttpAddressKey
			, DFSConfigKeys.DfsNamenodeBackupServiceRpcAddressKey, DFSConfigKeys.DfsNamenodeKerberosPrincipalKey
			, DFSConfigKeys.DfsNamenodeKerberosInternalSpnegoPrincipalKey, DFSConfigKeys.DfsHaFenceMethodsKey
			, DFSConfigKeys.DfsHaZkfcPortKey, DFSConfigKeys.DfsHaFenceMethodsKey };

		/// <seealso cref="NamenodeSpecificKeys">
		/// These keys are specific to a nameservice, but may not be overridden
		/// for a specific namenode.
		/// </seealso>
		public static readonly string[] NameserviceSpecificKeys = new string[] { DFSConfigKeys
			.DfsHaAutoFailoverEnabledKey };

		private static readonly string Usage = "Usage: java NameNode [" + HdfsServerConstants.StartupOption
			.Backup.GetName() + "] | \n\t[" + HdfsServerConstants.StartupOption.Checkpoint.GetName
			() + "] | \n\t[" + HdfsServerConstants.StartupOption.Format.GetName() + " [" + HdfsServerConstants.StartupOption
			.Clusterid.GetName() + " cid ] [" + HdfsServerConstants.StartupOption.Force.GetName
			() + "] [" + HdfsServerConstants.StartupOption.Noninteractive.GetName() + "] ] | \n\t["
			 + HdfsServerConstants.StartupOption.Upgrade.GetName() + " [" + HdfsServerConstants.StartupOption
			.Clusterid.GetName() + " cid]" + " [" + HdfsServerConstants.StartupOption.Renamereserved
			.GetName() + "<k-v pairs>] ] | \n\t[" + HdfsServerConstants.StartupOption.Upgradeonly
			.GetName() + " [" + HdfsServerConstants.StartupOption.Clusterid.GetName() + " cid]"
			 + " [" + HdfsServerConstants.StartupOption.Renamereserved.GetName() + "<k-v pairs>] ] | \n\t["
			 + HdfsServerConstants.StartupOption.Rollback.GetName() + "] | \n\t[" + HdfsServerConstants.StartupOption
			.Rollingupgrade.GetName() + " " + HdfsServerConstants.RollingUpgradeStartupOption
			.GetAllOptionString() + " ] | \n\t[" + HdfsServerConstants.StartupOption.Finalize
			.GetName() + "] | \n\t[" + HdfsServerConstants.StartupOption.Import.GetName() + 
			"] | \n\t[" + HdfsServerConstants.StartupOption.Initializesharededits.GetName() 
			+ "] | \n\t[" + HdfsServerConstants.StartupOption.Bootstrapstandby.GetName() + "] | \n\t["
			 + HdfsServerConstants.StartupOption.Recover.GetName() + " [ " + HdfsServerConstants.StartupOption
			.Force.GetName() + "] ] | \n\t[" + HdfsServerConstants.StartupOption.Metadataversion
			.GetName() + " ] " + " ]";

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetProtocolVersion(string protocol, long clientVersion)
		{
			if (protocol.Equals(typeof(ClientProtocol).FullName))
			{
				return ClientProtocol.versionID;
			}
			else
			{
				if (protocol.Equals(typeof(DatanodeProtocol).FullName))
				{
					return DatanodeProtocol.versionID;
				}
				else
				{
					if (protocol.Equals(typeof(NamenodeProtocol).FullName))
					{
						return NamenodeProtocol.versionID;
					}
					else
					{
						if (protocol.Equals(typeof(RefreshAuthorizationPolicyProtocol).FullName))
						{
							return RefreshAuthorizationPolicyProtocol.versionID;
						}
						else
						{
							if (protocol.Equals(typeof(RefreshUserMappingsProtocol).FullName))
							{
								return RefreshUserMappingsProtocol.versionID;
							}
							else
							{
								if (protocol.Equals(typeof(RefreshCallQueueProtocol).FullName))
								{
									return RefreshCallQueueProtocol.versionID;
								}
								else
								{
									if (protocol.Equals(typeof(GetUserMappingsProtocol).FullName))
									{
										return GetUserMappingsProtocol.versionID;
									}
									else
									{
										if (protocol.Equals(typeof(TraceAdminProtocol).FullName))
										{
											return TraceAdminProtocol.versionID;
										}
										else
										{
											throw new IOException("Unknown protocol to name node: " + protocol);
										}
									}
								}
							}
						}
					}
				}
			}
		}

		public const int DefaultPort = 8020;

		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode
			).FullName);

		public static readonly Logger stateChangeLog = LoggerFactory.GetLogger("org.apache.hadoop.hdfs.StateChange"
			);

		public static readonly Logger blockStateChangeLog = LoggerFactory.GetLogger("BlockStateChange"
			);

		public static readonly HAState ActiveState = new ActiveState();

		public static readonly HAState StandbyState = new StandbyState();

		protected internal FSNamesystem namesystem;

		protected internal readonly Configuration conf;

		protected internal readonly HdfsServerConstants.NamenodeRole role;

		private volatile HAState state;

		private readonly bool haEnabled;

		private readonly HAContext haContext;

		protected internal readonly bool allowStaleStandbyReads;

		private AtomicBoolean started = new AtomicBoolean(false);

		/// <summary>httpServer</summary>
		protected internal NameNodeHttpServer httpServer;

		private Sharpen.Thread emptier;

		/// <summary>only used for testing purposes</summary>
		protected internal bool stopRequested = false;

		/// <summary>Registration information of this name-node</summary>
		protected internal NamenodeRegistration nodeRegistration;

		/// <summary>Activated plug-ins.</summary>
		private IList<ServicePlugin> plugins;

		private NameNodeRpcServer rpcServer;

		private JvmPauseMonitor pauseMonitor;

		private ObjectName nameNodeStatusBeanName;

		internal SpanReceiverHost spanReceiverHost;

		/// <summary>
		/// The namenode address that clients will use to access this namenode
		/// or the name service.
		/// </summary>
		/// <remarks>
		/// The namenode address that clients will use to access this namenode
		/// or the name service. For HA configurations using logical URI, it
		/// will be the logical address.
		/// </remarks>
		private string clientNamenodeAddress;

		/// <summary>Format a new filesystem.</summary>
		/// <remarks>
		/// Format a new filesystem.  Destroys any filesystem that may already
		/// exist at this location.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static void Format(Configuration conf)
		{
			Format(conf, true, true);
		}

		internal static NameNodeMetrics metrics;

		private static readonly StartupProgress startupProgress = new StartupProgress();

		/// <summary>
		/// Return the
		/// <see cref="FSNamesystem"/>
		/// object.
		/// </summary>
		/// <returns>
		/// 
		/// <see cref="FSNamesystem"/>
		/// object.
		/// </returns>
		public virtual FSNamesystem GetNamesystem()
		{
			return namesystem;
		}

		public virtual NamenodeProtocols GetRpcServer()
		{
			return rpcServer;
		}

		internal static void InitMetrics(Configuration conf, HdfsServerConstants.NamenodeRole
			 role)
		{
			metrics = NameNodeMetrics.Create(conf, role);
		}

		public static NameNodeMetrics GetNameNodeMetrics()
		{
			return metrics;
		}

		/// <summary>Returns object used for reporting namenode startup progress.</summary>
		/// <returns>StartupProgress for reporting namenode startup progress</returns>
		public static StartupProgress GetStartupProgress()
		{
			return startupProgress;
		}

		/// <summary>Return the service name of the issued delegation token.</summary>
		/// <returns>The name service id in HA-mode, or the rpc address in non-HA mode</returns>
		public virtual string GetTokenServiceName()
		{
			return GetClientNamenodeAddress();
		}

		/// <summary>
		/// Set the namenode address that will be used by clients to access this
		/// namenode or name service.
		/// </summary>
		/// <remarks>
		/// Set the namenode address that will be used by clients to access this
		/// namenode or name service. This needs to be called before the config
		/// is overriden.
		/// </remarks>
		public virtual void SetClientNamenodeAddress(Configuration conf)
		{
			string nnAddr = conf.Get(CommonConfigurationKeysPublic.FsDefaultNameKey);
			if (nnAddr == null)
			{
				// default fs is not set.
				clientNamenodeAddress = null;
				return;
			}
			Log.Info("{} is {}", CommonConfigurationKeysPublic.FsDefaultNameKey, nnAddr);
			URI nnUri = URI.Create(nnAddr);
			string nnHost = nnUri.GetHost();
			if (nnHost == null)
			{
				clientNamenodeAddress = null;
				return;
			}
			if (DFSUtil.GetNameServiceIds(conf).Contains(nnHost))
			{
				// host name is logical
				clientNamenodeAddress = nnHost;
			}
			else
			{
				if (nnUri.GetPort() > 0)
				{
					// physical address with a valid port
					clientNamenodeAddress = nnUri.GetAuthority();
				}
				else
				{
					// the port is missing or 0. Figure out real bind address later.
					clientNamenodeAddress = null;
					return;
				}
			}
			Log.Info("Clients are to use {} to access" + " this namenode/service.", clientNamenodeAddress
				);
		}

		/// <summary>Get the namenode address to be used by clients.</summary>
		/// <returns>nn address</returns>
		public virtual string GetClientNamenodeAddress()
		{
			return clientNamenodeAddress;
		}

		public static IPEndPoint GetAddress(string address)
		{
			return NetUtils.CreateSocketAddr(address, DefaultPort);
		}

		/// <summary>
		/// Set the configuration property for the service rpc address
		/// to address
		/// </summary>
		public static void SetServiceAddress(Configuration conf, string address)
		{
			Log.Info("Setting ADDRESS {}", address);
			conf.Set(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, address);
		}

		/// <summary>
		/// Fetches the address for services to use when connecting to namenode
		/// based on the value of fallback returns null if the special
		/// address is not specified or returns the default namenode address
		/// to be used by both clients and services.
		/// </summary>
		/// <remarks>
		/// Fetches the address for services to use when connecting to namenode
		/// based on the value of fallback returns null if the special
		/// address is not specified or returns the default namenode address
		/// to be used by both clients and services.
		/// Services here are datanodes, backup node, any non client connection
		/// </remarks>
		public static IPEndPoint GetServiceAddress(Configuration conf, bool fallback)
		{
			string addr = conf.GetTrimmed(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey);
			if (addr == null || addr.IsEmpty())
			{
				return fallback ? GetAddress(conf) : null;
			}
			return GetAddress(addr);
		}

		public static IPEndPoint GetAddress(Configuration conf)
		{
			URI filesystemURI = FileSystem.GetDefaultUri(conf);
			return GetAddress(filesystemURI);
		}

		/// <returns>address of file system</returns>
		public static IPEndPoint GetAddress(URI filesystemURI)
		{
			string authority = filesystemURI.GetAuthority();
			if (authority == null)
			{
				throw new ArgumentException(string.Format("Invalid URI for NameNode address (check %s): %s has no authority."
					, FileSystem.FsDefaultNameKey, filesystemURI.ToString()));
			}
			if (!Sharpen.Runtime.EqualsIgnoreCase(HdfsConstants.HdfsUriScheme, filesystemURI.
				GetScheme()))
			{
				throw new ArgumentException(string.Format("Invalid URI for NameNode address (check %s): %s is not of scheme '%s'."
					, FileSystem.FsDefaultNameKey, filesystemURI.ToString(), HdfsConstants.HdfsUriScheme
					));
			}
			return GetAddress(authority);
		}

		public static URI GetUri(IPEndPoint namenode)
		{
			int port = namenode.Port;
			string portString = port == DefaultPort ? string.Empty : (":" + port);
			return URI.Create(HdfsConstants.HdfsUriScheme + "://" + namenode.GetHostName() + 
				portString);
		}

		//
		// Common NameNode methods implementation for the active name-node role.
		//
		public virtual HdfsServerConstants.NamenodeRole GetRole()
		{
			return role;
		}

		internal virtual bool IsRole(HdfsServerConstants.NamenodeRole that)
		{
			return role.Equals(that);
		}

		/// <summary>
		/// Given a configuration get the address of the service rpc server
		/// If the service rpc is not configured returns null
		/// </summary>
		protected internal virtual IPEndPoint GetServiceRpcServerAddress(Configuration conf
			)
		{
			return Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode.GetServiceAddress(conf, false
				);
		}

		protected internal virtual IPEndPoint GetRpcServerAddress(Configuration conf)
		{
			return GetAddress(conf);
		}

		/// <summary>
		/// Given a configuration get the bind host of the service rpc server
		/// If the bind host is not configured returns null.
		/// </summary>
		protected internal virtual string GetServiceRpcServerBindHost(Configuration conf)
		{
			string addr = conf.GetTrimmed(DFSConfigKeys.DfsNamenodeServiceRpcBindHostKey);
			if (addr == null || addr.IsEmpty())
			{
				return null;
			}
			return addr;
		}

		/// <summary>
		/// Given a configuration get the bind host of the client rpc server
		/// If the bind host is not configured returns null.
		/// </summary>
		protected internal virtual string GetRpcServerBindHost(Configuration conf)
		{
			string addr = conf.GetTrimmed(DFSConfigKeys.DfsNamenodeRpcBindHostKey);
			if (addr == null || addr.IsEmpty())
			{
				return null;
			}
			return addr;
		}

		/// <summary>Modifies the configuration passed to contain the service rpc address setting
		/// 	</summary>
		protected internal virtual void SetRpcServiceServerAddress(Configuration conf, IPEndPoint
			 serviceRPCAddress)
		{
			SetServiceAddress(conf, NetUtils.GetHostPortString(serviceRPCAddress));
		}

		protected internal virtual void SetRpcServerAddress(Configuration conf, IPEndPoint
			 rpcAddress)
		{
			FileSystem.SetDefaultUri(conf, GetUri(rpcAddress));
		}

		protected internal virtual IPEndPoint GetHttpServerAddress(Configuration conf)
		{
			return GetHttpAddress(conf);
		}

		/// <summary>HTTP server address for binding the endpoint.</summary>
		/// <remarks>
		/// HTTP server address for binding the endpoint. This method is
		/// for use by the NameNode and its derivatives. It may return
		/// a different address than the one that should be used by clients to
		/// connect to the NameNode. See
		/// <see cref="Org.Apache.Hadoop.Hdfs.DFSConfigKeys.DfsNamenodeHttpBindHostKey"/>
		/// </remarks>
		/// <param name="conf"/>
		/// <returns/>
		protected internal virtual IPEndPoint GetHttpServerBindAddress(Configuration conf
			)
		{
			IPEndPoint bindAddress = GetHttpServerAddress(conf);
			// If DFS_NAMENODE_HTTP_BIND_HOST_KEY exists then it overrides the
			// host name portion of DFS_NAMENODE_HTTP_ADDRESS_KEY.
			string bindHost = conf.GetTrimmed(DFSConfigKeys.DfsNamenodeHttpBindHostKey);
			if (bindHost != null && !bindHost.IsEmpty())
			{
				bindAddress = new IPEndPoint(bindHost, bindAddress.Port);
			}
			return bindAddress;
		}

		/// <returns>the NameNode HTTP address.</returns>
		public static IPEndPoint GetHttpAddress(Configuration conf)
		{
			return NetUtils.CreateSocketAddr(conf.GetTrimmed(DFSConfigKeys.DfsNamenodeHttpAddressKey
				, DFSConfigKeys.DfsNamenodeHttpAddressDefault));
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void LoadNamesystem(Configuration conf)
		{
			this.namesystem = FSNamesystem.LoadFromDisk(conf);
		}

		internal virtual NamenodeRegistration GetRegistration()
		{
			return nodeRegistration;
		}

		internal virtual NamenodeRegistration SetRegistration()
		{
			nodeRegistration = new NamenodeRegistration(NetUtils.GetHostPortString(rpcServer.
				GetRpcAddress()), NetUtils.GetHostPortString(GetHttpAddress()), GetFSImage().GetStorage
				(), GetRole());
			return nodeRegistration;
		}

		/* optimize ugi lookup for RPC operations to avoid a trip through
		* UGI.getCurrentUser which is synch'ed
		*/
		/// <exception cref="System.IO.IOException"/>
		public static UserGroupInformation GetRemoteUser()
		{
			UserGroupInformation ugi = Org.Apache.Hadoop.Ipc.Server.GetRemoteUser();
			return (ugi != null) ? ugi : UserGroupInformation.GetCurrentUser();
		}

		/// <summary>Login as the configured user for the NameNode.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void LoginAsNameNodeUser(Configuration conf)
		{
			IPEndPoint socAddr = GetRpcServerAddress(conf);
			SecurityUtil.Login(conf, DFSConfigKeys.DfsNamenodeKeytabFileKey, DFSConfigKeys.DfsNamenodeKerberosPrincipalKey
				, socAddr.GetHostName());
		}

		/// <summary>Initialize name-node.</summary>
		/// <param name="conf">the configuration</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void Initialize(Configuration conf)
		{
			if (conf.Get(CommonConfigurationKeys.HadoopUserGroupMetricsPercentilesIntervals) 
				== null)
			{
				string intervals = conf.Get(DFSConfigKeys.DfsMetricsPercentilesIntervalsKey);
				if (intervals != null)
				{
					conf.Set(CommonConfigurationKeys.HadoopUserGroupMetricsPercentilesIntervals, intervals
						);
				}
			}
			UserGroupInformation.SetConfiguration(conf);
			LoginAsNameNodeUser(conf);
			Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode.InitMetrics(conf, this.GetRole());
			StartupProgressMetrics.Register(startupProgress);
			if (HdfsServerConstants.NamenodeRole.Namenode == role)
			{
				StartHttpServer(conf);
			}
			this.spanReceiverHost = SpanReceiverHost.Get(conf, DFSConfigKeys.DfsServerHtracePrefix
				);
			LoadNamesystem(conf);
			rpcServer = CreateRpcServer(conf);
			if (clientNamenodeAddress == null)
			{
				// This is expected for MiniDFSCluster. Set it now using 
				// the RPC server's bind address.
				clientNamenodeAddress = NetUtils.GetHostPortString(rpcServer.GetRpcAddress());
				Log.Info("Clients are to use " + clientNamenodeAddress + " to access" + " this namenode/service."
					);
			}
			if (HdfsServerConstants.NamenodeRole.Namenode == role)
			{
				httpServer.SetNameNodeAddress(GetNameNodeAddress());
				httpServer.SetFSImage(GetFSImage());
			}
			pauseMonitor = new JvmPauseMonitor(conf);
			pauseMonitor.Start();
			metrics.GetJvmMetrics().SetPauseMonitor(pauseMonitor);
			StartCommonServices(conf);
		}

		/// <summary>Create the RPC server implementation.</summary>
		/// <remarks>
		/// Create the RPC server implementation. Used as an extension point for the
		/// BackupNode.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual NameNodeRpcServer CreateRpcServer(Configuration conf)
		{
			return new NameNodeRpcServer(conf, this);
		}

		/// <summary>Start the services common to active and standby states</summary>
		/// <exception cref="System.IO.IOException"/>
		private void StartCommonServices(Configuration conf)
		{
			namesystem.StartCommonServices(conf, haContext);
			RegisterNNSMXBean();
			if (HdfsServerConstants.NamenodeRole.Namenode != role)
			{
				StartHttpServer(conf);
				httpServer.SetNameNodeAddress(GetNameNodeAddress());
				httpServer.SetFSImage(GetFSImage());
			}
			rpcServer.Start();
			plugins = conf.GetInstances<ServicePlugin>(DFSConfigKeys.DfsNamenodePluginsKey);
			foreach (ServicePlugin p in plugins)
			{
				try
				{
					p.Start(this);
				}
				catch (Exception t)
				{
					Log.Warn("ServicePlugin " + p + " could not be started", t);
				}
			}
			Log.Info(GetRole() + " RPC up at: " + rpcServer.GetRpcAddress());
			if (rpcServer.GetServiceRpcAddress() != null)
			{
				Log.Info(GetRole() + " service RPC up at: " + rpcServer.GetServiceRpcAddress());
			}
		}

		private void StopCommonServices()
		{
			if (rpcServer != null)
			{
				rpcServer.Stop();
			}
			if (namesystem != null)
			{
				namesystem.Close();
			}
			if (pauseMonitor != null)
			{
				pauseMonitor.Stop();
			}
			if (plugins != null)
			{
				foreach (ServicePlugin p in plugins)
				{
					try
					{
						p.Stop();
					}
					catch (Exception t)
					{
						Log.Warn("ServicePlugin " + p + " could not be stopped", t);
					}
				}
			}
			StopHttpServer();
		}

		/// <exception cref="System.IO.IOException"/>
		private void StartTrashEmptier(Configuration conf)
		{
			long trashInterval = conf.GetLong(CommonConfigurationKeysPublic.FsTrashIntervalKey
				, CommonConfigurationKeysPublic.FsTrashIntervalDefault);
			if (trashInterval == 0)
			{
				return;
			}
			else
			{
				if (trashInterval < 0)
				{
					throw new IOException("Cannot start trash emptier with negative interval." + " Set "
						 + CommonConfigurationKeysPublic.FsTrashIntervalKey + " to a positive value.");
				}
			}
			// This may be called from the transitionToActive code path, in which
			// case the current user is the administrator, not the NN. The trash
			// emptier needs to run as the NN. See HDFS-3972.
			FileSystem fs = SecurityUtil.DoAsLoginUser(new _PrivilegedExceptionAction_732(conf
				));
			this.emptier = new Sharpen.Thread(new Trash(fs, conf).GetEmptier(), "Trash Emptier"
				);
			this.emptier.SetDaemon(true);
			this.emptier.Start();
		}

		private sealed class _PrivilegedExceptionAction_732 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_732(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public FileSystem Run()
			{
				return FileSystem.Get(conf);
			}

			private readonly Configuration conf;
		}

		private void StopTrashEmptier()
		{
			if (this.emptier != null)
			{
				emptier.Interrupt();
				emptier = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void StartHttpServer(Configuration conf)
		{
			httpServer = new NameNodeHttpServer(conf, this, GetHttpServerBindAddress(conf));
			httpServer.Start();
			httpServer.SetStartupProgress(startupProgress);
		}

		private void StopHttpServer()
		{
			try
			{
				if (httpServer != null)
				{
					httpServer.Stop();
				}
			}
			catch (Exception e)
			{
				Log.Error("Exception while stopping httpserver", e);
			}
		}

		/// <summary>Start NameNode.</summary>
		/// <remarks>
		/// Start NameNode.
		/// <p>
		/// The name-node can be started with one of the following startup options:
		/// <ul>
		/// <li>
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.StartupOption.Regular
		/// 	">REGULAR</see>
		/// - normal name node startup</li>
		/// <li>
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.StartupOption.Format
		/// 	">FORMAT</see>
		/// - format name node</li>
		/// <li>
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.StartupOption.Backup
		/// 	">BACKUP</see>
		/// - start backup node</li>
		/// <li>
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.StartupOption.Checkpoint
		/// 	">CHECKPOINT</see>
		/// - start checkpoint node</li>
		/// <li>
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.StartupOption.Upgrade
		/// 	">UPGRADE</see>
		/// - start the cluster
		/// <li>
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.StartupOption.Upgradeonly
		/// 	">UPGRADEONLY</see>
		/// - upgrade the cluster
		/// upgrade and create a snapshot of the current file system state</li>
		/// <li>
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.StartupOption.Recover
		/// 	">RECOVERY</see>
		/// - recover name node
		/// metadata</li>
		/// <li>
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.StartupOption.Rollback
		/// 	">ROLLBACK</see>
		/// - roll the
		/// cluster back to the previous state</li>
		/// <li>
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.StartupOption.Finalize
		/// 	">FINALIZE</see>
		/// - finalize
		/// previous upgrade</li>
		/// <li>
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.StartupOption.Import
		/// 	">IMPORT</see>
		/// - import checkpoint</li>
		/// </ul>
		/// The option is passed via configuration field:
		/// <tt>dfs.namenode.startup</tt>
		/// The conf will be modified to reflect the actual ports on which
		/// the NameNode is up and running if the user passes the port as
		/// <code>zero</code> in the conf.
		/// </remarks>
		/// <param name="conf">confirguration</param>
		/// <exception cref="System.IO.IOException"/>
		public NameNode(Configuration conf)
			: this(conf, HdfsServerConstants.NamenodeRole.Namenode)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal NameNode(Configuration conf, HdfsServerConstants.NamenodeRole 
			role)
		{
			this.conf = conf;
			this.role = role;
			SetClientNamenodeAddress(conf);
			string nsId = GetNameServiceId(conf);
			string namenodeId = HAUtil.GetNameNodeId(conf, nsId);
			this.haEnabled = HAUtil.IsHAEnabled(conf, nsId);
			state = CreateHAState(GetStartupOption(conf));
			this.allowStaleStandbyReads = HAUtil.ShouldAllowStandbyReads(conf);
			this.haContext = CreateHAContext();
			try
			{
				InitializeGenericKeys(conf, nsId, namenodeId);
				Initialize(conf);
				try
				{
					haContext.WriteLock();
					state.PrepareToEnterState(haContext);
					state.EnterState(haContext);
				}
				finally
				{
					haContext.WriteUnlock();
				}
			}
			catch (IOException e)
			{
				this.Stop();
				throw;
			}
			catch (HadoopIllegalArgumentException e)
			{
				this.Stop();
				throw;
			}
			this.started.Set(true);
		}

		protected internal virtual HAState CreateHAState(HdfsServerConstants.StartupOption
			 startOpt)
		{
			if (!haEnabled || startOpt == HdfsServerConstants.StartupOption.Upgrade || startOpt
				 == HdfsServerConstants.StartupOption.Upgradeonly)
			{
				return ActiveState;
			}
			else
			{
				return StandbyState;
			}
		}

		protected internal virtual HAContext CreateHAContext()
		{
			return new NameNode.NameNodeHAContext(this);
		}

		/// <summary>Wait for service to finish.</summary>
		/// <remarks>
		/// Wait for service to finish.
		/// (Normally, it runs forever.)
		/// </remarks>
		public virtual void Join()
		{
			try
			{
				rpcServer.Join();
			}
			catch (Exception ie)
			{
				Log.Info("Caught interrupted exception ", ie);
			}
		}

		/// <summary>Stop all NameNode threads and wait for all to finish.</summary>
		public virtual void Stop()
		{
			lock (this)
			{
				if (stopRequested)
				{
					return;
				}
				stopRequested = true;
			}
			try
			{
				if (state != null)
				{
					state.ExitState(haContext);
				}
			}
			catch (ServiceFailedException e)
			{
				Log.Warn("Encountered exception while exiting state ", e);
			}
			finally
			{
				StopCommonServices();
				if (metrics != null)
				{
					metrics.Shutdown();
				}
				if (namesystem != null)
				{
					namesystem.Shutdown();
				}
				if (nameNodeStatusBeanName != null)
				{
					MBeans.Unregister(nameNodeStatusBeanName);
					nameNodeStatusBeanName = null;
				}
				if (this.spanReceiverHost != null)
				{
					this.spanReceiverHost.CloseReceivers();
				}
			}
		}

		internal virtual bool IsStopRequested()
		{
			lock (this)
			{
				return stopRequested;
			}
		}

		/// <summary>Is the cluster currently in safe mode?</summary>
		public virtual bool IsInSafeMode()
		{
			return namesystem.IsInSafeMode();
		}

		/// <summary>get FSImage</summary>
		[VisibleForTesting]
		public virtual FSImage GetFSImage()
		{
			return namesystem.GetFSImage();
		}

		/// <returns>NameNode RPC address</returns>
		public virtual IPEndPoint GetNameNodeAddress()
		{
			return rpcServer.GetRpcAddress();
		}

		/// <returns>NameNode RPC address in "host:port" string form</returns>
		public virtual string GetNameNodeAddressHostPortString()
		{
			return NetUtils.GetHostPortString(rpcServer.GetRpcAddress());
		}

		/// <returns>
		/// NameNode service RPC address if configured, the
		/// NameNode RPC address otherwise
		/// </returns>
		public virtual IPEndPoint GetServiceRpcAddress()
		{
			IPEndPoint serviceAddr = rpcServer.GetServiceRpcAddress();
			return serviceAddr == null ? rpcServer.GetRpcAddress() : serviceAddr;
		}

		/// <returns>
		/// NameNode HTTP address, used by the Web UI, image transfer,
		/// and HTTP-based file system clients like Hftp and WebHDFS
		/// </returns>
		public virtual IPEndPoint GetHttpAddress()
		{
			return httpServer.GetHttpAddress();
		}

		/// <returns>
		/// NameNode HTTPS address, used by the Web UI, image transfer,
		/// and HTTP-based file system clients like Hftp and WebHDFS
		/// </returns>
		public virtual IPEndPoint GetHttpsAddress()
		{
			return httpServer.GetHttpsAddress();
		}

		/// <summary>
		/// Verify that configured directories exist, then
		/// Interactively confirm that formatting is desired
		/// for each existing directory and format them.
		/// </summary>
		/// <param name="conf">configuration to use</param>
		/// <param name="force">if true, format regardless of whether dirs exist</param>
		/// <returns>true if formatting was aborted, false otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		private static bool Format(Configuration conf, bool force, bool isInteractive)
		{
			string nsId = DFSUtil.GetNamenodeNameServiceId(conf);
			string namenodeId = HAUtil.GetNameNodeId(conf, nsId);
			InitializeGenericKeys(conf, nsId, namenodeId);
			CheckAllowFormat(conf);
			if (UserGroupInformation.IsSecurityEnabled())
			{
				IPEndPoint socAddr = GetAddress(conf);
				SecurityUtil.Login(conf, DFSConfigKeys.DfsNamenodeKeytabFileKey, DFSConfigKeys.DfsNamenodeKerberosPrincipalKey
					, socAddr.GetHostName());
			}
			ICollection<URI> nameDirsToFormat = FSNamesystem.GetNamespaceDirs(conf);
			IList<URI> sharedDirs = FSNamesystem.GetSharedEditsDirs(conf);
			IList<URI> dirsToPrompt = new AList<URI>();
			Sharpen.Collections.AddAll(dirsToPrompt, nameDirsToFormat);
			Sharpen.Collections.AddAll(dirsToPrompt, sharedDirs);
			IList<URI> editDirsToFormat = FSNamesystem.GetNamespaceEditsDirs(conf);
			// if clusterID is not provided - see if you can find the current one
			string clusterId = HdfsServerConstants.StartupOption.Format.GetClusterId();
			if (clusterId == null || clusterId.Equals(string.Empty))
			{
				//Generate a new cluster id
				clusterId = NNStorage.NewClusterID();
			}
			System.Console.Out.WriteLine("Formatting using clusterid: " + clusterId);
			FSImage fsImage = new FSImage(conf, nameDirsToFormat, editDirsToFormat);
			try
			{
				FSNamesystem fsn = new FSNamesystem(conf, fsImage);
				fsImage.GetEditLog().InitJournalsForWrite();
				if (!fsImage.ConfirmFormat(force, isInteractive))
				{
					return true;
				}
				// aborted
				fsImage.Format(fsn, clusterId);
			}
			catch (IOException ioe)
			{
				Log.Warn("Encountered exception during format: ", ioe);
				fsImage.Close();
				throw;
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CheckAllowFormat(Configuration conf)
		{
			if (!conf.GetBoolean(DFSConfigKeys.DfsNamenodeSupportAllowFormatKey, DFSConfigKeys
				.DfsNamenodeSupportAllowFormatDefault))
			{
				throw new IOException("The option " + DFSConfigKeys.DfsNamenodeSupportAllowFormatKey
					 + " is set to false for this filesystem, so it " + "cannot be formatted. You will need to set "
					 + DFSConfigKeys.DfsNamenodeSupportAllowFormatKey + " parameter " + "to true in order to format this filesystem"
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public static bool InitializeSharedEdits(Configuration conf)
		{
			return InitializeSharedEdits(conf, true);
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public static bool InitializeSharedEdits(Configuration conf, bool force)
		{
			return InitializeSharedEdits(conf, force, false);
		}

		/// <summary>Clone the supplied configuration but remove the shared edits dirs.</summary>
		/// <param name="conf">Supplies the original configuration.</param>
		/// <returns>Cloned configuration without the shared edit dirs.</returns>
		/// <exception cref="System.IO.IOException">on failure to generate the configuration.
		/// 	</exception>
		private static Configuration GetConfigurationWithoutSharedEdits(Configuration conf
			)
		{
			IList<URI> editsDirs = FSNamesystem.GetNamespaceEditsDirs(conf, false);
			string editsDirsString = Joiner.On(",").Join(editsDirs);
			Configuration confWithoutShared = new Configuration(conf);
			confWithoutShared.Unset(DFSConfigKeys.DfsNamenodeSharedEditsDirKey);
			confWithoutShared.SetStrings(DFSConfigKeys.DfsNamenodeEditsDirKey, editsDirsString
				);
			return confWithoutShared;
		}

		/// <summary>
		/// Format a new shared edits dir and copy in enough edit log segments so that
		/// the standby NN can start up.
		/// </summary>
		/// <param name="conf">configuration</param>
		/// <param name="force">format regardless of whether or not the shared edits dir exists
		/// 	</param>
		/// <param name="interactive">prompt the user when a dir exists</param>
		/// <returns>true if the command aborts, false otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		private static bool InitializeSharedEdits(Configuration conf, bool force, bool interactive
			)
		{
			string nsId = DFSUtil.GetNamenodeNameServiceId(conf);
			string namenodeId = HAUtil.GetNameNodeId(conf, nsId);
			InitializeGenericKeys(conf, nsId, namenodeId);
			if (conf.Get(DFSConfigKeys.DfsNamenodeSharedEditsDirKey) == null)
			{
				Log.Error("No shared edits directory configured for namespace " + nsId + " namenode "
					 + namenodeId);
				return false;
			}
			if (UserGroupInformation.IsSecurityEnabled())
			{
				IPEndPoint socAddr = GetAddress(conf);
				SecurityUtil.Login(conf, DFSConfigKeys.DfsNamenodeKeytabFileKey, DFSConfigKeys.DfsNamenodeKerberosPrincipalKey
					, socAddr.GetHostName());
			}
			NNStorage existingStorage = null;
			FSImage sharedEditsImage = null;
			try
			{
				FSNamesystem fsns = FSNamesystem.LoadFromDisk(GetConfigurationWithoutSharedEdits(
					conf));
				existingStorage = fsns.GetFSImage().GetStorage();
				NamespaceInfo nsInfo = existingStorage.GetNamespaceInfo();
				IList<URI> sharedEditsDirs = FSNamesystem.GetSharedEditsDirs(conf);
				sharedEditsImage = new FSImage(conf, Lists.NewArrayList<URI>(), sharedEditsDirs);
				sharedEditsImage.GetEditLog().InitJournalsForWrite();
				if (!sharedEditsImage.ConfirmFormat(force, interactive))
				{
					return true;
				}
				// abort
				NNStorage newSharedStorage = sharedEditsImage.GetStorage();
				// Call Storage.format instead of FSImage.format here, since we don't
				// actually want to save a checkpoint - just prime the dirs with
				// the existing namespace info
				newSharedStorage.Format(nsInfo);
				sharedEditsImage.GetEditLog().FormatNonFileJournals(nsInfo);
				// Need to make sure the edit log segments are in good shape to initialize
				// the shared edits dir.
				fsns.GetFSImage().GetEditLog().Close();
				fsns.GetFSImage().GetEditLog().InitJournalsForWrite();
				fsns.GetFSImage().GetEditLog().RecoverUnclosedStreams();
				CopyEditLogSegmentsToSharedDir(fsns, sharedEditsDirs, newSharedStorage, conf);
			}
			catch (IOException ioe)
			{
				Log.Error("Could not initialize shared edits dir", ioe);
				return true;
			}
			finally
			{
				// aborted
				if (sharedEditsImage != null)
				{
					try
					{
						sharedEditsImage.Close();
					}
					catch (IOException ioe)
					{
						Log.Warn("Could not close sharedEditsImage", ioe);
					}
				}
				// Have to unlock storage explicitly for the case when we're running in a
				// unit test, which runs in the same JVM as NNs.
				if (existingStorage != null)
				{
					try
					{
						existingStorage.UnlockAll();
					}
					catch (IOException ioe)
					{
						Log.Warn("Could not unlock storage directories", ioe);
						return true;
					}
				}
			}
			// aborted
			return false;
		}

		// did not abort
		/// <exception cref="System.IO.IOException"/>
		private static void CopyEditLogSegmentsToSharedDir(FSNamesystem fsns, ICollection
			<URI> sharedEditsDirs, NNStorage newSharedStorage, Configuration conf)
		{
			Preconditions.CheckArgument(!sharedEditsDirs.IsEmpty(), "No shared edits specified"
				);
			// Copy edit log segments into the new shared edits dir.
			IList<URI> sharedEditsUris = new AList<URI>(sharedEditsDirs);
			FSEditLog newSharedEditLog = new FSEditLog(conf, newSharedStorage, sharedEditsUris
				);
			newSharedEditLog.InitJournalsForWrite();
			newSharedEditLog.RecoverUnclosedStreams();
			FSEditLog sourceEditLog = fsns.GetFSImage().editLog;
			long fromTxId = fsns.GetFSImage().GetMostRecentCheckpointTxId();
			ICollection<EditLogInputStream> streams = null;
			try
			{
				streams = sourceEditLog.SelectInputStreams(fromTxId + 1, 0);
				// Set the nextTxid to the CheckpointTxId+1
				newSharedEditLog.SetNextTxId(fromTxId + 1);
				// Copy all edits after last CheckpointTxId to shared edits dir
				foreach (EditLogInputStream stream in streams)
				{
					Log.Debug("Beginning to copy stream " + stream + " to shared edits");
					FSEditLogOp op;
					bool segmentOpen = false;
					while ((op = stream.ReadOp()) != null)
					{
						if (Log.IsTraceEnabled())
						{
							Log.Trace("copying op: " + op);
						}
						if (!segmentOpen)
						{
							newSharedEditLog.StartLogSegment(op.txid, false);
							segmentOpen = true;
						}
						newSharedEditLog.LogEdit(op);
						if (op.opCode == FSEditLogOpCodes.OpEndLogSegment)
						{
							newSharedEditLog.LogSync();
							newSharedEditLog.EndCurrentLogSegment(false);
							Log.Debug("ending log segment because of END_LOG_SEGMENT op in " + stream);
							segmentOpen = false;
						}
					}
					if (segmentOpen)
					{
						Log.Debug("ending log segment because of end of stream in " + stream);
						newSharedEditLog.LogSync();
						newSharedEditLog.EndCurrentLogSegment(false);
						segmentOpen = false;
					}
				}
			}
			finally
			{
				if (streams != null)
				{
					FSEditLog.CloseAllStreams(streams);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public static bool DoRollback(Configuration conf, bool isConfirmationNeeded)
		{
			string nsId = DFSUtil.GetNamenodeNameServiceId(conf);
			string namenodeId = HAUtil.GetNameNodeId(conf, nsId);
			InitializeGenericKeys(conf, nsId, namenodeId);
			FSNamesystem nsys = new FSNamesystem(conf, new FSImage(conf));
			System.Console.Error.Write("\"rollBack\" will remove the current state of the file system,\n"
				 + "returning you to the state prior to initiating your recent.\n" + "upgrade. This action is permanent and cannot be undone. If you\n"
				 + "are performing a rollback in an HA environment, you should be\n" + "certain that no NameNode process is running on any host."
				);
			if (isConfirmationNeeded)
			{
				if (!ToolRunner.ConfirmPrompt("Roll back file system state?"))
				{
					System.Console.Error.WriteLine("Rollback aborted.");
					return true;
				}
			}
			nsys.GetFSImage().DoRollback(nsys);
			return false;
		}

		private static void PrintUsage(TextWriter @out)
		{
			@out.WriteLine(Usage + "\n");
		}

		[VisibleForTesting]
		internal static HdfsServerConstants.StartupOption ParseArguments(string[] args)
		{
			int argsLen = (args == null) ? 0 : args.Length;
			HdfsServerConstants.StartupOption startOpt = HdfsServerConstants.StartupOption.Regular;
			for (int i = 0; i < argsLen; i++)
			{
				string cmd = args[i];
				if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Format.GetName
					(), cmd))
				{
					startOpt = HdfsServerConstants.StartupOption.Format;
					for (i = i + 1; i < argsLen; i++)
					{
						if (Sharpen.Runtime.EqualsIgnoreCase(args[i], HdfsServerConstants.StartupOption.Clusterid
							.GetName()))
						{
							i++;
							if (i >= argsLen)
							{
								// if no cluster id specified, return null
								Log.Error("Must specify a valid cluster ID after the " + HdfsServerConstants.StartupOption
									.Clusterid.GetName() + " flag");
								return null;
							}
							string clusterId = args[i];
							// Make sure an id is specified and not another flag
							if (clusterId.IsEmpty() || Sharpen.Runtime.EqualsIgnoreCase(clusterId, HdfsServerConstants.StartupOption
								.Force.GetName()) || Sharpen.Runtime.EqualsIgnoreCase(clusterId, HdfsServerConstants.StartupOption
								.Noninteractive.GetName()))
							{
								Log.Error("Must specify a valid cluster ID after the " + HdfsServerConstants.StartupOption
									.Clusterid.GetName() + " flag");
								return null;
							}
							startOpt.SetClusterId(clusterId);
						}
						if (Sharpen.Runtime.EqualsIgnoreCase(args[i], HdfsServerConstants.StartupOption.Force
							.GetName()))
						{
							startOpt.SetForceFormat(true);
						}
						if (Sharpen.Runtime.EqualsIgnoreCase(args[i], HdfsServerConstants.StartupOption.Noninteractive
							.GetName()))
						{
							startOpt.SetInteractiveFormat(false);
						}
					}
				}
				else
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Genclusterid
						.GetName(), cmd))
					{
						startOpt = HdfsServerConstants.StartupOption.Genclusterid;
					}
					else
					{
						if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Regular.GetName
							(), cmd))
						{
							startOpt = HdfsServerConstants.StartupOption.Regular;
						}
						else
						{
							if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Backup.GetName
								(), cmd))
							{
								startOpt = HdfsServerConstants.StartupOption.Backup;
							}
							else
							{
								if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Checkpoint
									.GetName(), cmd))
								{
									startOpt = HdfsServerConstants.StartupOption.Checkpoint;
								}
								else
								{
									if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Upgrade.GetName
										(), cmd) || Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Upgradeonly
										.GetName(), cmd))
									{
										startOpt = Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Upgrade
											.GetName(), cmd) ? HdfsServerConstants.StartupOption.Upgrade : HdfsServerConstants.StartupOption
											.Upgradeonly;
										/* Can be followed by CLUSTERID with a required parameter or
										* RENAMERESERVED with an optional parameter
										*/
										while (i + 1 < argsLen)
										{
											string flag = args[i + 1];
											if (Sharpen.Runtime.EqualsIgnoreCase(flag, HdfsServerConstants.StartupOption.Clusterid
												.GetName()))
											{
												if (i + 2 < argsLen)
												{
													i += 2;
													startOpt.SetClusterId(args[i]);
												}
												else
												{
													Log.Error("Must specify a valid cluster ID after the " + HdfsServerConstants.StartupOption
														.Clusterid.GetName() + " flag");
													return null;
												}
											}
											else
											{
												if (Sharpen.Runtime.EqualsIgnoreCase(flag, HdfsServerConstants.StartupOption.Renamereserved
													.GetName()))
												{
													if (i + 2 < argsLen)
													{
														FSImageFormat.SetRenameReservedPairs(args[i + 2]);
														i += 2;
													}
													else
													{
														FSImageFormat.UseDefaultRenameReservedPairs();
														i += 1;
													}
												}
												else
												{
													Log.Error("Unknown upgrade flag " + flag);
													return null;
												}
											}
										}
									}
									else
									{
										if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Rollingupgrade
											.GetName(), cmd))
										{
											startOpt = HdfsServerConstants.StartupOption.Rollingupgrade;
											++i;
											if (i >= argsLen)
											{
												Log.Error("Must specify a rolling upgrade startup option " + HdfsServerConstants.RollingUpgradeStartupOption
													.GetAllOptionString());
												return null;
											}
											startOpt.SetRollingUpgradeStartupOption(args[i]);
										}
										else
										{
											if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Rollback.GetName
												(), cmd))
											{
												startOpt = HdfsServerConstants.StartupOption.Rollback;
											}
											else
											{
												if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Finalize.GetName
													(), cmd))
												{
													startOpt = HdfsServerConstants.StartupOption.Finalize;
												}
												else
												{
													if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Import.GetName
														(), cmd))
													{
														startOpt = HdfsServerConstants.StartupOption.Import;
													}
													else
													{
														if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Bootstrapstandby
															.GetName(), cmd))
														{
															startOpt = HdfsServerConstants.StartupOption.Bootstrapstandby;
															return startOpt;
														}
														else
														{
															if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Initializesharededits
																.GetName(), cmd))
															{
																startOpt = HdfsServerConstants.StartupOption.Initializesharededits;
																for (i = i + 1; i < argsLen; i++)
																{
																	if (HdfsServerConstants.StartupOption.Noninteractive.GetName().Equals(args[i]))
																	{
																		startOpt.SetInteractiveFormat(false);
																	}
																	else
																	{
																		if (HdfsServerConstants.StartupOption.Force.GetName().Equals(args[i]))
																		{
																			startOpt.SetForceFormat(true);
																		}
																		else
																		{
																			Log.Error("Invalid argument: " + args[i]);
																			return null;
																		}
																	}
																}
																return startOpt;
															}
															else
															{
																if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Recover.GetName
																	(), cmd))
																{
																	if (startOpt != HdfsServerConstants.StartupOption.Regular)
																	{
																		throw new RuntimeException("Can't combine -recover with " + "other startup options."
																			);
																	}
																	startOpt = HdfsServerConstants.StartupOption.Recover;
																	while (++i < argsLen)
																	{
																		if (Sharpen.Runtime.EqualsIgnoreCase(args[i], HdfsServerConstants.StartupOption.Force
																			.GetName()))
																		{
																			startOpt.SetForce(MetaRecoveryContext.ForceFirstChoice);
																		}
																		else
																		{
																			throw new RuntimeException("Error parsing recovery options: " + "can't understand option \""
																				 + args[i] + "\"");
																		}
																	}
																}
																else
																{
																	if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Metadataversion
																		.GetName(), cmd))
																	{
																		startOpt = HdfsServerConstants.StartupOption.Metadataversion;
																	}
																	else
																	{
																		return null;
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			return startOpt;
		}

		private static void SetStartupOption(Configuration conf, HdfsServerConstants.StartupOption
			 opt)
		{
			conf.Set(DFSConfigKeys.DfsNamenodeStartupKey, opt.ToString());
		}

		internal static HdfsServerConstants.StartupOption GetStartupOption(Configuration 
			conf)
		{
			return HdfsServerConstants.StartupOption.ValueOf(conf.Get(DFSConfigKeys.DfsNamenodeStartupKey
				, HdfsServerConstants.StartupOption.Regular.ToString()));
		}

		/// <exception cref="System.IO.IOException"/>
		private static void DoRecovery(HdfsServerConstants.StartupOption startOpt, Configuration
			 conf)
		{
			string nsId = DFSUtil.GetNamenodeNameServiceId(conf);
			string namenodeId = HAUtil.GetNameNodeId(conf, nsId);
			InitializeGenericKeys(conf, nsId, namenodeId);
			if (startOpt.GetForce() < MetaRecoveryContext.ForceAll)
			{
				if (!ToolRunner.ConfirmPrompt("You have selected Metadata Recovery mode.  " + "This mode is intended to recover lost metadata on a corrupt "
					 + "filesystem.  Metadata recovery mode often permanently deletes " + "data from your HDFS filesystem.  Please back up your edit log "
					 + "and fsimage before trying this!\n\n" + "Are you ready to proceed? (Y/N)\n"))
				{
					System.Console.Error.WriteLine("Recovery aborted at user request.\n");
					return;
				}
			}
			MetaRecoveryContext.Log.Info("starting recovery...");
			UserGroupInformation.SetConfiguration(conf);
			Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode.InitMetrics(conf, startOpt.ToNodeRole
				());
			FSNamesystem fsn = null;
			try
			{
				fsn = FSNamesystem.LoadFromDisk(conf);
				fsn.GetFSImage().SaveNamespace(fsn);
				MetaRecoveryContext.Log.Info("RECOVERY COMPLETE");
			}
			catch (IOException e)
			{
				MetaRecoveryContext.Log.Info("RECOVERY FAILED: caught exception", e);
				throw;
			}
			catch (RuntimeException e)
			{
				MetaRecoveryContext.Log.Info("RECOVERY FAILED: caught exception", e);
				throw;
			}
			finally
			{
				if (fsn != null)
				{
					fsn.Close();
				}
			}
		}

		/// <summary>
		/// Verify that configured directories exist, then print the metadata versions
		/// of the software and the image.
		/// </summary>
		/// <param name="conf">configuration to use</param>
		/// <exception cref="System.IO.IOException"/>
		private static bool PrintMetadataVersion(Configuration conf)
		{
			string nsId = DFSUtil.GetNamenodeNameServiceId(conf);
			string namenodeId = HAUtil.GetNameNodeId(conf, nsId);
			Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode.InitializeGenericKeys(conf, nsId, 
				namenodeId);
			FSImage fsImage = new FSImage(conf);
			FSNamesystem fs = new FSNamesystem(conf, fsImage, false);
			return fsImage.RecoverTransitionRead(HdfsServerConstants.StartupOption.Metadataversion
				, fs, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode CreateNameNode(string
			[] argv, Configuration conf)
		{
			Log.Info("createNameNode " + Arrays.AsList(argv));
			if (conf == null)
			{
				conf = new HdfsConfiguration();
			}
			HdfsServerConstants.StartupOption startOpt = ParseArguments(argv);
			if (startOpt == null)
			{
				PrintUsage(System.Console.Error);
				return null;
			}
			SetStartupOption(conf, startOpt);
			switch (startOpt)
			{
				case HdfsServerConstants.StartupOption.Format:
				{
					bool aborted = Format(conf, startOpt.GetForceFormat(), startOpt.GetInteractiveFormat
						());
					ExitUtil.Terminate(aborted ? 1 : 0);
					return null;
				}

				case HdfsServerConstants.StartupOption.Genclusterid:
				{
					// avoid javac warning
					System.Console.Error.WriteLine("Generating new cluster id:");
					System.Console.Out.WriteLine(NNStorage.NewClusterID());
					ExitUtil.Terminate(0);
					return null;
				}

				case HdfsServerConstants.StartupOption.Finalize:
				{
					System.Console.Error.WriteLine("Use of the argument '" + HdfsServerConstants.StartupOption
						.Finalize + "' is no longer supported. To finalize an upgrade, start the NN " + 
						" and then run `hdfs dfsadmin -finalizeUpgrade'");
					ExitUtil.Terminate(1);
					return null;
				}

				case HdfsServerConstants.StartupOption.Rollback:
				{
					// avoid javac warning
					bool aborted = DoRollback(conf, true);
					ExitUtil.Terminate(aborted ? 1 : 0);
					return null;
				}

				case HdfsServerConstants.StartupOption.Bootstrapstandby:
				{
					// avoid warning
					string[] toolArgs = Arrays.CopyOfRange(argv, 1, argv.Length);
					int rc = BootstrapStandby.Run(toolArgs, conf);
					ExitUtil.Terminate(rc);
					return null;
				}

				case HdfsServerConstants.StartupOption.Initializesharededits:
				{
					// avoid warning
					bool aborted = InitializeSharedEdits(conf, startOpt.GetForceFormat(), startOpt.GetInteractiveFormat
						());
					ExitUtil.Terminate(aborted ? 1 : 0);
					return null;
				}

				case HdfsServerConstants.StartupOption.Backup:
				case HdfsServerConstants.StartupOption.Checkpoint:
				{
					// avoid warning
					HdfsServerConstants.NamenodeRole role = startOpt.ToNodeRole();
					DefaultMetricsSystem.Initialize(role.ToString().Replace(" ", string.Empty));
					return new BackupNode(conf, role);
				}

				case HdfsServerConstants.StartupOption.Recover:
				{
					Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode.DoRecovery(startOpt, conf);
					return null;
				}

				case HdfsServerConstants.StartupOption.Metadataversion:
				{
					PrintMetadataVersion(conf);
					ExitUtil.Terminate(0);
					return null;
				}

				case HdfsServerConstants.StartupOption.Upgradeonly:
				{
					// avoid javac warning
					DefaultMetricsSystem.Initialize("NameNode");
					new Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode(conf);
					ExitUtil.Terminate(0);
					return null;
				}

				default:
				{
					DefaultMetricsSystem.Initialize("NameNode");
					return new Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode(conf);
				}
			}
		}

		/// <summary>
		/// In federation configuration is set for a set of
		/// namenode and secondary namenode/backup/checkpointer, which are
		/// grouped under a logical nameservice ID.
		/// </summary>
		/// <remarks>
		/// In federation configuration is set for a set of
		/// namenode and secondary namenode/backup/checkpointer, which are
		/// grouped under a logical nameservice ID. The configuration keys specific
		/// to them have suffix set to configured nameserviceId.
		/// This method copies the value from specific key of format key.nameserviceId
		/// to key, to set up the generic configuration. Once this is done, only
		/// generic version of the configuration is read in rest of the code, for
		/// backward compatibility and simpler code changes.
		/// </remarks>
		/// <param name="conf">
		/// Configuration object to lookup specific key and to set the value
		/// to the key passed. Note the conf object is modified
		/// </param>
		/// <param name="nameserviceId">name service Id (to distinguish federated NNs)</param>
		/// <param name="namenodeId">the namenode ID (to distinguish HA NNs)</param>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.DFSUtil.SetGenericConf(Org.Apache.Hadoop.Conf.Configuration, string, string, string[])
		/// 	"/>
		public static void InitializeGenericKeys(Configuration conf, string nameserviceId
			, string namenodeId)
		{
			if ((nameserviceId != null && !nameserviceId.IsEmpty()) || (namenodeId != null &&
				 !namenodeId.IsEmpty()))
			{
				if (nameserviceId != null)
				{
					conf.Set(DFSConfigKeys.DfsNameserviceId, nameserviceId);
				}
				if (namenodeId != null)
				{
					conf.Set(DFSConfigKeys.DfsHaNamenodeIdKey, namenodeId);
				}
				DFSUtil.SetGenericConf(conf, nameserviceId, namenodeId, NamenodeSpecificKeys);
				DFSUtil.SetGenericConf(conf, nameserviceId, null, NameserviceSpecificKeys);
			}
			// If the RPC address is set use it to (re-)configure the default FS
			if (conf.Get(DFSConfigKeys.DfsNamenodeRpcAddressKey) != null)
			{
				URI defaultUri = URI.Create(HdfsConstants.HdfsUriScheme + "://" + conf.Get(DFSConfigKeys
					.DfsNamenodeRpcAddressKey));
				conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, defaultUri.ToString());
				Log.Debug("Setting " + CommonConfigurationKeysPublic.FsDefaultNameKey + " to " + 
					defaultUri.ToString());
			}
		}

		/// <summary>Get the name service Id for the node</summary>
		/// <returns>name service Id or null if federation is not configured</returns>
		protected internal virtual string GetNameServiceId(Configuration conf)
		{
			return DFSUtil.GetNamenodeNameServiceId(conf);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			if (DFSUtil.ParseHelpArgument(argv, Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode
				.Usage, System.Console.Out, true))
			{
				System.Environment.Exit(0);
			}
			try
			{
				StringUtils.StartupShutdownMessage(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode
					), argv, Log);
				Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode namenode = CreateNameNode(argv, null
					);
				if (namenode != null)
				{
					namenode.Join();
				}
			}
			catch (Exception e)
			{
				Log.Error("Failed to start namenode.", e);
				ExitUtil.Terminate(1, e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.HealthCheckFailedException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual void MonitorHealth()
		{
			lock (this)
			{
				namesystem.CheckSuperuserPrivilege();
				if (!haEnabled)
				{
					return;
				}
				// no-op, if HA is not enabled
				GetNamesystem().CheckAvailableResources();
				if (!GetNamesystem().NameNodeHasResourcesAvailable())
				{
					throw new HealthCheckFailedException("The NameNode has no resources available");
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual void TransitionToActive()
		{
			lock (this)
			{
				namesystem.CheckSuperuserPrivilege();
				if (!haEnabled)
				{
					throw new ServiceFailedException("HA for namenode is not enabled");
				}
				state.SetState(haContext, ActiveState);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual void TransitionToStandby()
		{
			lock (this)
			{
				namesystem.CheckSuperuserPrivilege();
				if (!haEnabled)
				{
					throw new ServiceFailedException("HA for namenode is not enabled");
				}
				state.SetState(haContext, StandbyState);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual HAServiceStatus GetServiceStatus()
		{
			lock (this)
			{
				namesystem.CheckSuperuserPrivilege();
				if (!haEnabled)
				{
					throw new ServiceFailedException("HA for namenode is not enabled");
				}
				if (state == null)
				{
					return new HAServiceStatus(HAServiceProtocol.HAServiceState.Initializing);
				}
				HAServiceProtocol.HAServiceState retState = state.GetServiceState();
				HAServiceStatus ret = new HAServiceStatus(retState);
				if (retState == HAServiceProtocol.HAServiceState.Standby)
				{
					string safemodeTip = namesystem.GetSafeModeTip();
					if (!safemodeTip.IsEmpty())
					{
						ret.SetNotReadyToBecomeActive("The NameNode is in safemode. " + safemodeTip);
					}
					else
					{
						ret.SetReadyToBecomeActive();
					}
				}
				else
				{
					if (retState == HAServiceProtocol.HAServiceState.Active)
					{
						ret.SetReadyToBecomeActive();
					}
					else
					{
						ret.SetNotReadyToBecomeActive("State is " + state);
					}
				}
				return ret;
			}
		}

		internal virtual HAServiceProtocol.HAServiceState GetServiceState()
		{
			lock (this)
			{
				if (state == null)
				{
					return HAServiceProtocol.HAServiceState.Initializing;
				}
				return state.GetServiceState();
			}
		}

		/// <summary>Register NameNodeStatusMXBean</summary>
		private void RegisterNNSMXBean()
		{
			nameNodeStatusBeanName = MBeans.Register("NameNode", "NameNodeStatus", this);
		}

		public virtual string GetNNRole()
		{
			// NameNodeStatusMXBean
			string roleStr = string.Empty;
			HdfsServerConstants.NamenodeRole role = GetRole();
			if (null != role)
			{
				roleStr = role.ToString();
			}
			return roleStr;
		}

		public virtual string GetState()
		{
			// NameNodeStatusMXBean
			string servStateStr = string.Empty;
			HAServiceProtocol.HAServiceState servState = GetServiceState();
			if (null != servState)
			{
				servStateStr = servState.ToString();
			}
			return servStateStr;
		}

		public virtual string GetHostAndPort()
		{
			// NameNodeStatusMXBean
			return GetNameNodeAddressHostPortString();
		}

		public virtual bool IsSecurityEnabled()
		{
			// NameNodeStatusMXBean
			return UserGroupInformation.IsSecurityEnabled();
		}

		public virtual long GetLastHATransitionTime()
		{
			// NameNodeStatusMXBean
			return state.GetLastHATransitionTime();
		}

		/// <summary>Shutdown the NN immediately in an ungraceful way.</summary>
		/// <remarks>
		/// Shutdown the NN immediately in an ungraceful way. Used when it would be
		/// unsafe for the NN to continue operating, e.g. during a failed HA state
		/// transition.
		/// </remarks>
		/// <param name="t">
		/// exception which warrants the shutdown. Printed to the NN log
		/// before exit.
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Util.ExitUtil.ExitException">thrown only for testing.
		/// 	</exception>
		protected internal virtual void DoImmediateShutdown(Exception t)
		{
			lock (this)
			{
				string message = "Error encountered requiring NN shutdown. " + "Shutting down immediately.";
				try
				{
					Log.Error(message, t);
				}
				catch
				{
				}
				// This is unlikely to happen, but there's nothing we can do if it does.
				ExitUtil.Terminate(1, t);
			}
		}

		/// <summary>
		/// Class used to expose
		/// <see cref="NameNode"/>
		/// as context to
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.HAState"/>
		/// </summary>
		protected internal class NameNodeHAContext : HAContext
		{
			public virtual void SetState(HAState s)
			{
				this._enclosing.state = s;
			}

			public virtual HAState GetState()
			{
				return this._enclosing.state;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void StartActiveServices()
			{
				try
				{
					this._enclosing.namesystem.StartActiveServices();
					this._enclosing.StartTrashEmptier(this._enclosing.conf);
				}
				catch (Exception t)
				{
					this._enclosing.DoImmediateShutdown(t);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void StopActiveServices()
			{
				try
				{
					if (this._enclosing.namesystem != null)
					{
						this._enclosing.namesystem.StopActiveServices();
					}
					this._enclosing.StopTrashEmptier();
				}
				catch (Exception t)
				{
					this._enclosing.DoImmediateShutdown(t);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void StartStandbyServices()
			{
				try
				{
					this._enclosing.namesystem.StartStandbyServices(this._enclosing.conf);
				}
				catch (Exception t)
				{
					this._enclosing.DoImmediateShutdown(t);
				}
			}

			/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
			public virtual void PrepareToStopStandbyServices()
			{
				try
				{
					this._enclosing.namesystem.PrepareToStopStandbyServices();
				}
				catch (Exception t)
				{
					this._enclosing.DoImmediateShutdown(t);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void StopStandbyServices()
			{
				try
				{
					if (this._enclosing.namesystem != null)
					{
						this._enclosing.namesystem.StopStandbyServices();
					}
				}
				catch (Exception t)
				{
					this._enclosing.DoImmediateShutdown(t);
				}
			}

			public virtual void WriteLock()
			{
				this._enclosing.namesystem.WriteLock();
				this._enclosing.namesystem.LockRetryCache();
			}

			public virtual void WriteUnlock()
			{
				this._enclosing.namesystem.UnlockRetryCache();
				this._enclosing.namesystem.WriteUnlock();
			}

			/// <summary>Check if an operation of given category is allowed</summary>
			/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
			public virtual void CheckOperation(NameNode.OperationCategory op)
			{
				this._enclosing.state.CheckOperation(this._enclosing.haContext, op);
			}

			public virtual bool AllowStaleReads()
			{
				return this._enclosing.allowStaleStandbyReads;
			}

			internal NameNodeHAContext(NameNode _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly NameNode _enclosing;
		}

		public virtual bool IsStandbyState()
		{
			return (state.Equals(StandbyState));
		}

		public virtual bool IsActiveState()
		{
			return (state.Equals(ActiveState));
		}

		/// <summary>Returns whether the NameNode is completely started</summary>
		internal virtual bool IsStarted()
		{
			return this.started.Get();
		}

		/// <summary>Check that a request to change this node's HA state is valid.</summary>
		/// <remarks>
		/// Check that a request to change this node's HA state is valid.
		/// In particular, verifies that, if auto failover is enabled, non-forced
		/// requests from the HAAdmin CLI are rejected, and vice versa.
		/// </remarks>
		/// <param name="req">the request to check</param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if the request is disallowed
		/// 	</exception>
		internal virtual void CheckHaStateChange(HAServiceProtocol.StateChangeRequestInfo
			 req)
		{
			bool autoHaEnabled = conf.GetBoolean(DFSConfigKeys.DfsHaAutoFailoverEnabledKey, DFSConfigKeys
				.DfsHaAutoFailoverEnabledDefault);
			switch (req.GetSource())
			{
				case HAServiceProtocol.RequestSource.RequestByUser:
				{
					if (autoHaEnabled)
					{
						throw new AccessControlException("Manual HA control for this NameNode is disallowed, because "
							 + "automatic HA is enabled.");
					}
					break;
				}

				case HAServiceProtocol.RequestSource.RequestByUserForced:
				{
					if (autoHaEnabled)
					{
						Log.Warn("Allowing manual HA control from " + Org.Apache.Hadoop.Ipc.Server.GetRemoteAddress
							() + " even though automatic HA is enabled, because the user " + "specified the force flag"
							);
					}
					break;
				}

				case HAServiceProtocol.RequestSource.RequestByZkfc:
				{
					if (!autoHaEnabled)
					{
						throw new AccessControlException("Request from ZK failover controller at " + Org.Apache.Hadoop.Ipc.Server
							.GetRemoteAddress() + " denied since automatic HA " + "is not enabled");
					}
					break;
				}
			}
		}
	}
}
