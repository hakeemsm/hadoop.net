using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.HA.Proto;
using Org.Apache.Hadoop.HA.ProtocolPB;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Inotify;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Web.Resources;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Ipc.Proto;
using Org.Apache.Hadoop.Ipc.ProtocolPB;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Security.ProtocolPB;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Tools.Proto;
using Org.Apache.Hadoop.Tools.ProtocolPB;
using Org.Apache.Hadoop.Tracing;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This class is responsible for handling all of the RPC calls to the NameNode.
	/// 	</summary>
	/// <remarks>
	/// This class is responsible for handling all of the RPC calls to the NameNode.
	/// It is created, started, and stopped by
	/// <see cref="NameNode"/>
	/// .
	/// </remarks>
	internal class NameNodeRpcServer : NamenodeProtocols
	{
		private static readonly Logger Log = NameNode.Log;

		private static readonly Logger stateChangeLog = NameNode.stateChangeLog;

		private static readonly Logger blockStateChangeLog = NameNode.blockStateChangeLog;

		protected internal readonly FSNamesystem namesystem;

		protected internal readonly NameNode nn;

		private readonly NameNodeMetrics metrics;

		private readonly RetryCache retryCache;

		private readonly bool serviceAuthEnabled;

		/// <summary>The RPC server that listens to requests from DataNodes</summary>
		private readonly RPC.Server serviceRpcServer;

		private readonly IPEndPoint serviceRPCAddress;

		/// <summary>The RPC server that listens to requests from clients</summary>
		protected internal readonly RPC.Server clientRpcServer;

		protected internal readonly IPEndPoint clientRpcAddress;

		private readonly string minimumDataNodeVersion;

		/// <exception cref="System.IO.IOException"/>
		public NameNodeRpcServer(Configuration conf, NameNode nn)
		{
			// Dependencies from other parts of NN.
			this.nn = nn;
			this.namesystem = nn.GetNamesystem();
			this.retryCache = namesystem.GetRetryCache();
			this.metrics = NameNode.GetNameNodeMetrics();
			int handlerCount = conf.GetInt(DFSConfigKeys.DfsNamenodeHandlerCountKey, DFSConfigKeys
				.DfsNamenodeHandlerCountDefault);
			RPC.SetProtocolEngine(conf, typeof(ClientNamenodeProtocolPB), typeof(ProtobufRpcEngine
				));
			ClientNamenodeProtocolServerSideTranslatorPB clientProtocolServerTranslator = new 
				ClientNamenodeProtocolServerSideTranslatorPB(this);
			BlockingService clientNNPbService = ClientNamenodeProtocolProtos.ClientNamenodeProtocol
				.NewReflectiveBlockingService(clientProtocolServerTranslator);
			DatanodeProtocolServerSideTranslatorPB dnProtoPbTranslator = new DatanodeProtocolServerSideTranslatorPB
				(this);
			BlockingService dnProtoPbService = DatanodeProtocolProtos.DatanodeProtocolService
				.NewReflectiveBlockingService(dnProtoPbTranslator);
			NamenodeProtocolServerSideTranslatorPB namenodeProtocolXlator = new NamenodeProtocolServerSideTranslatorPB
				(this);
			BlockingService NNPbService = NamenodeProtocolProtos.NamenodeProtocolService.NewReflectiveBlockingService
				(namenodeProtocolXlator);
			RefreshAuthorizationPolicyProtocolServerSideTranslatorPB refreshAuthPolicyXlator = 
				new RefreshAuthorizationPolicyProtocolServerSideTranslatorPB(this);
			BlockingService refreshAuthService = RefreshAuthorizationPolicyProtocolProtos.RefreshAuthorizationPolicyProtocolService
				.NewReflectiveBlockingService(refreshAuthPolicyXlator);
			RefreshUserMappingsProtocolServerSideTranslatorPB refreshUserMappingXlator = new 
				RefreshUserMappingsProtocolServerSideTranslatorPB(this);
			BlockingService refreshUserMappingService = RefreshUserMappingsProtocolProtos.RefreshUserMappingsProtocolService
				.NewReflectiveBlockingService(refreshUserMappingXlator);
			RefreshCallQueueProtocolServerSideTranslatorPB refreshCallQueueXlator = new RefreshCallQueueProtocolServerSideTranslatorPB
				(this);
			BlockingService refreshCallQueueService = RefreshCallQueueProtocolProtos.RefreshCallQueueProtocolService
				.NewReflectiveBlockingService(refreshCallQueueXlator);
			GenericRefreshProtocolServerSideTranslatorPB genericRefreshXlator = new GenericRefreshProtocolServerSideTranslatorPB
				(this);
			BlockingService genericRefreshService = GenericRefreshProtocolProtos.GenericRefreshProtocolService
				.NewReflectiveBlockingService(genericRefreshXlator);
			GetUserMappingsProtocolServerSideTranslatorPB getUserMappingXlator = new GetUserMappingsProtocolServerSideTranslatorPB
				(this);
			BlockingService getUserMappingService = GetUserMappingsProtocolProtos.GetUserMappingsProtocolService
				.NewReflectiveBlockingService(getUserMappingXlator);
			HAServiceProtocolServerSideTranslatorPB haServiceProtocolXlator = new HAServiceProtocolServerSideTranslatorPB
				(this);
			BlockingService haPbService = HAServiceProtocolProtos.HAServiceProtocolService.NewReflectiveBlockingService
				(haServiceProtocolXlator);
			TraceAdminProtocolServerSideTranslatorPB traceAdminXlator = new TraceAdminProtocolServerSideTranslatorPB
				(this);
			BlockingService traceAdminService = TraceAdminPB.TraceAdminService.NewReflectiveBlockingService
				(traceAdminXlator);
			WritableRpcEngine.EnsureInitialized();
			IPEndPoint serviceRpcAddr = nn.GetServiceRpcServerAddress(conf);
			if (serviceRpcAddr != null)
			{
				string bindHost = nn.GetServiceRpcServerBindHost(conf);
				if (bindHost == null)
				{
					bindHost = serviceRpcAddr.GetHostName();
				}
				Log.Info("Service RPC server is binding to " + bindHost + ":" + serviceRpcAddr.Port
					);
				int serviceHandlerCount = conf.GetInt(DFSConfigKeys.DfsNamenodeServiceHandlerCountKey
					, DFSConfigKeys.DfsNamenodeServiceHandlerCountDefault);
				this.serviceRpcServer = new RPC.Builder(conf).SetProtocol(typeof(ClientNamenodeProtocolPB
					)).SetInstance(clientNNPbService).SetBindAddress(bindHost).SetPort(serviceRpcAddr
					.Port).SetNumHandlers(serviceHandlerCount).SetVerbose(false).SetSecretManager(namesystem
					.GetDelegationTokenSecretManager()).Build();
				// Add all the RPC protocols that the namenode implements
				DFSUtil.AddPBProtocol(conf, typeof(HAServiceProtocolPB), haPbService, serviceRpcServer
					);
				DFSUtil.AddPBProtocol(conf, typeof(NamenodeProtocolPB), NNPbService, serviceRpcServer
					);
				DFSUtil.AddPBProtocol(conf, typeof(DatanodeProtocolPB), dnProtoPbService, serviceRpcServer
					);
				DFSUtil.AddPBProtocol(conf, typeof(RefreshAuthorizationPolicyProtocolPB), refreshAuthService
					, serviceRpcServer);
				DFSUtil.AddPBProtocol(conf, typeof(RefreshUserMappingsProtocolPB), refreshUserMappingService
					, serviceRpcServer);
				// We support Refreshing call queue here in case the client RPC queue is full
				DFSUtil.AddPBProtocol(conf, typeof(RefreshCallQueueProtocolPB), refreshCallQueueService
					, serviceRpcServer);
				DFSUtil.AddPBProtocol(conf, typeof(GenericRefreshProtocolPB), genericRefreshService
					, serviceRpcServer);
				DFSUtil.AddPBProtocol(conf, typeof(GetUserMappingsProtocolPB), getUserMappingService
					, serviceRpcServer);
				DFSUtil.AddPBProtocol(conf, typeof(TraceAdminProtocolPB), traceAdminService, serviceRpcServer
					);
				// Update the address with the correct port
				IPEndPoint listenAddr = serviceRpcServer.GetListenerAddress();
				serviceRPCAddress = new IPEndPoint(serviceRpcAddr.GetHostName(), listenAddr.Port);
				nn.SetRpcServiceServerAddress(conf, serviceRPCAddress);
			}
			else
			{
				serviceRpcServer = null;
				serviceRPCAddress = null;
			}
			IPEndPoint rpcAddr = nn.GetRpcServerAddress(conf);
			string bindHost_1 = nn.GetRpcServerBindHost(conf);
			if (bindHost_1 == null)
			{
				bindHost_1 = rpcAddr.GetHostName();
			}
			Log.Info("RPC server is binding to " + bindHost_1 + ":" + rpcAddr.Port);
			this.clientRpcServer = new RPC.Builder(conf).SetProtocol(typeof(ClientNamenodeProtocolPB
				)).SetInstance(clientNNPbService).SetBindAddress(bindHost_1).SetPort(rpcAddr.Port
				).SetNumHandlers(handlerCount).SetVerbose(false).SetSecretManager(namesystem.GetDelegationTokenSecretManager
				()).Build();
			// Add all the RPC protocols that the namenode implements
			DFSUtil.AddPBProtocol(conf, typeof(HAServiceProtocolPB), haPbService, clientRpcServer
				);
			DFSUtil.AddPBProtocol(conf, typeof(NamenodeProtocolPB), NNPbService, clientRpcServer
				);
			DFSUtil.AddPBProtocol(conf, typeof(DatanodeProtocolPB), dnProtoPbService, clientRpcServer
				);
			DFSUtil.AddPBProtocol(conf, typeof(RefreshAuthorizationPolicyProtocolPB), refreshAuthService
				, clientRpcServer);
			DFSUtil.AddPBProtocol(conf, typeof(RefreshUserMappingsProtocolPB), refreshUserMappingService
				, clientRpcServer);
			DFSUtil.AddPBProtocol(conf, typeof(RefreshCallQueueProtocolPB), refreshCallQueueService
				, clientRpcServer);
			DFSUtil.AddPBProtocol(conf, typeof(GenericRefreshProtocolPB), genericRefreshService
				, clientRpcServer);
			DFSUtil.AddPBProtocol(conf, typeof(GetUserMappingsProtocolPB), getUserMappingService
				, clientRpcServer);
			DFSUtil.AddPBProtocol(conf, typeof(TraceAdminProtocolPB), traceAdminService, clientRpcServer
				);
			// set service-level authorization security policy
			if (serviceAuthEnabled = conf.GetBoolean(CommonConfigurationKeys.HadoopSecurityAuthorization
				, false))
			{
				clientRpcServer.RefreshServiceAcl(conf, new HDFSPolicyProvider());
				if (serviceRpcServer != null)
				{
					serviceRpcServer.RefreshServiceAcl(conf, new HDFSPolicyProvider());
				}
			}
			// The rpc-server port can be ephemeral... ensure we have the correct info
			IPEndPoint listenAddr_1 = clientRpcServer.GetListenerAddress();
			clientRpcAddress = new IPEndPoint(rpcAddr.GetHostName(), listenAddr_1.Port);
			nn.SetRpcServerAddress(conf, clientRpcAddress);
			minimumDataNodeVersion = conf.Get(DFSConfigKeys.DfsNamenodeMinSupportedDatanodeVersionKey
				, DFSConfigKeys.DfsNamenodeMinSupportedDatanodeVersionDefault);
			// Set terse exception whose stack trace won't be logged
			this.clientRpcServer.AddTerseExceptions(typeof(SafeModeException), typeof(FileNotFoundException
				), typeof(HadoopIllegalArgumentException), typeof(FileAlreadyExistsException), typeof(
				InvalidPathException), typeof(ParentNotDirectoryException), typeof(UnresolvedLinkException
				), typeof(AlreadyBeingCreatedException), typeof(QuotaExceededException), typeof(
				RecoveryInProgressException), typeof(AccessControlException), typeof(SecretManager.InvalidToken
				), typeof(LeaseExpiredException), typeof(NSQuotaExceededException), typeof(DSQuotaExceededException
				), typeof(AclException), typeof(FSLimitException.PathComponentTooLongException), 
				typeof(FSLimitException.MaxDirectoryItemsExceededException), typeof(UnresolvedPathException
				));
		}

		/// <summary>Allow access to the client RPC server for testing</summary>
		[VisibleForTesting]
		internal virtual RPC.Server GetClientRpcServer()
		{
			return clientRpcServer;
		}

		/// <summary>Allow access to the service RPC server for testing</summary>
		[VisibleForTesting]
		internal virtual RPC.Server GetServiceRpcServer()
		{
			return serviceRpcServer;
		}

		/// <summary>Start client and service RPC servers.</summary>
		internal virtual void Start()
		{
			clientRpcServer.Start();
			if (serviceRpcServer != null)
			{
				serviceRpcServer.Start();
			}
		}

		/// <summary>Wait until the RPC servers have shutdown.</summary>
		/// <exception cref="System.Exception"/>
		internal virtual void Join()
		{
			clientRpcServer.Join();
			if (serviceRpcServer != null)
			{
				serviceRpcServer.Join();
			}
		}

		/// <summary>Stop client and service RPC servers.</summary>
		internal virtual void Stop()
		{
			if (clientRpcServer != null)
			{
				clientRpcServer.Stop();
			}
			if (serviceRpcServer != null)
			{
				serviceRpcServer.Stop();
			}
		}

		internal virtual IPEndPoint GetServiceRpcAddress()
		{
			return serviceRPCAddress;
		}

		internal virtual IPEndPoint GetRpcAddress()
		{
			return clientRpcAddress;
		}

		/// <exception cref="System.IO.IOException"/>
		private static UserGroupInformation GetRemoteUser()
		{
			return NameNode.GetRemoteUser();
		}

		/////////////////////////////////////////////////////
		// NamenodeProtocol
		/////////////////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual BlocksWithLocations GetBlocks(DatanodeInfo datanode, long size)
		{
			// NamenodeProtocol
			if (size <= 0)
			{
				throw new ArgumentException("Unexpected not positive size: " + size);
			}
			CheckNNStartup();
			namesystem.CheckSuperuserPrivilege();
			return namesystem.GetBlockManager().GetBlocks(datanode, size);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ExportedBlockKeys GetBlockKeys()
		{
			// NamenodeProtocol
			CheckNNStartup();
			namesystem.CheckSuperuserPrivilege();
			return namesystem.GetBlockManager().GetBlockKeys();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ErrorReport(NamenodeRegistration registration, int errorCode, 
			string msg)
		{
			// NamenodeProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Unchecked);
			namesystem.CheckSuperuserPrivilege();
			VerifyRequest(registration);
			Log.Info("Error report from " + registration + ": " + msg);
			if (errorCode == Fatal)
			{
				namesystem.ReleaseBackupNode(registration);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual NamenodeRegistration RegisterSubordinateNamenode(NamenodeRegistration
			 registration)
		{
			// NamenodeProtocol
			CheckNNStartup();
			namesystem.CheckSuperuserPrivilege();
			VerifyLayoutVersion(registration.GetVersion());
			NamenodeRegistration myRegistration = nn.SetRegistration();
			namesystem.RegisterBackupNode(registration, myRegistration);
			return myRegistration;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual NamenodeCommand StartCheckpoint(NamenodeRegistration registration)
		{
			// NamenodeProtocol
			CheckNNStartup();
			namesystem.CheckSuperuserPrivilege();
			VerifyRequest(registration);
			if (!nn.IsRole(HdfsServerConstants.NamenodeRole.Namenode))
			{
				throw new IOException("Only an ACTIVE node can invoke startCheckpoint.");
			}
			RetryCache.CacheEntryWithPayload cacheEntry = RetryCache.WaitForCompletion(retryCache
				, null);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return (NamenodeCommand)cacheEntry.GetPayload();
			}
			NamenodeCommand ret = null;
			try
			{
				ret = namesystem.StartCheckpoint(registration, nn.SetRegistration());
			}
			finally
			{
				RetryCache.SetState(cacheEntry, ret != null, ret);
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndCheckpoint(NamenodeRegistration registration, CheckpointSignature
			 sig)
		{
			// NamenodeProtocol
			CheckNNStartup();
			namesystem.CheckSuperuserPrivilege();
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			// Return previous response
			bool success = false;
			try
			{
				namesystem.EndCheckpoint(registration, sig);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> 
			GetDelegationToken(Text renewer)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.GetDelegationToken(renewer);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual long RenewDelegationToken(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
			> token)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.RenewDelegationToken(token);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CancelDelegationToken(Org.Apache.Hadoop.Security.Token.Token<
			DelegationTokenIdentifier> token)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CancelDelegationToken(token);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual LocatedBlocks GetBlockLocations(string src, long offset, long length
			)
		{
			// ClientProtocol
			CheckNNStartup();
			metrics.IncrGetBlockLocations();
			return namesystem.GetBlockLocations(GetClientMachine(), src, offset, length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual FsServerDefaults GetServerDefaults()
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.GetServerDefaults();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsFileStatus Create(string src, FsPermission masked, string clientName
			, EnumSetWritable<CreateFlag> flag, bool createParent, short replication, long blockSize
			, CryptoProtocolVersion[] supportedVersions)
		{
			// ClientProtocol
			CheckNNStartup();
			string clientMachine = GetClientMachine();
			if (stateChangeLog.IsDebugEnabled())
			{
				stateChangeLog.Debug("*DIR* NameNode.create: file " + src + " for " + clientName 
					+ " at " + clientMachine);
			}
			if (!CheckPathLength(src))
			{
				throw new IOException("create: Pathname too long.  Limit " + HdfsConstants.MaxPathLength
					 + " characters, " + HdfsConstants.MaxPathDepth + " levels.");
			}
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntryWithPayload cacheEntry = RetryCache.WaitForCompletion(retryCache
				, null);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return (HdfsFileStatus)cacheEntry.GetPayload();
			}
			HdfsFileStatus status = null;
			try
			{
				PermissionStatus perm = new PermissionStatus(GetRemoteUser().GetShortUserName(), 
					null, masked);
				status = namesystem.StartFile(src, perm, clientName, clientMachine, flag.Get(), createParent
					, replication, blockSize, supportedVersions, cacheEntry != null);
			}
			finally
			{
				RetryCache.SetState(cacheEntry, status != null, status);
			}
			metrics.IncrFilesCreated();
			metrics.IncrCreateFileOps();
			return status;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual LastBlockWithStatus Append(string src, string clientName, EnumSetWritable
			<CreateFlag> flag)
		{
			// ClientProtocol
			CheckNNStartup();
			string clientMachine = GetClientMachine();
			if (stateChangeLog.IsDebugEnabled())
			{
				stateChangeLog.Debug("*DIR* NameNode.append: file " + src + " for " + clientName 
					+ " at " + clientMachine);
			}
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntryWithPayload cacheEntry = RetryCache.WaitForCompletion(retryCache
				, null);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return (LastBlockWithStatus)cacheEntry.GetPayload();
			}
			LastBlockWithStatus info = null;
			bool success = false;
			try
			{
				info = namesystem.AppendFile(src, clientName, clientMachine, flag.Get(), cacheEntry
					 != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success, info);
			}
			metrics.IncrFilesAppended();
			return info;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool RecoverLease(string src, string clientName)
		{
			// ClientProtocol
			CheckNNStartup();
			string clientMachine = GetClientMachine();
			return namesystem.RecoverLease(src, clientName, clientMachine);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool SetReplication(string src, short replication)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.SetReplication(src, replication);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetStoragePolicy(string src, string policyName)
		{
			CheckNNStartup();
			namesystem.SetStoragePolicy(src, policyName);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual BlockStoragePolicy[] GetStoragePolicies()
		{
			CheckNNStartup();
			return namesystem.GetStoragePolicies();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetPermission(string src, FsPermission permissions)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.SetPermission(src, permissions);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetOwner(string src, string username, string groupname)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.SetOwner(src, username, groupname);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual LocatedBlock AddBlock(string src, string clientName, ExtendedBlock
			 previous, DatanodeInfo[] excludedNodes, long fileId, string[] favoredNodes)
		{
			CheckNNStartup();
			if (stateChangeLog.IsDebugEnabled())
			{
				stateChangeLog.Debug("*BLOCK* NameNode.addBlock: file " + src + " fileId=" + fileId
					 + " for " + clientName);
			}
			ICollection<Node> excludedNodesSet = null;
			if (excludedNodes != null)
			{
				excludedNodesSet = new HashSet<Node>(excludedNodes.Length);
				foreach (Node node in excludedNodes)
				{
					excludedNodesSet.AddItem(node);
				}
			}
			IList<string> favoredNodesList = (favoredNodes == null) ? null : Arrays.AsList(favoredNodes
				);
			LocatedBlock locatedBlock = namesystem.GetAdditionalBlock(src, fileId, clientName
				, previous, excludedNodesSet, favoredNodesList);
			if (locatedBlock != null)
			{
				metrics.IncrAddBlockOps();
			}
			return locatedBlock;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual LocatedBlock GetAdditionalDatanode(string src, long fileId, ExtendedBlock
			 blk, DatanodeInfo[] existings, string[] existingStorageIDs, DatanodeInfo[] excludes
			, int numAdditionalNodes, string clientName)
		{
			// ClientProtocol
			CheckNNStartup();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("getAdditionalDatanode: src=" + src + ", fileId=" + fileId + ", blk=" +
					 blk + ", existings=" + Arrays.AsList(existings) + ", excludes=" + Arrays.AsList
					(excludes) + ", numAdditionalNodes=" + numAdditionalNodes + ", clientName=" + clientName
					);
			}
			metrics.IncrGetAdditionalDatanodeOps();
			ICollection<Node> excludeSet = null;
			if (excludes != null)
			{
				excludeSet = new HashSet<Node>(excludes.Length);
				foreach (Node node in excludes)
				{
					excludeSet.AddItem(node);
				}
			}
			return namesystem.GetAdditionalDatanode(src, fileId, blk, existings, existingStorageIDs
				, excludeSet, numAdditionalNodes, clientName);
		}

		/// <summary>The client needs to give up on the block.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AbandonBlock(ExtendedBlock b, long fileId, string src, string
			 holder)
		{
			// ClientProtocol
			CheckNNStartup();
			if (stateChangeLog.IsDebugEnabled())
			{
				stateChangeLog.Debug("*BLOCK* NameNode.abandonBlock: " + b + " of file " + src);
			}
			if (!namesystem.AbandonBlock(b, fileId, src, holder))
			{
				throw new IOException("Cannot abandon block during write to " + src);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool Complete(string src, string clientName, ExtendedBlock last, long
			 fileId)
		{
			// ClientProtocol
			CheckNNStartup();
			if (stateChangeLog.IsDebugEnabled())
			{
				stateChangeLog.Debug("*DIR* NameNode.complete: " + src + " fileId=" + fileId + " for "
					 + clientName);
			}
			return namesystem.CompleteFile(src, clientName, last, fileId);
		}

		/// <summary>
		/// The client has detected an error on the specified located blocks
		/// and is reporting them to the server.
		/// </summary>
		/// <remarks>
		/// The client has detected an error on the specified located blocks
		/// and is reporting them to the server.  For now, the namenode will
		/// mark the block as corrupt.  In the future we might
		/// check the blocks are actually corrupt.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReportBadBlocks(LocatedBlock[] blocks)
		{
			// ClientProtocol, DatanodeProtocol
			CheckNNStartup();
			namesystem.ReportBadBlocks(blocks);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual LocatedBlock UpdateBlockForPipeline(ExtendedBlock block, string clientName
			)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.UpdateBlockForPipeline(block, clientName);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void UpdatePipeline(string clientName, ExtendedBlock oldBlock, ExtendedBlock
			 newBlock, DatanodeID[] newNodes, string[] newStorageIDs)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			// Return previous response
			bool success = false;
			try
			{
				namesystem.UpdatePipeline(clientName, oldBlock, newBlock, newNodes, newStorageIDs
					, cacheEntry != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CommitBlockSynchronization(ExtendedBlock block, long newgenerationstamp
			, long newlength, bool closeFile, bool deleteblock, DatanodeID[] newtargets, string
			[] newtargetstorages)
		{
			// DatanodeProtocol
			CheckNNStartup();
			namesystem.CommitBlockSynchronization(block, newgenerationstamp, newlength, closeFile
				, deleteblock, newtargets, newtargetstorages);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPreferredBlockSize(string filename)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.GetPreferredBlockSize(filename);
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public virtual bool Rename(string src, string dst)
		{
			// ClientProtocol
			CheckNNStartup();
			if (stateChangeLog.IsDebugEnabled())
			{
				stateChangeLog.Debug("*DIR* NameNode.rename: " + src + " to " + dst);
			}
			if (!CheckPathLength(dst))
			{
				throw new IOException("rename: Pathname too long.  Limit " + HdfsConstants.MaxPathLength
					 + " characters, " + HdfsConstants.MaxPathDepth + " levels.");
			}
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return true;
			}
			// Return previous response
			bool ret = false;
			try
			{
				ret = namesystem.RenameTo(src, dst, cacheEntry != null);
			}
			finally
			{
				RetryCache.SetState(cacheEntry, ret);
			}
			if (ret)
			{
				metrics.IncrFilesRenamed();
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Concat(string trg, string[] src)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			// Return previous response
			bool success = false;
			try
			{
				namesystem.Concat(trg, src, cacheEntry != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Rename2(string src, string dst, params Options.Rename[] options
			)
		{
			// ClientProtocol
			CheckNNStartup();
			if (stateChangeLog.IsDebugEnabled())
			{
				stateChangeLog.Debug("*DIR* NameNode.rename: " + src + " to " + dst);
			}
			if (!CheckPathLength(dst))
			{
				throw new IOException("rename: Pathname too long.  Limit " + HdfsConstants.MaxPathLength
					 + " characters, " + HdfsConstants.MaxPathDepth + " levels.");
			}
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			// Return previous response
			bool success = false;
			try
			{
				namesystem.RenameTo(src, dst, cacheEntry != null, options);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
			metrics.IncrFilesRenamed();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool Truncate(string src, long newLength, string clientName)
		{
			// ClientProtocol
			if (stateChangeLog.IsDebugEnabled())
			{
				stateChangeLog.Debug("*DIR* NameNode.truncate: " + src + " to " + newLength);
			}
			string clientMachine = GetClientMachine();
			try
			{
				return namesystem.Truncate(src, newLength, clientName, clientMachine, Time.Now());
			}
			finally
			{
				metrics.IncrFilesTruncated();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool Delete(string src, bool recursive)
		{
			// ClientProtocol
			CheckNNStartup();
			if (stateChangeLog.IsDebugEnabled())
			{
				stateChangeLog.Debug("*DIR* Namenode.delete: src=" + src + ", recursive=" + recursive
					);
			}
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return true;
			}
			// Return previous response
			bool ret = false;
			try
			{
				ret = namesystem.Delete(src, recursive, cacheEntry != null);
			}
			finally
			{
				RetryCache.SetState(cacheEntry, ret);
			}
			if (ret)
			{
				metrics.IncrDeleteFileOps();
			}
			return ret;
		}

		/// <summary>Check path length does not exceed maximum.</summary>
		/// <remarks>
		/// Check path length does not exceed maximum.  Returns true if
		/// length and depth are okay.  Returns false if length is too long
		/// or depth is too great.
		/// </remarks>
		private bool CheckPathLength(string src)
		{
			Path srcPath = new Path(src);
			return (src.Length <= HdfsConstants.MaxPathLength && srcPath.Depth() <= HdfsConstants
				.MaxPathDepth);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool Mkdirs(string src, FsPermission masked, bool createParent)
		{
			// ClientProtocol
			CheckNNStartup();
			if (stateChangeLog.IsDebugEnabled())
			{
				stateChangeLog.Debug("*DIR* NameNode.mkdirs: " + src);
			}
			if (!CheckPathLength(src))
			{
				throw new IOException("mkdirs: Pathname too long.  Limit " + HdfsConstants.MaxPathLength
					 + " characters, " + HdfsConstants.MaxPathDepth + " levels.");
			}
			return namesystem.Mkdirs(src, new PermissionStatus(GetRemoteUser().GetShortUserName
				(), null, masked), createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RenewLease(string clientName)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.RenewLease(clientName);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DirectoryListing GetListing(string src, byte[] startAfter, bool needLocation
			)
		{
			// ClientProtocol
			CheckNNStartup();
			DirectoryListing files = namesystem.GetListing(src, startAfter, needLocation);
			if (files != null)
			{
				metrics.IncrGetListingOps();
				metrics.IncrFilesInGetListingOps(files.GetPartialListing().Length);
			}
			return files;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsFileStatus GetFileInfo(string src)
		{
			// ClientProtocol
			CheckNNStartup();
			metrics.IncrFileInfoOps();
			return namesystem.GetFileInfo(src, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsFileClosed(string src)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.IsFileClosed(src);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsFileStatus GetFileLinkInfo(string src)
		{
			// ClientProtocol
			CheckNNStartup();
			metrics.IncrFileInfoOps();
			return namesystem.GetFileInfo(src, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long[] GetStats()
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Read);
			return namesystem.GetStats();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeInfo[] GetDatanodeReport(HdfsConstants.DatanodeReportType 
			type)
		{
			// ClientProtocol
			CheckNNStartup();
			DatanodeInfo[] results = namesystem.DatanodeReport(type);
			return results;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeStorageReport[] GetDatanodeStorageReport(HdfsConstants.DatanodeReportType
			 type)
		{
			// ClientProtocol
			CheckNNStartup();
			DatanodeStorageReport[] reports = namesystem.GetDatanodeStorageReport(type);
			return reports;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool SetSafeMode(HdfsConstants.SafeModeAction action, bool isChecked
			)
		{
			// ClientProtocol
			CheckNNStartup();
			NameNode.OperationCategory opCategory = NameNode.OperationCategory.Unchecked;
			if (isChecked)
			{
				if (action == HdfsConstants.SafeModeAction.SafemodeGet)
				{
					opCategory = NameNode.OperationCategory.Read;
				}
				else
				{
					opCategory = NameNode.OperationCategory.Write;
				}
			}
			namesystem.CheckOperation(opCategory);
			return namesystem.SetSafeMode(action);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool RestoreFailedStorage(string arg)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.RestoreFailedStorage(arg);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SaveNamespace()
		{
			// ClientProtocol
			CheckNNStartup();
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			// Return previous response
			bool success = false;
			try
			{
				namesystem.SaveNamespace();
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual long RollEdits()
		{
			// ClientProtocol
			CheckNNStartup();
			CheckpointSignature sig = namesystem.RollEditLog();
			return sig.GetCurSegmentTxId();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshNodes()
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.RefreshNodes();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetTransactionID()
		{
			// NamenodeProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Unchecked);
			namesystem.CheckSuperuserPrivilege();
			return namesystem.GetFSImage().GetLastAppliedOrWrittenTxId();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetMostRecentCheckpointTxId()
		{
			// NamenodeProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Unchecked);
			namesystem.CheckSuperuserPrivilege();
			return namesystem.GetFSImage().GetMostRecentCheckpointTxId();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual CheckpointSignature RollEditLog()
		{
			// NamenodeProtocol
			CheckNNStartup();
			namesystem.CheckSuperuserPrivilege();
			return namesystem.RollEditLog();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteEditLogManifest GetEditLogManifest(long sinceTxId)
		{
			// NamenodeProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Read);
			namesystem.CheckSuperuserPrivilege();
			return namesystem.GetEditLog().GetEditLogManifest(sinceTxId);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsUpgradeFinalized()
		{
			// NamenodeProtocol
			CheckNNStartup();
			namesystem.CheckSuperuserPrivilege();
			return namesystem.IsUpgradeFinalized();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void FinalizeUpgrade()
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.FinalizeUpgrade();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RollingUpgradeInfo RollingUpgrade(HdfsConstants.RollingUpgradeAction
			 action)
		{
			// ClientProtocol
			CheckNNStartup();
			Log.Info("rollingUpgrade " + action);
			switch (action)
			{
				case HdfsConstants.RollingUpgradeAction.Query:
				{
					return namesystem.QueryRollingUpgrade();
				}

				case HdfsConstants.RollingUpgradeAction.Prepare:
				{
					return namesystem.StartRollingUpgrade();
				}

				case HdfsConstants.RollingUpgradeAction.Finalize:
				{
					return namesystem.FinalizeRollingUpgrade();
				}

				default:
				{
					throw new UnsupportedActionException(action + " is not yet supported.");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void MetaSave(string filename)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.MetaSave(filename);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual CorruptFileBlocks ListCorruptFileBlocks(string path, string cookie
			)
		{
			// ClientProtocol
			CheckNNStartup();
			string[] cookieTab = new string[] { cookie };
			ICollection<FSNamesystem.CorruptFileBlockInfo> fbs = namesystem.ListCorruptFileBlocks
				(path, cookieTab);
			string[] files = new string[fbs.Count];
			int i = 0;
			foreach (FSNamesystem.CorruptFileBlockInfo fb in fbs)
			{
				files[i++] = fb.path;
			}
			return new CorruptFileBlocks(files, cookieTab[0]);
		}

		/// <summary>
		/// Tell all datanodes to use a new, non-persistent bandwidth value for
		/// dfs.datanode.balance.bandwidthPerSec.
		/// </summary>
		/// <param name="bandwidth">Balancer bandwidth in bytes per second for all datanodes.
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetBalancerBandwidth(long bandwidth)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.SetBalancerBandwidth(bandwidth);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ContentSummary GetContentSummary(string path)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.GetContentSummary(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetQuota(string path, long namespaceQuota, long storagespaceQuota
			, StorageType type)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.SetQuota(path, namespaceQuota, storagespaceQuota, type);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Fsync(string src, long fileId, string clientName, long lastBlockLength
			)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.Fsync(src, fileId, clientName, lastBlockLength);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetTimes(string src, long mtime, long atime)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.SetTimes(src, mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CreateSymlink(string target, string link, FsPermission dirPerms
			, bool createParent)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			// Return previous response
			/* We enforce the MAX_PATH_LENGTH limit even though a symlink target
			* URI may refer to a non-HDFS file system.
			*/
			if (!CheckPathLength(link))
			{
				throw new IOException("Symlink path exceeds " + HdfsConstants.MaxPathLength + " character limit"
					);
			}
			UserGroupInformation ugi = GetRemoteUser();
			bool success = false;
			try
			{
				PermissionStatus perm = new PermissionStatus(ugi.GetShortUserName(), null, dirPerms
					);
				namesystem.CreateSymlink(target, link, perm, createParent, cacheEntry != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string GetLinkTarget(string path)
		{
			// ClientProtocol
			CheckNNStartup();
			metrics.IncrGetLinkTargetOps();
			HdfsFileStatus stat = null;
			try
			{
				stat = namesystem.GetFileInfo(path, false);
			}
			catch (UnresolvedPathException e)
			{
				return e.GetResolvedPath().ToString();
			}
			catch (UnresolvedLinkException)
			{
				// The NameNode should only throw an UnresolvedPathException
				throw new Exception("UnresolvedLinkException thrown");
			}
			if (stat == null)
			{
				throw new FileNotFoundException("File does not exist: " + path);
			}
			else
			{
				if (!stat.IsSymlink())
				{
					throw new IOException("Path " + path + " is not a symbolic link");
				}
			}
			return stat.GetSymlink();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeRegistration RegisterDatanode(DatanodeRegistration nodeReg
			)
		{
			// DatanodeProtocol
			CheckNNStartup();
			VerifySoftwareVersion(nodeReg);
			namesystem.RegisterDatanode(nodeReg);
			return nodeReg;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual HeartbeatResponse SendHeartbeat(DatanodeRegistration nodeReg, StorageReport
			[] report, long dnCacheCapacity, long dnCacheUsed, int xmitsInProgress, int xceiverCount
			, int failedVolumes, VolumeFailureSummary volumeFailureSummary)
		{
			// DatanodeProtocol
			CheckNNStartup();
			VerifyRequest(nodeReg);
			return namesystem.HandleHeartbeat(nodeReg, report, dnCacheCapacity, dnCacheUsed, 
				xceiverCount, xmitsInProgress, failedVolumes, volumeFailureSummary);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeCommand BlockReport(DatanodeRegistration nodeReg, string poolId
			, StorageBlockReport[] reports, BlockReportContext context)
		{
			// DatanodeProtocol
			CheckNNStartup();
			VerifyRequest(nodeReg);
			if (blockStateChangeLog.IsDebugEnabled())
			{
				blockStateChangeLog.Debug("*BLOCK* NameNode.blockReport: " + "from " + nodeReg + 
					", reports.length=" + reports.Length);
			}
			BlockManager bm = namesystem.GetBlockManager();
			bool noStaleStorages = false;
			for (int r = 0; r < reports.Length; r++)
			{
				BlockListAsLongs blocks = reports[r].GetBlocks();
				//
				// BlockManager.processReport accumulates information of prior calls
				// for the same node and storage, so the value returned by the last
				// call of this loop is the final updated value for noStaleStorage.
				//
				noStaleStorages = bm.ProcessReport(nodeReg, reports[r].GetStorage(), blocks, context
					, (r == reports.Length - 1));
				metrics.IncrStorageBlockReportOps();
			}
			if (nn.GetFSImage().IsUpgradeFinalized() && !namesystem.IsRollingUpgrade() && !nn
				.IsStandbyState() && noStaleStorages)
			{
				return new FinalizeCommand(poolId);
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeCommand CacheReport(DatanodeRegistration nodeReg, string poolId
			, IList<long> blockIds)
		{
			CheckNNStartup();
			VerifyRequest(nodeReg);
			if (blockStateChangeLog.IsDebugEnabled())
			{
				blockStateChangeLog.Debug("*BLOCK* NameNode.cacheReport: " + "from " + nodeReg + 
					" " + blockIds.Count + " blocks");
			}
			namesystem.GetCacheManager().ProcessCacheReport(nodeReg, blockIds);
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void BlockReceivedAndDeleted(DatanodeRegistration nodeReg, string 
			poolId, StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks)
		{
			// DatanodeProtocol
			CheckNNStartup();
			VerifyRequest(nodeReg);
			metrics.IncrBlockReceivedAndDeletedOps();
			if (blockStateChangeLog.IsDebugEnabled())
			{
				blockStateChangeLog.Debug("*BLOCK* NameNode.blockReceivedAndDeleted: " + "from " 
					+ nodeReg + " " + receivedAndDeletedBlocks.Length + " blocks.");
			}
			foreach (StorageReceivedDeletedBlocks r in receivedAndDeletedBlocks)
			{
				namesystem.ProcessIncrementalBlockReport(nodeReg, r);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ErrorReport(DatanodeRegistration nodeReg, int errorCode, string
			 msg)
		{
			// DatanodeProtocol
			CheckNNStartup();
			string dnName = (nodeReg == null) ? "Unknown DataNode" : nodeReg.ToString();
			if (errorCode == DatanodeProtocol.Notify)
			{
				Log.Info("Error report from " + dnName + ": " + msg);
				return;
			}
			VerifyRequest(nodeReg);
			if (errorCode == DatanodeProtocol.DiskError)
			{
				Log.Warn("Disk error on " + dnName + ": " + msg);
			}
			else
			{
				if (errorCode == DatanodeProtocol.FatalDiskError)
				{
					Log.Warn("Fatal disk error on " + dnName + ": " + msg);
					namesystem.GetBlockManager().GetDatanodeManager().RemoveDatanode(nodeReg);
				}
				else
				{
					Log.Info("Error report from " + dnName + ": " + msg);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual NamespaceInfo VersionRequest()
		{
			// DatanodeProtocol, NamenodeProtocol
			CheckNNStartup();
			namesystem.CheckSuperuserPrivilege();
			return namesystem.GetNamespaceInfo();
		}

		/// <summary>Verifies the given registration.</summary>
		/// <param name="nodeReg">node registration</param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.UnregisteredNodeException">if the registration is invalid
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		private void VerifyRequest(NodeRegistration nodeReg)
		{
			// verify registration ID
			string id = nodeReg.GetRegistrationID();
			string expectedID = namesystem.GetRegistrationID();
			if (!expectedID.Equals(id))
			{
				Log.Warn("Registration IDs mismatched: the " + nodeReg.GetType().Name + " ID is "
					 + id + " but the expected ID is " + expectedID);
				throw new UnregisteredNodeException(nodeReg);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshServiceAcl()
		{
			// RefreshAuthorizationPolicyProtocol
			CheckNNStartup();
			if (!serviceAuthEnabled)
			{
				throw new AuthorizationException("Service Level Authorization not enabled!");
			}
			this.clientRpcServer.RefreshServiceAcl(new Configuration(), new HDFSPolicyProvider
				());
			if (this.serviceRpcServer != null)
			{
				this.serviceRpcServer.RefreshServiceAcl(new Configuration(), new HDFSPolicyProvider
					());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshUserToGroupsMappings()
		{
			// RefreshAuthorizationPolicyProtocol
			Log.Info("Refreshing all user-to-groups mappings. Requested by user: " + GetRemoteUser
				().GetShortUserName());
			Groups.GetUserToGroupsMappingService().Refresh();
		}

		public virtual void RefreshSuperUserGroupsConfiguration()
		{
			// RefreshAuthorizationPolicyProtocol
			Log.Info("Refreshing SuperUser proxy group mapping list ");
			ProxyUsers.RefreshSuperUserGroupsConfiguration();
		}

		public virtual void RefreshCallQueue()
		{
			// RefreshCallQueueProtocol
			Log.Info("Refreshing call queue.");
			Configuration conf = new Configuration();
			clientRpcServer.RefreshCallQueue(conf);
			if (this.serviceRpcServer != null)
			{
				serviceRpcServer.RefreshCallQueue(conf);
			}
		}

		public virtual ICollection<RefreshResponse> Refresh(string identifier, string[] args
			)
		{
			// GenericRefreshProtocol
			// Let the registry handle as needed
			return RefreshRegistry.DefaultRegistry().Dispatch(identifier, args);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string[] GetGroupsForUser(string user)
		{
			// GetUserMappingsProtocol
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Getting groups for user " + user);
			}
			return UserGroupInformation.CreateRemoteUser(user).GetGroupNames();
		}

		/// <exception cref="Org.Apache.Hadoop.HA.HealthCheckFailedException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void MonitorHealth()
		{
			lock (this)
			{
				// HAServiceProtocol
				CheckNNStartup();
				nn.MonitorHealth();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TransitionToActive(HAServiceProtocol.StateChangeRequestInfo req
			)
		{
			lock (this)
			{
				// HAServiceProtocol
				CheckNNStartup();
				nn.CheckHaStateChange(req);
				nn.TransitionToActive();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TransitionToStandby(HAServiceProtocol.StateChangeRequestInfo 
			req)
		{
			lock (this)
			{
				// HAServiceProtocol
				CheckNNStartup();
				nn.CheckHaStateChange(req);
				nn.TransitionToStandby();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual HAServiceStatus GetServiceStatus()
		{
			lock (this)
			{
				// HAServiceProtocol
				CheckNNStartup();
				return nn.GetServiceStatus();
			}
		}

		/// <summary>Verify version.</summary>
		/// <param name="version">layout version</param>
		/// <exception cref="System.IO.IOException">on layout version mismatch</exception>
		internal virtual void VerifyLayoutVersion(int version)
		{
			if (version != HdfsConstants.NamenodeLayoutVersion)
			{
				throw new IncorrectVersionException(HdfsConstants.NamenodeLayoutVersion, version, 
					"data node");
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Common.IncorrectVersionException"/
		/// 	>
		private void VerifySoftwareVersion(DatanodeRegistration dnReg)
		{
			string dnVersion = dnReg.GetSoftwareVersion();
			if (VersionUtil.CompareVersions(dnVersion, minimumDataNodeVersion) < 0)
			{
				IncorrectVersionException ive = new IncorrectVersionException(minimumDataNodeVersion
					, dnVersion, "DataNode", "NameNode");
				Log.Warn(ive.Message + " DN: " + dnReg);
				throw ive;
			}
			string nnVersion = VersionInfo.GetVersion();
			if (!dnVersion.Equals(nnVersion))
			{
				string messagePrefix = "Reported DataNode version '" + dnVersion + "' of DN " + dnReg
					 + " does not match NameNode version '" + nnVersion + "'";
				long nnCTime = nn.GetFSImage().GetStorage().GetCTime();
				long dnCTime = dnReg.GetStorageInfo().GetCTime();
				if (nnCTime != dnCTime)
				{
					IncorrectVersionException ive = new IncorrectVersionException(messagePrefix + " and CTime of DN ('"
						 + dnCTime + "') does not match CTime of NN ('" + nnCTime + "')");
					Log.Warn(ive.ToString(), ive);
					throw ive;
				}
				else
				{
					Log.Info(messagePrefix + ". Note: This is normal during a rolling upgrade.");
				}
			}
		}

		private static string GetClientMachine()
		{
			string clientMachine = NamenodeWebHdfsMethods.GetRemoteAddress();
			if (clientMachine == null)
			{
				//not a web client
				clientMachine = Org.Apache.Hadoop.Ipc.Server.GetRemoteAddress();
			}
			if (clientMachine == null)
			{
				//not a RPC client
				clientMachine = string.Empty;
			}
			return clientMachine;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DataEncryptionKey GetDataEncryptionKey()
		{
			CheckNNStartup();
			return namesystem.GetBlockManager().GenerateDataEncryptionKey();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string CreateSnapshot(string snapshotRoot, string snapshotName)
		{
			CheckNNStartup();
			if (!CheckPathLength(snapshotRoot))
			{
				throw new IOException("createSnapshot: Pathname too long.  Limit " + HdfsConstants
					.MaxPathLength + " characters, " + HdfsConstants.MaxPathDepth + " levels.");
			}
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntryWithPayload cacheEntry = RetryCache.WaitForCompletion(retryCache
				, null);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return (string)cacheEntry.GetPayload();
			}
			metrics.IncrCreateSnapshotOps();
			string ret = null;
			try
			{
				ret = namesystem.CreateSnapshot(snapshotRoot, snapshotName, cacheEntry != null);
			}
			finally
			{
				RetryCache.SetState(cacheEntry, ret != null, ret);
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DeleteSnapshot(string snapshotRoot, string snapshotName)
		{
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			metrics.IncrDeleteSnapshotOps();
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			// Return previous response
			bool success = false;
			try
			{
				namesystem.DeleteSnapshot(snapshotRoot, snapshotName, cacheEntry != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void AllowSnapshot(string snapshotRoot)
		{
			// Client Protocol
			CheckNNStartup();
			metrics.IncrAllowSnapshotOps();
			namesystem.AllowSnapshot(snapshotRoot);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DisallowSnapshot(string snapshot)
		{
			// Client Protocol
			CheckNNStartup();
			metrics.IncrDisAllowSnapshotOps();
			namesystem.DisallowSnapshot(snapshot);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RenameSnapshot(string snapshotRoot, string snapshotOldName, string
			 snapshotNewName)
		{
			// ClientProtocol
			CheckNNStartup();
			if (snapshotNewName == null || snapshotNewName.IsEmpty())
			{
				throw new IOException("The new snapshot name is null or empty.");
			}
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			metrics.IncrRenameSnapshotOps();
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			// Return previous response
			bool success = false;
			try
			{
				namesystem.RenameSnapshot(snapshotRoot, snapshotOldName, snapshotNewName, cacheEntry
					 != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual SnapshottableDirectoryStatus[] GetSnapshottableDirListing()
		{
			// Client Protocol
			CheckNNStartup();
			SnapshottableDirectoryStatus[] status = namesystem.GetSnapshottableDirListing();
			metrics.IncrListSnapshottableDirOps();
			return status;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual SnapshotDiffReport GetSnapshotDiffReport(string snapshotRoot, string
			 earlierSnapshotName, string laterSnapshotName)
		{
			// ClientProtocol
			CheckNNStartup();
			SnapshotDiffReport report = namesystem.GetSnapshotDiffReport(snapshotRoot, earlierSnapshotName
				, laterSnapshotName);
			metrics.IncrSnapshotDiffReportOps();
			return report;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long AddCacheDirective(CacheDirectiveInfo path, EnumSet<CacheFlag>
			 flags)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntryWithPayload cacheEntry = RetryCache.WaitForCompletion(retryCache
				, null);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return (long)cacheEntry.GetPayload();
			}
			bool success = false;
			long ret = 0;
			try
			{
				ret = namesystem.AddCacheDirective(path, flags, cacheEntry != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success, ret);
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ModifyCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag
			> flags)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			bool success = false;
			try
			{
				namesystem.ModifyCacheDirective(directive, flags, cacheEntry != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveCacheDirective(long id)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			bool success = false;
			try
			{
				namesystem.RemoveCacheDirective(id, cacheEntry != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> ListCacheDirectives
			(long prevId, CacheDirectiveInfo filter)
		{
			// ClientProtocol
			CheckNNStartup();
			if (filter == null)
			{
				filter = new CacheDirectiveInfo.Builder().Build();
			}
			return namesystem.ListCacheDirectives(prevId, filter);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void AddCachePool(CachePoolInfo info)
		{
			//ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			// Return previous response
			bool success = false;
			try
			{
				namesystem.AddCachePool(info, cacheEntry != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ModifyCachePool(CachePoolInfo info)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			// Return previous response
			bool success = false;
			try
			{
				namesystem.ModifyCachePool(info, cacheEntry != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveCachePool(string cachePoolName)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			bool success = false;
			try
			{
				namesystem.RemoveCachePool(cachePoolName, cacheEntry != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual BatchedRemoteIterator.BatchedEntries<CachePoolEntry> ListCachePools
			(string prevKey)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.ListCachePools(prevKey != null ? prevKey : string.Empty);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ModifyAclEntries(string src, IList<AclEntry> aclSpec)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.ModifyAclEntries(src, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveAclEntries(string src, IList<AclEntry> aclSpec)
		{
			// ClienProtocol
			CheckNNStartup();
			namesystem.RemoveAclEntries(src, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveDefaultAcl(string src)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.RemoveDefaultAcl(src);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveAcl(string src)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.RemoveAcl(src);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetAcl(string src, IList<AclEntry> aclSpec)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.SetAcl(src, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual AclStatus GetAclStatus(string src)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.GetAclStatus(src);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CreateEncryptionZone(string src, string keyName)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			bool success = false;
			try
			{
				namesystem.CreateEncryptionZone(src, keyName, cacheEntry != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual EncryptionZone GetEZForPath(string src)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.GetEZForPath(src);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual BatchedRemoteIterator.BatchedEntries<EncryptionZone> ListEncryptionZones
			(long prevId)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.ListEncryptionZones(prevId);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetXAttr(string src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			// Return previous response
			bool success = false;
			try
			{
				namesystem.SetXAttr(src, xAttr, flag, cacheEntry != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IList<XAttr> GetXAttrs(string src, IList<XAttr> xAttrs)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.GetXAttrs(src, xAttrs);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IList<XAttr> ListXAttrs(string src)
		{
			// ClientProtocol
			CheckNNStartup();
			return namesystem.ListXAttrs(src);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveXAttr(string src, XAttr xAttr)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Write);
			RetryCache.CacheEntry cacheEntry = RetryCache.WaitForCompletion(retryCache);
			if (cacheEntry != null && cacheEntry.IsSuccess())
			{
				return;
			}
			// Return previous response
			bool success = false;
			try
			{
				namesystem.RemoveXAttr(src, xAttr, cacheEntry != null);
				success = true;
			}
			finally
			{
				RetryCache.SetState(cacheEntry, success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckNNStartup()
		{
			if (!this.nn.IsStarted())
			{
				throw new RetriableException(this.nn.GetRole() + " still not started");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CheckAccess(string path, FsAction mode)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckAccess(path, mode);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetCurrentEditLogTxid()
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Read);
			// only active
			namesystem.CheckSuperuserPrivilege();
			// if it's not yet open for write, we may be in the process of transitioning
			// from standby to active and may not yet know what the latest committed
			// txid is
			return namesystem.GetEditLog().IsOpenForWrite() ? namesystem.GetEditLog().GetLastWrittenTxId
				() : -1;
		}

		/// <exception cref="System.IO.IOException"/>
		private static FSEditLogOp ReadOp(EditLogInputStream elis)
		{
			try
			{
				return elis.ReadOp();
			}
			catch (FileNotFoundException e)
			{
				// we can get the below two exceptions if a segment is deleted
				// (because we have accumulated too many edits) or (for the local journal/
				// no-QJM case only) if a in-progress segment is finalized under us ...
				// no need to throw an exception back to the client in this case
				Log.Debug("Tried to read from deleted or moved edit log segment", e);
				return null;
			}
			catch (TransferFsImage.HttpGetFailedException e)
			{
				Log.Debug("Tried to read from deleted edit log segment", e);
				return null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual EventBatchList GetEditsFromTxid(long txid)
		{
			// ClientProtocol
			CheckNNStartup();
			namesystem.CheckOperation(NameNode.OperationCategory.Read);
			// only active
			namesystem.CheckSuperuserPrivilege();
			int maxEventsPerRPC = nn.conf.GetInt(DFSConfigKeys.DfsNamenodeInotifyMaxEventsPerRpcKey
				, DFSConfigKeys.DfsNamenodeInotifyMaxEventsPerRpcDefault);
			FSEditLog log = namesystem.GetFSImage().GetEditLog();
			long syncTxid = log.GetSyncTxId();
			// If we haven't synced anything yet, we can only read finalized
			// segments since we can't reliably determine which txns in in-progress
			// segments have actually been committed (e.g. written to a quorum of JNs).
			// If we have synced txns, we can definitely read up to syncTxid since
			// syncTxid is only updated after a transaction is committed to all
			// journals. (In-progress segments written by old writers are already
			// discarded for us, so if we read any in-progress segments they are
			// guaranteed to have been written by this NameNode.)
			bool readInProgress = syncTxid > 0;
			IList<EventBatch> batches = Lists.NewArrayList();
			int totalEvents = 0;
			long maxSeenTxid = -1;
			long firstSeenTxid = -1;
			if (syncTxid > 0 && txid > syncTxid)
			{
				// we can't read past syncTxid, so there's no point in going any further
				return new EventBatchList(batches, firstSeenTxid, maxSeenTxid, syncTxid);
			}
			ICollection<EditLogInputStream> streams = null;
			try
			{
				streams = log.SelectInputStreams(txid, 0, null, readInProgress);
			}
			catch (InvalidOperationException)
			{
				// can happen if we have
				// transitioned out of active and haven't yet transitioned to standby
				// and are using QJM -- the edit log will be closed and this exception
				// will result
				Log.Info("NN is transitioning from active to standby and FSEditLog " + "is closed -- could not read edits"
					);
				return new EventBatchList(batches, firstSeenTxid, maxSeenTxid, syncTxid);
			}
			bool breakOuter = false;
			foreach (EditLogInputStream elis in streams)
			{
				// our assumption in this code is the EditLogInputStreams are ordered by
				// starting txid
				try
				{
					FSEditLogOp op = null;
					while ((op = ReadOp(elis)) != null)
					{
						// break out of here in the unlikely event that syncTxid is so
						// out of date that its segment has already been deleted, so the first
						// txid we get is greater than syncTxid
						if (syncTxid > 0 && op.GetTransactionId() > syncTxid)
						{
							breakOuter = true;
							break;
						}
						EventBatch eventBatch = InotifyFSEditLogOpTranslator.Translate(op);
						if (eventBatch != null)
						{
							batches.AddItem(eventBatch);
							totalEvents += eventBatch.GetEvents().Length;
						}
						if (op.GetTransactionId() > maxSeenTxid)
						{
							maxSeenTxid = op.GetTransactionId();
						}
						if (firstSeenTxid == -1)
						{
							firstSeenTxid = op.GetTransactionId();
						}
						if (totalEvents >= maxEventsPerRPC || (syncTxid > 0 && op.GetTransactionId() == syncTxid
							))
						{
							// we're done
							breakOuter = true;
							break;
						}
					}
				}
				finally
				{
					elis.Close();
				}
				if (breakOuter)
				{
					break;
				}
			}
			return new EventBatchList(batches, firstSeenTxid, maxSeenTxid, syncTxid);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual SpanReceiverInfo[] ListSpanReceivers()
		{
			// TraceAdminProtocol
			CheckNNStartup();
			namesystem.CheckSuperuserPrivilege();
			return nn.spanReceiverHost.ListSpanReceivers();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long AddSpanReceiver(SpanReceiverInfo info)
		{
			// TraceAdminProtocol
			CheckNNStartup();
			namesystem.CheckSuperuserPrivilege();
			return nn.spanReceiverHost.AddSpanReceiver(info);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveSpanReceiver(long id)
		{
			// TraceAdminProtocol
			CheckNNStartup();
			namesystem.CheckSuperuserPrivilege();
			nn.spanReceiverHost.RemoveSpanReceiver(id);
		}
	}
}
