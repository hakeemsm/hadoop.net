using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Lang.Mutable;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Utility class to create BlockReader implementations.</summary>
	public class BlockReaderFactory : ShortCircuitCache.ShortCircuitReplicaCreator
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.BlockReaderFactory
			));

		public class FailureInjector
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void InjectRequestFileDescriptorsFailure()
			{
			}

			// do nothing
			public virtual bool GetSupportsReceiptVerification()
			{
				return true;
			}
		}

		[VisibleForTesting]
		internal static ShortCircuitCache.ShortCircuitReplicaCreator createShortCircuitReplicaInfoCallback
			 = null;

		private readonly DFSClient.Conf conf;

		/// <summary>Injects failures into specific operations during unit tests.</summary>
		private readonly BlockReaderFactory.FailureInjector failureInjector;

		/// <summary>The file name, for logging and debugging purposes.</summary>
		private string fileName;

		/// <summary>The block ID and block pool ID to use.</summary>
		private ExtendedBlock block;

		/// <summary>The block token to use for security purposes.</summary>
		private Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token;

		/// <summary>The offset within the block to start reading at.</summary>
		private long startOffset;

		/// <summary>If false, we won't try to verify the block checksum.</summary>
		private bool verifyChecksum;

		/// <summary>The name of this client.</summary>
		private string clientName;

		/// <summary>The DataNode we're talking to.</summary>
		private DatanodeInfo datanode;

		/// <summary>StorageType of replica on DataNode.</summary>
		private StorageType storageType;

		/// <summary>If false, we won't try short-circuit local reads.</summary>
		private bool allowShortCircuitLocalReads;

		/// <summary>The ClientContext to use for things like the PeerCache.</summary>
		private ClientContext clientContext;

		/// <summary>Number of bytes to read.</summary>
		/// <remarks>Number of bytes to read.  -1 indicates no limit.</remarks>
		private long length = -1;

		/// <summary>Caching strategy to use when reading the block.</summary>
		private CachingStrategy cachingStrategy;

		/// <summary>Socket address to use to connect to peer.</summary>
		private IPEndPoint inetSocketAddress;

		/// <summary>Remote peer factory to use to create a peer, if needed.</summary>
		private RemotePeerFactory remotePeerFactory;

		/// <summary>UserGroupInformation  to use for legacy block reader local objects, if needed.
		/// 	</summary>
		private UserGroupInformation userGroupInformation;

		/// <summary>Configuration to use for legacy block reader local objects, if needed.</summary>
		private Configuration configuration;

		/// <summary>
		/// Information about the domain socket path we should use to connect to the
		/// local peer-- or null if we haven't examined the local domain socket.
		/// </summary>
		private DomainSocketFactory.PathInfo pathInfo;

		/// <summary>
		/// The remaining number of times that we'll try to pull a socket out of the
		/// cache.
		/// </summary>
		private int remainingCacheTries;

		public BlockReaderFactory(DFSClient.Conf conf)
		{
			this.conf = conf;
			this.failureInjector = conf.brfFailureInjector;
			this.remainingCacheTries = conf.nCachedConnRetry;
		}

		public virtual BlockReaderFactory SetFileName(string fileName)
		{
			this.fileName = fileName;
			return this;
		}

		public virtual BlockReaderFactory SetBlock(ExtendedBlock block)
		{
			this.block = block;
			return this;
		}

		public virtual BlockReaderFactory SetBlockToken(Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> token)
		{
			this.token = token;
			return this;
		}

		public virtual BlockReaderFactory SetStartOffset(long startOffset)
		{
			this.startOffset = startOffset;
			return this;
		}

		public virtual BlockReaderFactory SetVerifyChecksum(bool verifyChecksum)
		{
			this.verifyChecksum = verifyChecksum;
			return this;
		}

		public virtual BlockReaderFactory SetClientName(string clientName)
		{
			this.clientName = clientName;
			return this;
		}

		public virtual BlockReaderFactory SetDatanodeInfo(DatanodeInfo datanode)
		{
			this.datanode = datanode;
			return this;
		}

		public virtual BlockReaderFactory SetStorageType(StorageType storageType)
		{
			this.storageType = storageType;
			return this;
		}

		public virtual BlockReaderFactory SetAllowShortCircuitLocalReads(bool allowShortCircuitLocalReads
			)
		{
			this.allowShortCircuitLocalReads = allowShortCircuitLocalReads;
			return this;
		}

		public virtual BlockReaderFactory SetClientCacheContext(ClientContext clientContext
			)
		{
			this.clientContext = clientContext;
			return this;
		}

		public virtual BlockReaderFactory SetLength(long length)
		{
			this.length = length;
			return this;
		}

		public virtual BlockReaderFactory SetCachingStrategy(CachingStrategy cachingStrategy
			)
		{
			this.cachingStrategy = cachingStrategy;
			return this;
		}

		public virtual BlockReaderFactory SetInetSocketAddress(IPEndPoint inetSocketAddress
			)
		{
			this.inetSocketAddress = inetSocketAddress;
			return this;
		}

		public virtual BlockReaderFactory SetUserGroupInformation(UserGroupInformation userGroupInformation
			)
		{
			this.userGroupInformation = userGroupInformation;
			return this;
		}

		public virtual BlockReaderFactory SetRemotePeerFactory(RemotePeerFactory remotePeerFactory
			)
		{
			this.remotePeerFactory = remotePeerFactory;
			return this;
		}

		public virtual BlockReaderFactory SetConfiguration(Configuration configuration)
		{
			this.configuration = configuration;
			return this;
		}

		/// <summary>Build a BlockReader with the given options.</summary>
		/// <remarks>
		/// Build a BlockReader with the given options.
		/// This function will do the best it can to create a block reader that meets
		/// all of our requirements.  We prefer short-circuit block readers
		/// (BlockReaderLocal and BlockReaderLocalLegacy) over remote ones, since the
		/// former avoid the overhead of socket communication.  If short-circuit is
		/// unavailable, our next fallback is data transfer over UNIX domain sockets,
		/// if dfs.client.domain.socket.data.traffic has been enabled.  If that doesn't
		/// work, we will try to create a remote block reader that operates over TCP
		/// sockets.
		/// There are a few caches that are important here.
		/// The ShortCircuitCache stores file descriptor objects which have been passed
		/// from the DataNode.
		/// The DomainSocketFactory stores information about UNIX domain socket paths
		/// that we not been able to use in the past, so that we don't waste time
		/// retrying them over and over.  (Like all the caches, it does have a timeout,
		/// though.)
		/// The PeerCache stores peers that we have used in the past.  If we can reuse
		/// one of these peers, we avoid the overhead of re-opening a socket.  However,
		/// if the socket has been timed out on the remote end, our attempt to reuse
		/// the socket may end with an IOException.  For that reason, we limit our
		/// attempts at socket reuse to dfs.client.cached.conn.retry times.  After
		/// that, we create new sockets.  This avoids the problem where a thread tries
		/// to talk to a peer that it hasn't talked to in a while, and has to clean out
		/// every entry in a socket cache full of stale entries.
		/// </remarks>
		/// <returns>The new BlockReader.  We will not return null.</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken">
		/// If the block token was invalid.
		/// InvalidEncryptionKeyException
		/// If the encryption key was invalid.
		/// Other IOException
		/// If there was another problem.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual BlockReader Build()
		{
			BlockReader reader = null;
			Preconditions.CheckNotNull(configuration);
			if (conf.shortCircuitLocalReads && allowShortCircuitLocalReads)
			{
				if (clientContext.GetUseLegacyBlockReaderLocal())
				{
					reader = GetLegacyBlockReaderLocal();
					if (reader != null)
					{
						if (Log.IsTraceEnabled())
						{
							Log.Trace(this + ": returning new legacy block reader local.");
						}
						return reader;
					}
				}
				else
				{
					reader = GetBlockReaderLocal();
					if (reader != null)
					{
						if (Log.IsTraceEnabled())
						{
							Log.Trace(this + ": returning new block reader local.");
						}
						return reader;
					}
				}
			}
			if (conf.domainSocketDataTraffic)
			{
				reader = GetRemoteBlockReaderFromDomain();
				if (reader != null)
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace(this + ": returning new remote block reader using " + "UNIX domain socket on "
							 + pathInfo.GetPath());
					}
					return reader;
				}
			}
			Preconditions.CheckState(!DFSInputStream.tcpReadsDisabledForTesting, "TCP reads were disabled for testing, but we failed to "
				 + "do a non-TCP read.");
			return GetRemoteBlockReaderFromTcp();
		}

		/// <summary>
		/// Get
		/// <see cref="BlockReaderLocalLegacy"/>
		/// for short circuited local reads.
		/// This block reader implements the path-based style of local reads
		/// first introduced in HDFS-2246.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private BlockReader GetLegacyBlockReaderLocal()
		{
			if (Log.IsTraceEnabled())
			{
				Log.Trace(this + ": trying to construct BlockReaderLocalLegacy");
			}
			if (!DFSClient.IsLocalAddress(inetSocketAddress))
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": can't construct BlockReaderLocalLegacy because " + "the address "
						 + inetSocketAddress + " is not local");
				}
				return null;
			}
			if (clientContext.GetDisableLegacyBlockReaderLocal())
			{
				PerformanceAdvisory.Log.Debug(this + ": can't construct " + "BlockReaderLocalLegacy because "
					 + "disableLegacyBlockReaderLocal is set.");
				return null;
			}
			IOException ioe = null;
			try
			{
				return BlockReaderLocalLegacy.NewBlockReader(conf, userGroupInformation, configuration
					, fileName, block, token, datanode, startOffset, length, storageType);
			}
			catch (RemoteException remoteException)
			{
				ioe = remoteException.UnwrapRemoteException(typeof(SecretManager.InvalidToken), typeof(
					AccessControlException));
			}
			catch (IOException e)
			{
				ioe = e;
			}
			if ((!(ioe is AccessControlException)) && IsSecurityException(ioe))
			{
				// Handle security exceptions.
				// We do not handle AccessControlException here, since
				// BlockReaderLocalLegacy#newBlockReader uses that exception to indicate
				// that the user is not in dfs.block.local-path-access.user, a condition
				// which requires us to disable legacy SCR.
				throw ioe;
			}
			Log.Warn(this + ": error creating legacy BlockReaderLocal.  " + "Disabling legacy local reads."
				, ioe);
			clientContext.SetDisableLegacyBlockReaderLocal();
			return null;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		private BlockReader GetBlockReaderLocal()
		{
			if (Log.IsTraceEnabled())
			{
				Log.Trace(this + ": trying to construct a BlockReaderLocal " + "for short-circuit reads."
					);
			}
			if (pathInfo == null)
			{
				pathInfo = clientContext.GetDomainSocketFactory().GetPathInfo(inetSocketAddress, 
					conf);
			}
			if (!pathInfo.GetPathState().GetUsableForShortCircuit())
			{
				PerformanceAdvisory.Log.Debug(this + ": " + pathInfo + " is not " + "usable for short circuit; giving up on BlockReaderLocal."
					);
				return null;
			}
			ShortCircuitCache cache = clientContext.GetShortCircuitCache();
			ExtendedBlockId key = new ExtendedBlockId(block.GetBlockId(), block.GetBlockPoolId
				());
			ShortCircuitReplicaInfo info = cache.FetchOrCreate(key, this);
			SecretManager.InvalidToken exc = info.GetInvalidTokenException();
			if (exc != null)
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": got InvalidToken exception while trying to " + "construct BlockReaderLocal via "
						 + pathInfo.GetPath());
				}
				throw exc;
			}
			if (info.GetReplica() == null)
			{
				if (Log.IsTraceEnabled())
				{
					PerformanceAdvisory.Log.Debug(this + ": failed to get " + "ShortCircuitReplica. Cannot construct "
						 + "BlockReaderLocal via " + pathInfo.GetPath());
				}
				return null;
			}
			return new BlockReaderLocal.Builder(conf).SetFilename(fileName).SetBlock(block).SetStartOffset
				(startOffset).SetShortCircuitReplica(info.GetReplica()).SetVerifyChecksum(verifyChecksum
				).SetCachingStrategy(cachingStrategy).SetStorageType(storageType).Build();
		}

		/// <summary>Fetch a pair of short-circuit block descriptors from a local DataNode.</summary>
		/// <returns>
		/// Null if we could not communicate with the datanode,
		/// a new ShortCircuitReplicaInfo object otherwise.
		/// ShortCircuitReplicaInfo objects may contain either an InvalidToken
		/// exception, or a ShortCircuitReplica object ready to use.
		/// </returns>
		public virtual ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo()
		{
			if (createShortCircuitReplicaInfoCallback != null)
			{
				ShortCircuitReplicaInfo info = createShortCircuitReplicaInfoCallback.CreateShortCircuitReplicaInfo
					();
				if (info != null)
				{
					return info;
				}
			}
			if (Log.IsTraceEnabled())
			{
				Log.Trace(this + ": trying to create ShortCircuitReplicaInfo.");
			}
			BlockReaderFactory.BlockReaderPeer curPeer;
			while (true)
			{
				curPeer = NextDomainPeer();
				if (curPeer == null)
				{
					break;
				}
				if (curPeer.fromCache)
				{
					remainingCacheTries--;
				}
				DomainPeer peer = (DomainPeer)curPeer.peer;
				ShortCircuitShm.Slot slot = null;
				ShortCircuitCache cache = clientContext.GetShortCircuitCache();
				try
				{
					MutableBoolean usedPeer = new MutableBoolean(false);
					slot = cache.AllocShmSlot(datanode, peer, usedPeer, new ExtendedBlockId(block.GetBlockId
						(), block.GetBlockPoolId()), clientName);
					if (usedPeer.BooleanValue())
					{
						if (Log.IsTraceEnabled())
						{
							Log.Trace(this + ": allocShmSlot used up our previous socket " + peer.GetDomainSocket
								() + ".  Allocating a new one...");
						}
						curPeer = NextDomainPeer();
						if (curPeer == null)
						{
							break;
						}
						peer = (DomainPeer)curPeer.peer;
					}
					ShortCircuitReplicaInfo info = RequestFileDescriptors(peer, slot);
					clientContext.GetPeerCache().Put(datanode, peer);
					return info;
				}
				catch (IOException e)
				{
					if (slot != null)
					{
						cache.FreeSlot(slot);
					}
					if (curPeer.fromCache)
					{
						// Handle an I/O error we got when using a cached socket.
						// These are considered less serious, because the socket may be stale.
						if (Log.IsDebugEnabled())
						{
							Log.Debug(this + ": closing stale domain peer " + peer, e);
						}
						IOUtils.Cleanup(Log, peer);
					}
					else
					{
						// Handle an I/O error we got when using a newly created socket.
						// We temporarily disable the domain socket path for a few minutes in
						// this case, to prevent wasting more time on it.
						Log.Warn(this + ": I/O error requesting file descriptors.  " + "Disabling domain socket "
							 + peer.GetDomainSocket(), e);
						IOUtils.Cleanup(Log, peer);
						clientContext.GetDomainSocketFactory().DisableDomainSocketPath(pathInfo.GetPath()
							);
						return null;
					}
				}
			}
			return null;
		}

		/// <summary>Request file descriptors from a DomainPeer.</summary>
		/// <param name="peer">The peer to use for communication.</param>
		/// <param name="slot">
		/// If non-null, the shared memory slot to associate with the
		/// new ShortCircuitReplica.
		/// </param>
		/// <returns>
		/// A ShortCircuitReplica object if we could communicate with the
		/// datanode; null, otherwise.
		/// </returns>
		/// <exception cref="System.IO.IOException">
		/// If we encountered an I/O exception while communicating
		/// with the datanode.
		/// </exception>
		private ShortCircuitReplicaInfo RequestFileDescriptors(DomainPeer peer, ShortCircuitShm.Slot
			 slot)
		{
			ShortCircuitCache cache = clientContext.GetShortCircuitCache();
			DataOutputStream @out = new DataOutputStream(new BufferedOutputStream(peer.GetOutputStream
				()));
			ShortCircuitShm.SlotId slotId = slot == null ? null : slot.GetSlotId();
			new Sender(@out).RequestShortCircuitFds(block, token, slotId, 1, failureInjector.
				GetSupportsReceiptVerification());
			DataInputStream @in = new DataInputStream(peer.GetInputStream());
			DataTransferProtos.BlockOpResponseProto resp = DataTransferProtos.BlockOpResponseProto
				.ParseFrom(PBHelper.VintPrefixed(@in));
			DomainSocket sock = peer.GetDomainSocket();
			failureInjector.InjectRequestFileDescriptorsFailure();
			switch (resp.GetStatus())
			{
				case DataTransferProtos.Status.Success:
				{
					byte[] buf = new byte[1];
					FileInputStream[] fis = new FileInputStream[2];
					sock.RecvFileInputStreams(fis, buf, 0, buf.Length);
					ShortCircuitReplica replica = null;
					try
					{
						ExtendedBlockId key = new ExtendedBlockId(block.GetBlockId(), block.GetBlockPoolId
							());
						if (buf[0] == DataTransferProtos.ShortCircuitFdResponse.UseReceiptVerification.GetNumber
							())
						{
							Log.Trace("Sending receipt verification byte for slot " + slot);
							sock.GetOutputStream().Write(0);
						}
						replica = new ShortCircuitReplica(key, fis[0], fis[1], cache, Time.MonotonicNow()
							, slot);
						return new ShortCircuitReplicaInfo(replica);
					}
					catch (IOException e)
					{
						// This indicates an error reading from disk, or a format error.  Since
						// it's not a socket communication problem, we return null rather than
						// throwing an exception.
						Log.Warn(this + ": error creating ShortCircuitReplica.", e);
						return null;
					}
					finally
					{
						if (replica == null)
						{
							IOUtils.Cleanup(DFSClient.Log, fis[0], fis[1]);
						}
					}
					goto case DataTransferProtos.Status.ErrorUnsupported;
				}

				case DataTransferProtos.Status.ErrorUnsupported:
				{
					if (!resp.HasShortCircuitAccessVersion())
					{
						Log.Warn("short-circuit read access is disabled for " + "DataNode " + datanode + 
							".  reason: " + resp.GetMessage());
						clientContext.GetDomainSocketFactory().DisableShortCircuitForPath(pathInfo.GetPath
							());
					}
					else
					{
						Log.Warn("short-circuit read access for the file " + fileName + " is disabled for DataNode "
							 + datanode + ".  reason: " + resp.GetMessage());
					}
					return null;
				}

				case DataTransferProtos.Status.ErrorAccessToken:
				{
					string msg = "access control error while " + "attempting to set up short-circuit access to "
						 + fileName + resp.GetMessage();
					if (Log.IsDebugEnabled())
					{
						Log.Debug(this + ":" + msg);
					}
					return new ShortCircuitReplicaInfo(new SecretManager.InvalidToken(msg));
				}

				default:
				{
					Log.Warn(this + ": unknown response code " + resp.GetStatus() + " while attempting to set up short-circuit access. "
						 + resp.GetMessage());
					clientContext.GetDomainSocketFactory().DisableShortCircuitForPath(pathInfo.GetPath
						());
					return null;
				}
			}
		}

		/// <summary>Get a RemoteBlockReader that communicates over a UNIX domain socket.</summary>
		/// <returns>
		/// The new BlockReader, or null if we failed to create the block
		/// reader.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken">
		/// If the block token was invalid.
		/// Potentially other security-related execptions.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		private BlockReader GetRemoteBlockReaderFromDomain()
		{
			if (pathInfo == null)
			{
				pathInfo = clientContext.GetDomainSocketFactory().GetPathInfo(inetSocketAddress, 
					conf);
			}
			if (!pathInfo.GetPathState().GetUsableForDataTransfer())
			{
				PerformanceAdvisory.Log.Debug(this + ": not trying to create a " + "remote block reader because the UNIX domain socket at "
					 + pathInfo + " is not usable.");
				return null;
			}
			if (Log.IsTraceEnabled())
			{
				Log.Trace(this + ": trying to create a remote block reader from the " + "UNIX domain socket at "
					 + pathInfo.GetPath());
			}
			while (true)
			{
				BlockReaderFactory.BlockReaderPeer curPeer = NextDomainPeer();
				if (curPeer == null)
				{
					break;
				}
				if (curPeer.fromCache)
				{
					remainingCacheTries--;
				}
				DomainPeer peer = (DomainPeer)curPeer.peer;
				BlockReader blockReader = null;
				try
				{
					blockReader = GetRemoteBlockReader(peer);
					return blockReader;
				}
				catch (IOException ioe)
				{
					IOUtils.Cleanup(Log, peer);
					if (IsSecurityException(ioe))
					{
						if (Log.IsTraceEnabled())
						{
							Log.Trace(this + ": got security exception while constructing " + "a remote block reader from the unix domain socket at "
								 + pathInfo.GetPath(), ioe);
						}
						throw;
					}
					if (curPeer.fromCache)
					{
						// Handle an I/O error we got when using a cached peer.  These are
						// considered less serious, because the underlying socket may be stale.
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Closed potentially stale domain peer " + peer, ioe);
						}
					}
					else
					{
						// Handle an I/O error we got when using a newly created domain peer.
						// We temporarily disable the domain socket path for a few minutes in
						// this case, to prevent wasting more time on it.
						Log.Warn("I/O error constructing remote block reader.  Disabling " + "domain socket "
							 + peer.GetDomainSocket(), ioe);
						clientContext.GetDomainSocketFactory().DisableDomainSocketPath(pathInfo.GetPath()
							);
						return null;
					}
				}
				finally
				{
					if (blockReader == null)
					{
						IOUtils.Cleanup(Log, peer);
					}
				}
			}
			return null;
		}

		/// <summary>Get a RemoteBlockReader that communicates over a TCP socket.</summary>
		/// <returns>
		/// The new BlockReader.  We will not return null, but instead throw
		/// an exception if this fails.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken">
		/// If the block token was invalid.
		/// InvalidEncryptionKeyException
		/// If the encryption key was invalid.
		/// Other IOException
		/// If there was another problem.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		private BlockReader GetRemoteBlockReaderFromTcp()
		{
			if (Log.IsTraceEnabled())
			{
				Log.Trace(this + ": trying to create a remote block reader from a " + "TCP socket"
					);
			}
			BlockReader blockReader = null;
			while (true)
			{
				BlockReaderFactory.BlockReaderPeer curPeer = null;
				Peer peer = null;
				try
				{
					curPeer = NextTcpPeer();
					if (curPeer.fromCache)
					{
						remainingCacheTries--;
					}
					peer = curPeer.peer;
					blockReader = GetRemoteBlockReader(peer);
					return blockReader;
				}
				catch (IOException ioe)
				{
					if (IsSecurityException(ioe))
					{
						if (Log.IsTraceEnabled())
						{
							Log.Trace(this + ": got security exception while constructing " + "a remote block reader from "
								 + peer, ioe);
						}
						throw;
					}
					if ((curPeer != null) && curPeer.fromCache)
					{
						// Handle an I/O error we got when using a cached peer.  These are
						// considered less serious, because the underlying socket may be
						// stale.
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Closed potentially stale remote peer " + peer, ioe);
						}
					}
					else
					{
						// Handle an I/O error we got when using a newly created peer.
						Log.Warn("I/O error constructing remote block reader.", ioe);
						throw;
					}
				}
				finally
				{
					if (blockReader == null)
					{
						IOUtils.Cleanup(Log, peer);
					}
				}
			}
		}

		public class BlockReaderPeer
		{
			internal readonly Peer peer;

			internal readonly bool fromCache;

			internal BlockReaderPeer(Peer peer, bool fromCache)
			{
				this.peer = peer;
				this.fromCache = fromCache;
			}
		}

		/// <summary>Get the next DomainPeer-- either from the cache or by creating it.</summary>
		/// <returns>the next DomainPeer, or null if we could not construct one.</returns>
		private BlockReaderFactory.BlockReaderPeer NextDomainPeer()
		{
			if (remainingCacheTries > 0)
			{
				Peer peer = clientContext.GetPeerCache().Get(datanode, true);
				if (peer != null)
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace("nextDomainPeer: reusing existing peer " + peer);
					}
					return new BlockReaderFactory.BlockReaderPeer(peer, true);
				}
			}
			DomainSocket sock = clientContext.GetDomainSocketFactory().CreateSocket(pathInfo, 
				conf.socketTimeout);
			if (sock == null)
			{
				return null;
			}
			return new BlockReaderFactory.BlockReaderPeer(new DomainPeer(sock), false);
		}

		/// <summary>Get the next TCP-based peer-- either from the cache or by creating it.</summary>
		/// <returns>the next Peer, or null if we could not construct one.</returns>
		/// <exception cref="System.IO.IOException">
		/// If there was an error while constructing the peer
		/// (such as an InvalidEncryptionKeyException)
		/// </exception>
		private BlockReaderFactory.BlockReaderPeer NextTcpPeer()
		{
			if (remainingCacheTries > 0)
			{
				Peer peer = clientContext.GetPeerCache().Get(datanode, false);
				if (peer != null)
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace("nextTcpPeer: reusing existing peer " + peer);
					}
					return new BlockReaderFactory.BlockReaderPeer(peer, true);
				}
			}
			try
			{
				Peer peer = remotePeerFactory.NewConnectedPeer(inetSocketAddress, token, datanode
					);
				if (Log.IsTraceEnabled())
				{
					Log.Trace("nextTcpPeer: created newConnectedPeer " + peer);
				}
				return new BlockReaderFactory.BlockReaderPeer(peer, false);
			}
			catch (IOException e)
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace("nextTcpPeer: failed to create newConnectedPeer " + "connected to " + datanode
						);
				}
				throw;
			}
		}

		/// <summary>Determine if an exception is security-related.</summary>
		/// <remarks>
		/// Determine if an exception is security-related.
		/// We need to handle these exceptions differently than other IOExceptions.
		/// They don't indicate a communication problem.  Instead, they mean that there
		/// is some action the client needs to take, such as refetching block tokens,
		/// renewing encryption keys, etc.
		/// </remarks>
		/// <param name="ioe">The exception</param>
		/// <returns>True only if the exception is security-related.</returns>
		private static bool IsSecurityException(IOException ioe)
		{
			return (ioe is SecretManager.InvalidToken) || (ioe is InvalidEncryptionKeyException
				) || (ioe is InvalidBlockTokenException) || (ioe is AccessControlException);
		}

		/// <exception cref="System.IO.IOException"/>
		private BlockReader GetRemoteBlockReader(Peer peer)
		{
			if (conf.useLegacyBlockReader)
			{
				return RemoteBlockReader.NewBlockReader(fileName, block, token, startOffset, length
					, conf.ioBufferSize, verifyChecksum, clientName, peer, datanode, clientContext.GetPeerCache
					(), cachingStrategy);
			}
			else
			{
				return RemoteBlockReader2.NewBlockReader(fileName, block, token, startOffset, length
					, verifyChecksum, clientName, peer, datanode, clientContext.GetPeerCache(), cachingStrategy
					);
			}
		}

		public override string ToString()
		{
			return "BlockReaderFactory(fileName=" + fileName + ", block=" + block + ")";
		}

		/// <summary>File name to print when accessing a block directly (from servlets)</summary>
		/// <param name="s">Address of the block location</param>
		/// <param name="poolId">Block pool ID of the block</param>
		/// <param name="blockId">Block ID of the block</param>
		/// <returns>string that has a file name for debug purposes</returns>
		public static string GetFileName(IPEndPoint s, string poolId, long blockId)
		{
			return s.ToString() + ":" + poolId + ":" + blockId;
		}
	}
}
