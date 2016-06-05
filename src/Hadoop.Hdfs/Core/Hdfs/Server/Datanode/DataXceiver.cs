using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Com.Google.Common.Base;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Thread for processing incoming/outgoing data stream.</summary>
	internal class DataXceiver : Receiver, Runnable
	{
		public static readonly Log Log = DataNode.Log;

		internal static readonly Log ClientTraceLog = DataNode.ClientTraceLog;

		private Peer peer;

		private readonly string remoteAddress;

		private readonly string remoteAddressWithoutPort;

		private readonly string localAddress;

		private readonly DataNode datanode;

		private readonly DNConf dnConf;

		private readonly DataXceiverServer dataXceiverServer;

		private readonly bool connectToDnViaHostname;

		private long opStartTime;

		private readonly InputStream socketIn;

		private OutputStream socketOut;

		private BlockReceiver blockReceiver = null;

		/// <summary>Client Name used in previous operation.</summary>
		/// <remarks>
		/// Client Name used in previous operation. Not available on first request
		/// on the socket.
		/// </remarks>
		private string previousOpClientName;

		// address of remote side
		// only the address, no port
		// local address of this daemon
		//the start time of receiving an Op
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Hdfs.Server.Datanode.DataXceiver Create(Peer peer
			, DataNode dn, DataXceiverServer dataXceiverServer)
		{
			return new Org.Apache.Hadoop.Hdfs.Server.Datanode.DataXceiver(peer, dn, dataXceiverServer
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private DataXceiver(Peer peer, DataNode datanode, DataXceiverServer dataXceiverServer
			)
		{
			this.peer = peer;
			this.dnConf = datanode.GetDnConf();
			this.socketIn = peer.GetInputStream();
			this.socketOut = peer.GetOutputStream();
			this.datanode = datanode;
			this.dataXceiverServer = dataXceiverServer;
			this.connectToDnViaHostname = datanode.GetDnConf().connectToDnViaHostname;
			remoteAddress = peer.GetRemoteAddressString();
			int colonIdx = remoteAddress.IndexOf(':');
			remoteAddressWithoutPort = (colonIdx < 0) ? remoteAddress : Sharpen.Runtime.Substring
				(remoteAddress, 0, colonIdx);
			localAddress = peer.GetLocalAddressString();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Number of active connections is: " + datanode.GetXceiverCount());
			}
		}

		/// <summary>Update the current thread's name to contain the current status.</summary>
		/// <remarks>
		/// Update the current thread's name to contain the current status.
		/// Use this only after this receiver has started on its thread, i.e.,
		/// outside the constructor.
		/// </remarks>
		private void UpdateCurrentThreadName(string status)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("DataXceiver for client ");
			if (previousOpClientName != null)
			{
				sb.Append(previousOpClientName).Append(" at ");
			}
			sb.Append(remoteAddress);
			if (status != null)
			{
				sb.Append(" [").Append(status).Append("]");
			}
			Sharpen.Thread.CurrentThread().SetName(sb.ToString());
		}

		/// <summary>Return the datanode object.</summary>
		internal virtual DataNode GetDataNode()
		{
			return datanode;
		}

		private OutputStream GetOutputStream()
		{
			return socketOut;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void SendOOB()
		{
			Log.Info("Sending OOB to peer: " + peer);
			if (blockReceiver != null)
			{
				blockReceiver.SendOOB();
			}
		}

		/// <summary>Read/write data from/to the DataXceiverServer.</summary>
		public virtual void Run()
		{
			int opsProcessed = 0;
			OP op = null;
			try
			{
				dataXceiverServer.AddPeer(peer, Sharpen.Thread.CurrentThread(), this);
				peer.SetWriteTimeout(datanode.GetDnConf().socketWriteTimeout);
				InputStream input = socketIn;
				try
				{
					IOStreamPair saslStreams = datanode.saslServer.Receive(peer, socketOut, socketIn, 
						datanode.GetXferAddress().Port, datanode.GetDatanodeId());
					input = new BufferedInputStream(saslStreams.@in, HdfsConstants.SmallBufferSize);
					socketOut = saslStreams.@out;
				}
				catch (InvalidMagicNumberException imne)
				{
					if (imne.IsHandshake4Encryption())
					{
						Log.Info("Failed to read expected encryption handshake from client " + "at " + peer
							.GetRemoteAddressString() + ". Perhaps the client " + "is running an older version of Hadoop which does not support "
							 + "encryption");
					}
					else
					{
						Log.Info("Failed to read expected SASL data transfer protection " + "handshake from client at "
							 + peer.GetRemoteAddressString() + ". Perhaps the client is running an older version of Hadoop "
							 + "which does not support SASL data transfer protection");
					}
					return;
				}
				base.Initialize(new DataInputStream(input));
				do
				{
					// We process requests in a loop, and stay around for a short timeout.
					// This optimistic behaviour allows the other end to reuse connections.
					// Setting keepalive timeout to 0 disable this behavior.
					UpdateCurrentThreadName("Waiting for operation #" + (opsProcessed + 1));
					try
					{
						if (opsProcessed != 0)
						{
							System.Diagnostics.Debug.Assert(dnConf.socketKeepaliveTimeout > 0);
							peer.SetReadTimeout(dnConf.socketKeepaliveTimeout);
						}
						else
						{
							peer.SetReadTimeout(dnConf.socketTimeout);
						}
						op = ReadOp();
					}
					catch (ThreadInterruptedException)
					{
						// Time out while we wait for client rpc
						break;
					}
					catch (IOException err)
					{
						// Since we optimistically expect the next op, it's quite normal to get EOF here.
						if (opsProcessed > 0 && (err is EOFException || err is ClosedChannelException))
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("Cached " + peer + " closing after " + opsProcessed + " ops");
							}
						}
						else
						{
							IncrDatanodeNetworkErrors();
							throw;
						}
						break;
					}
					// restore normal timeout
					if (opsProcessed != 0)
					{
						peer.SetReadTimeout(dnConf.socketTimeout);
					}
					opStartTime = Time.MonotonicNow();
					ProcessOp(op);
					++opsProcessed;
				}
				while ((peer != null) && (!peer.IsClosed() && dnConf.socketKeepaliveTimeout > 0));
			}
			catch (Exception t)
			{
				string s = datanode.GetDisplayName() + ":DataXceiver error processing " + ((op ==
					 null) ? "unknown" : op.ToString()) + " operation " + " src: " + remoteAddress +
					 " dst: " + localAddress;
				if (op == OP.WriteBlock && t is ReplicaAlreadyExistsException)
				{
					// For WRITE_BLOCK, it is okay if the replica already exists since
					// client and replication may write the same block to the same datanode
					// at the same time.
					if (Log.IsTraceEnabled())
					{
						Log.Trace(s, t);
					}
					else
					{
						Log.Info(s + "; " + t);
					}
				}
				else
				{
					if (op == OP.ReadBlock && t is SocketTimeoutException)
					{
						string s1 = "Likely the client has stopped reading, disconnecting it";
						s1 += " (" + s + ")";
						if (Log.IsTraceEnabled())
						{
							Log.Trace(s1, t);
						}
						else
						{
							Log.Info(s1 + "; " + t);
						}
					}
					else
					{
						Log.Error(s, t);
					}
				}
			}
			finally
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug(datanode.GetDisplayName() + ":Number of active connections is: " + datanode
						.GetXceiverCount());
				}
				UpdateCurrentThreadName("Cleaning up");
				if (peer != null)
				{
					dataXceiverServer.ClosePeer(peer);
					IOUtils.CloseStream(@in);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RequestShortCircuitFds(ExtendedBlock blk, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> token, ShortCircuitShm.SlotId slotId, int maxVersion, bool
			 supportsReceiptVerification)
		{
			UpdateCurrentThreadName("Passing file descriptors for block " + blk);
			DataOutputStream @out = GetBufferedOutputStream();
			CheckAccess(@out, true, blk, token, OP.RequestShortCircuitFds, BlockTokenSecretManager.AccessMode
				.Read);
			DataTransferProtos.BlockOpResponseProto.Builder bld = DataTransferProtos.BlockOpResponseProto
				.NewBuilder();
			FileInputStream[] fis = null;
			ShortCircuitShm.SlotId registeredSlotId = null;
			bool success = false;
			try
			{
				try
				{
					if (peer.GetDomainSocket() == null)
					{
						throw new IOException("You cannot pass file descriptors over " + "anything but a UNIX domain socket."
							);
					}
					if (slotId != null)
					{
						bool isCached = datanode.data.IsCached(blk.GetBlockPoolId(), blk.GetBlockId());
						datanode.shortCircuitRegistry.RegisterSlot(ExtendedBlockId.FromExtendedBlock(blk)
							, slotId, isCached);
						registeredSlotId = slotId;
					}
					fis = datanode.RequestShortCircuitFdsForRead(blk, token, maxVersion);
					Preconditions.CheckState(fis != null);
					bld.SetStatus(DataTransferProtos.Status.Success);
					bld.SetShortCircuitAccessVersion(DataNode.CurrentBlockFormatVersion);
				}
				catch (DataNode.ShortCircuitFdsVersionException e)
				{
					bld.SetStatus(DataTransferProtos.Status.ErrorUnsupported);
					bld.SetShortCircuitAccessVersion(DataNode.CurrentBlockFormatVersion);
					bld.SetMessage(e.Message);
				}
				catch (DataNode.ShortCircuitFdsUnsupportedException e)
				{
					bld.SetStatus(DataTransferProtos.Status.ErrorUnsupported);
					bld.SetMessage(e.Message);
				}
				catch (IOException e)
				{
					bld.SetStatus(DataTransferProtos.Status.Error);
					bld.SetMessage(e.Message);
				}
				((DataTransferProtos.BlockOpResponseProto)bld.Build()).WriteDelimitedTo(socketOut
					);
				if (fis != null)
				{
					FileDescriptor[] fds = new FileDescriptor[fis.Length];
					for (int i = 0; i < fds.Length; i++)
					{
						fds[i] = fis[i].GetFD();
					}
					byte[] buf = new byte[1];
					if (supportsReceiptVerification)
					{
						buf[0] = unchecked((byte)DataTransferProtos.ShortCircuitFdResponse.UseReceiptVerification
							.GetNumber());
					}
					else
					{
						buf[0] = unchecked((byte)DataTransferProtos.ShortCircuitFdResponse.DoNotUseReceiptVerification
							.GetNumber());
					}
					DomainSocket sock = peer.GetDomainSocket();
					sock.SendFileDescriptors(fds, buf, 0, buf.Length);
					if (supportsReceiptVerification)
					{
						Log.Trace("Reading receipt verification byte for " + slotId);
						int val = sock.GetInputStream().Read();
						if (val < 0)
						{
							throw new EOFException();
						}
					}
					else
					{
						Log.Trace("Receipt verification is not enabled on the DataNode.  " + "Not verifying "
							 + slotId);
					}
					success = true;
				}
			}
			finally
			{
				if ((!success) && (registeredSlotId != null))
				{
					Log.Info("Unregistering " + registeredSlotId + " because the " + "requestShortCircuitFdsForRead operation failed."
						);
					datanode.shortCircuitRegistry.UnregisterSlot(registeredSlotId);
				}
				if (ClientTraceLog.IsInfoEnabled())
				{
					DatanodeRegistration dnR = datanode.GetDNRegistrationForBP(blk.GetBlockPoolId());
					BlockSender.ClientTraceLog.Info(string.Format("src: 127.0.0.1, dest: 127.0.0.1, op: REQUEST_SHORT_CIRCUIT_FDS,"
						 + " blockid: %s, srvID: %s, success: %b", blk.GetBlockId(), dnR.GetDatanodeUuid
						(), success));
				}
				if (fis != null)
				{
					IOUtils.Cleanup(Log, fis);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReleaseShortCircuitFds(ShortCircuitShm.SlotId slotId)
		{
			bool success = false;
			try
			{
				string error;
				DataTransferProtos.Status status;
				try
				{
					datanode.shortCircuitRegistry.UnregisterSlot(slotId);
					error = null;
					status = DataTransferProtos.Status.Success;
				}
				catch (NotSupportedException)
				{
					error = "unsupported operation";
					status = DataTransferProtos.Status.ErrorUnsupported;
				}
				catch (Exception e)
				{
					error = e.Message;
					status = DataTransferProtos.Status.ErrorInvalid;
				}
				DataTransferProtos.ReleaseShortCircuitAccessResponseProto.Builder bld = DataTransferProtos.ReleaseShortCircuitAccessResponseProto
					.NewBuilder();
				bld.SetStatus(status);
				if (error != null)
				{
					bld.SetError(error);
				}
				((DataTransferProtos.ReleaseShortCircuitAccessResponseProto)bld.Build()).WriteDelimitedTo
					(socketOut);
				success = true;
			}
			finally
			{
				if (ClientTraceLog.IsInfoEnabled())
				{
					BlockSender.ClientTraceLog.Info(string.Format("src: 127.0.0.1, dest: 127.0.0.1, op: RELEASE_SHORT_CIRCUIT_FDS,"
						 + " shmId: %016x%016x, slotIdx: %d, srvID: %s, success: %b", slotId.GetShmId().
						GetHi(), slotId.GetShmId().GetLo(), slotId.GetSlotIdx(), datanode.GetDatanodeUuid
						(), success));
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void SendShmErrorResponse(DataTransferProtos.Status status, string error)
		{
			((DataTransferProtos.ShortCircuitShmResponseProto)DataTransferProtos.ShortCircuitShmResponseProto
				.NewBuilder().SetStatus(status).SetError(error).Build()).WriteDelimitedTo(socketOut
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private void SendShmSuccessResponse(DomainSocket sock, ShortCircuitRegistry.NewShmInfo
			 shmInfo)
		{
			DataNodeFaultInjector.Get().SendShortCircuitShmResponse();
			((DataTransferProtos.ShortCircuitShmResponseProto)DataTransferProtos.ShortCircuitShmResponseProto
				.NewBuilder().SetStatus(DataTransferProtos.Status.Success).SetId(PBHelper.Convert
				(shmInfo.shmId)).Build()).WriteDelimitedTo(socketOut);
			// Send the file descriptor for the shared memory segment.
			byte[] buf = new byte[] { unchecked((byte)0) };
			FileDescriptor[] shmFdArray = new FileDescriptor[] { shmInfo.stream.GetFD() };
			sock.SendFileDescriptors(shmFdArray, buf, 0, buf.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RequestShortCircuitShm(string clientName)
		{
			ShortCircuitRegistry.NewShmInfo shmInfo = null;
			bool success = false;
			DomainSocket sock = peer.GetDomainSocket();
			try
			{
				if (sock == null)
				{
					SendShmErrorResponse(DataTransferProtos.Status.ErrorInvalid, "Bad request from " 
						+ peer + ": must request a shared " + "memory segment over a UNIX domain socket."
						);
					return;
				}
				try
				{
					shmInfo = datanode.shortCircuitRegistry.CreateNewMemorySegment(clientName, sock);
					// After calling #{ShortCircuitRegistry#createNewMemorySegment}, the
					// socket is managed by the DomainSocketWatcher, not the DataXceiver.
					ReleaseSocket();
				}
				catch (NotSupportedException)
				{
					SendShmErrorResponse(DataTransferProtos.Status.ErrorUnsupported, "This datanode has not been configured to support "
						 + "short-circuit shared memory segments.");
					return;
				}
				catch (IOException e)
				{
					SendShmErrorResponse(DataTransferProtos.Status.Error, "Failed to create shared file descriptor: "
						 + e.Message);
					return;
				}
				SendShmSuccessResponse(sock, shmInfo);
				success = true;
			}
			finally
			{
				if (ClientTraceLog.IsInfoEnabled())
				{
					if (success)
					{
						BlockSender.ClientTraceLog.Info(string.Format("cliID: %s, src: 127.0.0.1, dest: 127.0.0.1, "
							 + "op: REQUEST_SHORT_CIRCUIT_SHM," + " shmId: %016x%016x, srvID: %s, success: true"
							, clientName, shmInfo.shmId.GetHi(), shmInfo.shmId.GetLo(), datanode.GetDatanodeUuid
							()));
					}
					else
					{
						BlockSender.ClientTraceLog.Info(string.Format("cliID: %s, src: 127.0.0.1, dest: 127.0.0.1, "
							 + "op: REQUEST_SHORT_CIRCUIT_SHM, " + "shmId: n/a, srvID: %s, success: false", 
							clientName, datanode.GetDatanodeUuid()));
					}
				}
				if ((!success) && (peer == null))
				{
					// The socket is now managed by the DomainSocketWatcher.  However,
					// we failed to pass it to the client.  We call shutdown() on the
					// UNIX domain socket now.  This will trigger the DomainSocketWatcher
					// callback.  The callback will close the domain socket.
					// We don't want to close the socket here, since that might lead to
					// bad behavior inside the poll() call.  See HADOOP-11802 for details.
					try
					{
						Log.Warn("Failed to send success response back to the client.  " + "Shutting down socket for "
							 + shmInfo.shmId + ".");
						sock.Shutdown();
					}
					catch (IOException e)
					{
						Log.Warn("Failed to shut down socket in error handler", e);
					}
				}
				IOUtils.Cleanup(null, shmInfo);
			}
		}

		internal virtual void ReleaseSocket()
		{
			dataXceiverServer.ReleasePeer(peer);
			peer = null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadBlock(ExtendedBlock block, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, string clientName, long blockOffset, long length
			, bool sendChecksum, CachingStrategy cachingStrategy)
		{
			previousOpClientName = clientName;
			long read = 0;
			UpdateCurrentThreadName("Sending block " + block);
			OutputStream baseStream = GetOutputStream();
			DataOutputStream @out = GetBufferedOutputStream();
			CheckAccess(@out, true, block, blockToken, OP.ReadBlock, BlockTokenSecretManager.AccessMode
				.Read);
			// send the block
			BlockSender blockSender = null;
			DatanodeRegistration dnR = datanode.GetDNRegistrationForBP(block.GetBlockPoolId()
				);
			string clientTraceFmt = clientName.Length > 0 && ClientTraceLog.IsInfoEnabled() ? 
				string.Format(DataNode.DnClienttraceFormat, localAddress, remoteAddress, "%d", "HDFS_READ"
				, clientName, "%d", dnR.GetDatanodeUuid(), block, "%d") : dnR + " Served block "
				 + block + " to " + remoteAddress;
			try
			{
				try
				{
					blockSender = new BlockSender(block, blockOffset, length, true, false, sendChecksum
						, datanode, clientTraceFmt, cachingStrategy);
				}
				catch (IOException e)
				{
					string msg = "opReadBlock " + block + " received exception " + e;
					Log.Info(msg);
					SendResponse(DataTransferProtos.Status.Error, msg);
					throw;
				}
				// send op status
				WriteSuccessWithChecksumInfo(blockSender, new DataOutputStream(GetOutputStream())
					);
				long beginRead = Time.MonotonicNow();
				read = blockSender.SendBlock(@out, baseStream, null);
				// send data
				long duration = Time.MonotonicNow() - beginRead;
				if (blockSender.DidSendEntireByteRange())
				{
					// If we sent the entire range, then we should expect the client
					// to respond with a Status enum.
					try
					{
						DataTransferProtos.ClientReadStatusProto stat = DataTransferProtos.ClientReadStatusProto
							.ParseFrom(PBHelper.VintPrefixed(@in));
						if (!stat.HasStatus())
						{
							Log.Warn("Client " + peer.GetRemoteAddressString() + " did not send a valid status code after reading. "
								 + "Will close connection.");
							IOUtils.CloseStream(@out);
						}
					}
					catch (IOException ioe)
					{
						Log.Debug("Error reading client status response. Will close connection.", ioe);
						IOUtils.CloseStream(@out);
						IncrDatanodeNetworkErrors();
					}
				}
				else
				{
					IOUtils.CloseStream(@out);
				}
				datanode.metrics.IncrBytesRead((int)read);
				datanode.metrics.IncrBlocksRead();
				datanode.metrics.IncrTotalReadTime(duration);
			}
			catch (SocketException ignored)
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace(dnR + ":Ignoring exception while serving " + block + " to " + remoteAddress
						, ignored);
				}
				// Its ok for remote side to close the connection anytime.
				datanode.metrics.IncrBlocksRead();
				IOUtils.CloseStream(@out);
			}
			catch (IOException ioe)
			{
				/* What exactly should we do here?
				* Earlier version shutdown() datanode if there is disk error.
				*/
				if (!(ioe is SocketTimeoutException))
				{
					Log.Warn(dnR + ":Got exception while serving " + block + " to " + remoteAddress, 
						ioe);
					IncrDatanodeNetworkErrors();
				}
				throw;
			}
			finally
			{
				IOUtils.CloseStream(blockSender);
			}
			//update metrics
			datanode.metrics.AddReadBlockOp(Elapsed());
			datanode.metrics.IncrReadsFromClient(peer.IsLocal(), read);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void WriteBlock(ExtendedBlock block, StorageType storageType, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, string clientname, DatanodeInfo[] targets, StorageType
			[] targetStorageTypes, DatanodeInfo srcDataNode, BlockConstructionStage stage, int
			 pipelineSize, long minBytesRcvd, long maxBytesRcvd, long latestGenerationStamp, 
			DataChecksum requestedChecksum, CachingStrategy cachingStrategy, bool allowLazyPersist
			, bool pinning, bool[] targetPinnings)
		{
			previousOpClientName = clientname;
			UpdateCurrentThreadName("Receiving block " + block);
			bool isDatanode = clientname.Length == 0;
			bool isClient = !isDatanode;
			bool isTransfer = stage == BlockConstructionStage.TransferRbw || stage == BlockConstructionStage
				.TransferFinalized;
			long size = 0;
			// reply to upstream datanode or client 
			DataOutputStream replyOut = GetBufferedOutputStream();
			CheckAccess(replyOut, isClient, block, blockToken, OP.WriteBlock, BlockTokenSecretManager.AccessMode
				.Write);
			// check single target for transfer-RBW/Finalized 
			if (isTransfer && targets.Length > 0)
			{
				throw new IOException(stage + " does not support multiple targets " + Arrays.AsList
					(targets));
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("opWriteBlock: stage=" + stage + ", clientname=" + clientname + "\n  block  ="
					 + block + ", newGs=" + latestGenerationStamp + ", bytesRcvd=[" + minBytesRcvd +
					 ", " + maxBytesRcvd + "]" + "\n  targets=" + Arrays.AsList(targets) + "; pipelineSize="
					 + pipelineSize + ", srcDataNode=" + srcDataNode + ", pinning=" + pinning);
				Log.Debug("isDatanode=" + isDatanode + ", isClient=" + isClient + ", isTransfer="
					 + isTransfer);
				Log.Debug("writeBlock receive buf size " + peer.GetReceiveBufferSize() + " tcp no delay "
					 + peer.GetTcpNoDelay());
			}
			// We later mutate block's generation stamp and length, but we need to
			// forward the original version of the block to downstream mirrors, so
			// make a copy here.
			ExtendedBlock originalBlock = new ExtendedBlock(block);
			if (block.GetNumBytes() == 0)
			{
				block.SetNumBytes(dataXceiverServer.estimateBlockSize);
			}
			Log.Info("Receiving " + block + " src: " + remoteAddress + " dest: " + localAddress
				);
			DataOutputStream mirrorOut = null;
			// stream to next target
			DataInputStream mirrorIn = null;
			// reply from next target
			Socket mirrorSock = null;
			// socket to next target
			string mirrorNode = null;
			// the name:port of next target
			string firstBadLink = string.Empty;
			// first datanode that failed in connection setup
			DataTransferProtos.Status mirrorInStatus = DataTransferProtos.Status.Success;
			string storageUuid;
			try
			{
				if (isDatanode || stage != BlockConstructionStage.PipelineCloseRecovery)
				{
					// open a block receiver
					blockReceiver = new BlockReceiver(block, storageType, @in, peer.GetRemoteAddressString
						(), peer.GetLocalAddressString(), stage, latestGenerationStamp, minBytesRcvd, maxBytesRcvd
						, clientname, srcDataNode, datanode, requestedChecksum, cachingStrategy, allowLazyPersist
						, pinning);
					storageUuid = blockReceiver.GetStorageUuid();
				}
				else
				{
					storageUuid = datanode.data.RecoverClose(block, latestGenerationStamp, minBytesRcvd
						);
				}
				//
				// Connect to downstream machine, if appropriate
				//
				if (targets.Length > 0)
				{
					IPEndPoint mirrorTarget = null;
					// Connect to backup machine
					mirrorNode = targets[0].GetXferAddr(connectToDnViaHostname);
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Connecting to datanode " + mirrorNode);
					}
					mirrorTarget = NetUtils.CreateSocketAddr(mirrorNode);
					mirrorSock = datanode.NewSocket();
					try
					{
						int timeoutValue = dnConf.socketTimeout + (HdfsServerConstants.ReadTimeoutExtension
							 * targets.Length);
						int writeTimeout = dnConf.socketWriteTimeout + (HdfsServerConstants.WriteTimeoutExtension
							 * targets.Length);
						NetUtils.Connect(mirrorSock, mirrorTarget, timeoutValue);
						mirrorSock.ReceiveTimeout = timeoutValue;
						mirrorSock.SetSendBufferSize(HdfsConstants.DefaultDataSocketSize);
						OutputStream unbufMirrorOut = NetUtils.GetOutputStream(mirrorSock, writeTimeout);
						InputStream unbufMirrorIn = NetUtils.GetInputStream(mirrorSock);
						DataEncryptionKeyFactory keyFactory = datanode.GetDataEncryptionKeyFactoryForBlock
							(block);
						IOStreamPair saslStreams = datanode.saslClient.SocketSend(mirrorSock, unbufMirrorOut
							, unbufMirrorIn, keyFactory, blockToken, targets[0]);
						unbufMirrorOut = saslStreams.@out;
						unbufMirrorIn = saslStreams.@in;
						mirrorOut = new DataOutputStream(new BufferedOutputStream(unbufMirrorOut, HdfsConstants
							.SmallBufferSize));
						mirrorIn = new DataInputStream(unbufMirrorIn);
						// Do not propagate allowLazyPersist to downstream DataNodes.
						if (targetPinnings != null && targetPinnings.Length > 0)
						{
							new Sender(mirrorOut).WriteBlock(originalBlock, targetStorageTypes[0], blockToken
								, clientname, targets, targetStorageTypes, srcDataNode, stage, pipelineSize, minBytesRcvd
								, maxBytesRcvd, latestGenerationStamp, requestedChecksum, cachingStrategy, false
								, targetPinnings[0], targetPinnings);
						}
						else
						{
							new Sender(mirrorOut).WriteBlock(originalBlock, targetStorageTypes[0], blockToken
								, clientname, targets, targetStorageTypes, srcDataNode, stage, pipelineSize, minBytesRcvd
								, maxBytesRcvd, latestGenerationStamp, requestedChecksum, cachingStrategy, false
								, false, targetPinnings);
						}
						mirrorOut.Flush();
						DataNodeFaultInjector.Get().WriteBlockAfterFlush();
						// read connect ack (only for clients, not for replication req)
						if (isClient)
						{
							DataTransferProtos.BlockOpResponseProto connectAck = DataTransferProtos.BlockOpResponseProto
								.ParseFrom(PBHelper.VintPrefixed(mirrorIn));
							mirrorInStatus = connectAck.GetStatus();
							firstBadLink = connectAck.GetFirstBadLink();
							if (Log.IsDebugEnabled() || mirrorInStatus != DataTransferProtos.Status.Success)
							{
								Log.Info("Datanode " + targets.Length + " got response for connect ack " + " from downstream datanode with firstbadlink as "
									 + firstBadLink);
							}
						}
					}
					catch (IOException e)
					{
						if (isClient)
						{
							((DataTransferProtos.BlockOpResponseProto)DataTransferProtos.BlockOpResponseProto
								.NewBuilder().SetStatus(DataTransferProtos.Status.Error).SetFirstBadLink(targets
								[0].GetXferAddr()).Build()).WriteDelimitedTo(replyOut);
							// NB: Unconditionally using the xfer addr w/o hostname
							replyOut.Flush();
						}
						IOUtils.CloseStream(mirrorOut);
						mirrorOut = null;
						IOUtils.CloseStream(mirrorIn);
						mirrorIn = null;
						IOUtils.CloseSocket(mirrorSock);
						mirrorSock = null;
						if (isClient)
						{
							Log.Error(datanode + ":Exception transfering block " + block + " to mirror " + mirrorNode
								 + ": " + e);
							throw;
						}
						else
						{
							Log.Info(datanode + ":Exception transfering " + block + " to mirror " + mirrorNode
								 + "- continuing without the mirror", e);
							IncrDatanodeNetworkErrors();
						}
					}
				}
				// send connect-ack to source for clients and not transfer-RBW/Finalized
				if (isClient && !isTransfer)
				{
					if (Log.IsDebugEnabled() || mirrorInStatus != DataTransferProtos.Status.Success)
					{
						Log.Info("Datanode " + targets.Length + " forwarding connect ack to upstream firstbadlink is "
							 + firstBadLink);
					}
					((DataTransferProtos.BlockOpResponseProto)DataTransferProtos.BlockOpResponseProto
						.NewBuilder().SetStatus(mirrorInStatus).SetFirstBadLink(firstBadLink).Build()).WriteDelimitedTo
						(replyOut);
					replyOut.Flush();
				}
				// receive the block and mirror to the next target
				if (blockReceiver != null)
				{
					string mirrorAddr = (mirrorSock == null) ? null : mirrorNode;
					blockReceiver.ReceiveBlock(mirrorOut, mirrorIn, replyOut, mirrorAddr, null, targets
						, false);
					// send close-ack for transfer-RBW/Finalized 
					if (isTransfer)
					{
						if (Log.IsTraceEnabled())
						{
							Log.Trace("TRANSFER: send close-ack");
						}
						WriteResponse(DataTransferProtos.Status.Success, null, replyOut);
					}
				}
				// update its generation stamp
				if (isClient && stage == BlockConstructionStage.PipelineCloseRecovery)
				{
					block.SetGenerationStamp(latestGenerationStamp);
					block.SetNumBytes(minBytesRcvd);
				}
				// if this write is for a replication request or recovering
				// a failed close for client, then confirm block. For other client-writes,
				// the block is finalized in the PacketResponder.
				if (isDatanode || stage == BlockConstructionStage.PipelineCloseRecovery)
				{
					datanode.CloseBlock(block, DataNode.EmptyDelHint, storageUuid);
					Log.Info("Received " + block + " src: " + remoteAddress + " dest: " + localAddress
						 + " of size " + block.GetNumBytes());
				}
				if (isClient)
				{
					size = block.GetNumBytes();
				}
			}
			catch (IOException ioe)
			{
				Log.Info("opWriteBlock " + block + " received exception " + ioe);
				IncrDatanodeNetworkErrors();
				throw;
			}
			finally
			{
				// close all opened streams
				IOUtils.CloseStream(mirrorOut);
				IOUtils.CloseStream(mirrorIn);
				IOUtils.CloseStream(replyOut);
				IOUtils.CloseSocket(mirrorSock);
				IOUtils.CloseStream(blockReceiver);
				blockReceiver = null;
			}
			//update metrics
			datanode.metrics.AddWriteBlockOp(Elapsed());
			datanode.metrics.IncrWritesFromClient(peer.IsLocal(), size);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TransferBlock(ExtendedBlock blk, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, string clientName, DatanodeInfo[] targets, StorageType
			[] targetStorageTypes)
		{
			previousOpClientName = clientName;
			UpdateCurrentThreadName(OP.TransferBlock + " " + blk);
			DataOutputStream @out = new DataOutputStream(GetOutputStream());
			CheckAccess(@out, true, blk, blockToken, OP.TransferBlock, BlockTokenSecretManager.AccessMode
				.Copy);
			try
			{
				datanode.TransferReplicaForPipelineRecovery(blk, targets, targetStorageTypes, clientName
					);
				WriteResponse(DataTransferProtos.Status.Success, null, @out);
			}
			catch (IOException ioe)
			{
				Log.Info("transferBlock " + blk + " received exception " + ioe);
				IncrDatanodeNetworkErrors();
				throw;
			}
			finally
			{
				IOUtils.CloseStream(@out);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private MD5Hash CalcPartialBlockChecksum(ExtendedBlock block, long requestLength, 
			DataChecksum checksum, DataInputStream checksumIn)
		{
			int bytesPerCRC = checksum.GetBytesPerChecksum();
			int csize = checksum.GetChecksumSize();
			byte[] buffer = new byte[4 * 1024];
			MessageDigest digester = MD5Hash.GetDigester();
			long remaining = requestLength / bytesPerCRC * csize;
			for (int toDigest = 0; remaining > 0; remaining -= toDigest)
			{
				toDigest = checksumIn.Read(buffer, 0, (int)Math.Min(remaining, buffer.Length));
				if (toDigest < 0)
				{
					break;
				}
				digester.Update(buffer, 0, toDigest);
			}
			int partialLength = (int)(requestLength % bytesPerCRC);
			if (partialLength > 0)
			{
				byte[] buf = new byte[partialLength];
				InputStream blockIn = datanode.data.GetBlockInputStream(block, requestLength - partialLength
					);
				try
				{
					// Get the CRC of the partialLength.
					IOUtils.ReadFully(blockIn, buf, 0, partialLength);
				}
				finally
				{
					IOUtils.CloseStream(blockIn);
				}
				checksum.Update(buf, 0, partialLength);
				byte[] partialCrc = new byte[csize];
				checksum.WriteValue(partialCrc, 0, true);
				digester.Update(partialCrc);
			}
			return new MD5Hash(digester.Digest());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void BlockChecksum(ExtendedBlock block, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken)
		{
			UpdateCurrentThreadName("Getting checksum for block " + block);
			DataOutputStream @out = new DataOutputStream(GetOutputStream());
			CheckAccess(@out, true, block, blockToken, OP.BlockChecksum, BlockTokenSecretManager.AccessMode
				.Read);
			// client side now can specify a range of the block for checksum
			long requestLength = block.GetNumBytes();
			Preconditions.CheckArgument(requestLength >= 0);
			long visibleLength = datanode.data.GetReplicaVisibleLength(block);
			bool partialBlk = requestLength < visibleLength;
			LengthInputStream metadataIn = datanode.data.GetMetaDataInputStream(block);
			DataInputStream checksumIn = new DataInputStream(new BufferedInputStream(metadataIn
				, HdfsConstants.IoFileBufferSize));
			try
			{
				//read metadata file
				BlockMetadataHeader header = BlockMetadataHeader.ReadHeader(checksumIn);
				DataChecksum checksum = header.GetChecksum();
				int csize = checksum.GetChecksumSize();
				int bytesPerCRC = checksum.GetBytesPerChecksum();
				long crcPerBlock = csize <= 0 ? 0 : (metadataIn.GetLength() - BlockMetadataHeader
					.GetHeaderSize()) / csize;
				MD5Hash md5 = partialBlk && crcPerBlock > 0 ? CalcPartialBlockChecksum(block, requestLength
					, checksum, checksumIn) : MD5Hash.Digest(checksumIn);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("block=" + block + ", bytesPerCRC=" + bytesPerCRC + ", crcPerBlock=" + 
						crcPerBlock + ", md5=" + md5);
				}
				//write reply
				((DataTransferProtos.BlockOpResponseProto)DataTransferProtos.BlockOpResponseProto
					.NewBuilder().SetStatus(DataTransferProtos.Status.Success).SetChecksumResponse(DataTransferProtos.OpBlockChecksumResponseProto
					.NewBuilder().SetBytesPerCrc(bytesPerCRC).SetCrcPerBlock(crcPerBlock).SetMd5(ByteString
					.CopyFrom(md5.GetDigest())).SetCrcType(PBHelper.Convert(checksum.GetChecksumType
					()))).Build()).WriteDelimitedTo(@out);
				@out.Flush();
			}
			catch (IOException ioe)
			{
				Log.Info("blockChecksum " + block + " received exception " + ioe);
				IncrDatanodeNetworkErrors();
				throw;
			}
			finally
			{
				IOUtils.CloseStream(@out);
				IOUtils.CloseStream(checksumIn);
				IOUtils.CloseStream(metadataIn);
			}
			//update metrics
			datanode.metrics.AddBlockChecksumOp(Elapsed());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CopyBlock(ExtendedBlock block, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken)
		{
			UpdateCurrentThreadName("Copying block " + block);
			DataOutputStream reply = GetBufferedOutputStream();
			CheckAccess(reply, true, block, blockToken, OP.CopyBlock, BlockTokenSecretManager.AccessMode
				.Copy);
			if (datanode.data.GetPinning(block))
			{
				string msg = "Not able to copy block " + block.GetBlockId() + " " + "to " + peer.
					GetRemoteAddressString() + " because it's pinned ";
				Log.Info(msg);
				SendResponse(DataTransferProtos.Status.Error, msg);
			}
			if (!dataXceiverServer.balanceThrottler.Acquire())
			{
				// not able to start
				string msg = "Not able to copy block " + block.GetBlockId() + " " + "to " + peer.
					GetRemoteAddressString() + " because threads " + "quota is exceeded.";
				Log.Info(msg);
				SendResponse(DataTransferProtos.Status.Error, msg);
				return;
			}
			BlockSender blockSender = null;
			bool isOpSuccess = true;
			try
			{
				// check if the block exists or not
				blockSender = new BlockSender(block, 0, -1, false, false, true, datanode, null, CachingStrategy
					.NewDropBehind());
				OutputStream baseStream = GetOutputStream();
				// send status first
				WriteSuccessWithChecksumInfo(blockSender, reply);
				long beginRead = Time.MonotonicNow();
				// send block content to the target
				long read = blockSender.SendBlock(reply, baseStream, dataXceiverServer.balanceThrottler
					);
				long duration = Time.MonotonicNow() - beginRead;
				datanode.metrics.IncrBytesRead((int)read);
				datanode.metrics.IncrBlocksRead();
				datanode.metrics.IncrTotalReadTime(duration);
				Log.Info("Copied " + block + " to " + peer.GetRemoteAddressString());
			}
			catch (IOException ioe)
			{
				isOpSuccess = false;
				Log.Info("opCopyBlock " + block + " received exception " + ioe);
				IncrDatanodeNetworkErrors();
				throw;
			}
			finally
			{
				dataXceiverServer.balanceThrottler.Release();
				if (isOpSuccess)
				{
					try
					{
						// send one last byte to indicate that the resource is cleaned.
						reply.WriteChar('d');
					}
					catch (IOException)
					{
					}
				}
				IOUtils.CloseStream(reply);
				IOUtils.CloseStream(blockSender);
			}
			//update metrics    
			datanode.metrics.AddCopyBlockOp(Elapsed());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReplaceBlock(ExtendedBlock block, StorageType storageType, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, string delHint, DatanodeInfo proxySource)
		{
			UpdateCurrentThreadName("Replacing block " + block + " from " + delHint);
			DataOutputStream replyOut = new DataOutputStream(GetOutputStream());
			CheckAccess(replyOut, true, block, blockToken, OP.ReplaceBlock, BlockTokenSecretManager.AccessMode
				.Replace);
			if (!dataXceiverServer.balanceThrottler.Acquire())
			{
				// not able to start
				string msg = "Not able to receive block " + block.GetBlockId() + " from " + peer.
					GetRemoteAddressString() + " because threads " + "quota is exceeded.";
				Log.Warn(msg);
				SendResponse(DataTransferProtos.Status.Error, msg);
				return;
			}
			Socket proxySock = null;
			DataOutputStream proxyOut = null;
			DataTransferProtos.Status opStatus = DataTransferProtos.Status.Success;
			string errMsg = null;
			BlockReceiver blockReceiver = null;
			DataInputStream proxyReply = null;
			bool IoeDuringCopyBlockOperation = false;
			try
			{
				// Move the block to different storage in the same datanode
				if (proxySource.Equals(datanode.GetDatanodeId()))
				{
					ReplicaInfo oldReplica = datanode.data.MoveBlockAcrossStorage(block, storageType);
					if (oldReplica != null)
					{
						Log.Info("Moved " + block + " from StorageType " + oldReplica.GetVolume().GetStorageType
							() + " to " + storageType);
					}
				}
				else
				{
					block.SetNumBytes(dataXceiverServer.estimateBlockSize);
					// get the output stream to the proxy
					string dnAddr = proxySource.GetXferAddr(connectToDnViaHostname);
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Connecting to datanode " + dnAddr);
					}
					IPEndPoint proxyAddr = NetUtils.CreateSocketAddr(dnAddr);
					proxySock = datanode.NewSocket();
					NetUtils.Connect(proxySock, proxyAddr, dnConf.socketTimeout);
					proxySock.ReceiveTimeout = dnConf.socketTimeout;
					OutputStream unbufProxyOut = NetUtils.GetOutputStream(proxySock, dnConf.socketWriteTimeout
						);
					InputStream unbufProxyIn = NetUtils.GetInputStream(proxySock);
					DataEncryptionKeyFactory keyFactory = datanode.GetDataEncryptionKeyFactoryForBlock
						(block);
					IOStreamPair saslStreams = datanode.saslClient.SocketSend(proxySock, unbufProxyOut
						, unbufProxyIn, keyFactory, blockToken, proxySource);
					unbufProxyOut = saslStreams.@out;
					unbufProxyIn = saslStreams.@in;
					proxyOut = new DataOutputStream(new BufferedOutputStream(unbufProxyOut, HdfsConstants
						.SmallBufferSize));
					proxyReply = new DataInputStream(new BufferedInputStream(unbufProxyIn, HdfsConstants
						.IoFileBufferSize));
					/* send request to the proxy */
					IoeDuringCopyBlockOperation = true;
					new Sender(proxyOut).CopyBlock(block, blockToken);
					IoeDuringCopyBlockOperation = false;
					// receive the response from the proxy
					DataTransferProtos.BlockOpResponseProto copyResponse = DataTransferProtos.BlockOpResponseProto
						.ParseFrom(PBHelper.VintPrefixed(proxyReply));
					string logInfo = "copy block " + block + " from " + proxySock.RemoteEndPoint;
					DataTransferProtoUtil.CheckBlockOpStatus(copyResponse, logInfo);
					// get checksum info about the block we're copying
					DataTransferProtos.ReadOpChecksumInfoProto checksumInfo = copyResponse.GetReadOpChecksumInfo
						();
					DataChecksum remoteChecksum = DataTransferProtoUtil.FromProto(checksumInfo.GetChecksum
						());
					// open a block receiver and check if the block does not exist
					blockReceiver = new BlockReceiver(block, storageType, proxyReply, proxySock.RemoteEndPoint
						.ToString(), proxySock.GetLocalSocketAddress().ToString(), null, 0, 0, 0, string.Empty
						, null, datanode, remoteChecksum, CachingStrategy.NewDropBehind(), false, false);
					// receive a block
					blockReceiver.ReceiveBlock(null, null, replyOut, null, dataXceiverServer.balanceThrottler
						, null, true);
					// notify name node
					datanode.NotifyNamenodeReceivedBlock(block, delHint, blockReceiver.GetStorageUuid
						());
					Log.Info("Moved " + block + " from " + peer.GetRemoteAddressString() + ", delHint="
						 + delHint);
				}
			}
			catch (IOException ioe)
			{
				opStatus = DataTransferProtos.Status.Error;
				errMsg = "opReplaceBlock " + block + " received exception " + ioe;
				Log.Info(errMsg);
				if (!IoeDuringCopyBlockOperation)
				{
					// Don't double count IO errors
					IncrDatanodeNetworkErrors();
				}
				throw;
			}
			finally
			{
				// receive the last byte that indicates the proxy released its thread resource
				if (opStatus == DataTransferProtos.Status.Success && proxyReply != null)
				{
					try
					{
						proxyReply.ReadChar();
					}
					catch (IOException)
					{
					}
				}
				// now release the thread resource
				dataXceiverServer.balanceThrottler.Release();
				// send response back
				try
				{
					SendResponse(opStatus, errMsg);
				}
				catch (IOException)
				{
					Log.Warn("Error writing reply back to " + peer.GetRemoteAddressString());
					IncrDatanodeNetworkErrors();
				}
				IOUtils.CloseStream(proxyOut);
				IOUtils.CloseStream(blockReceiver);
				IOUtils.CloseStream(proxyReply);
				IOUtils.CloseStream(replyOut);
			}
			//update metrics
			datanode.metrics.AddReplaceBlockOp(Elapsed());
		}

		/// <summary>Separated for testing.</summary>
		/// <returns/>
		internal virtual DataOutputStream GetBufferedOutputStream()
		{
			return new DataOutputStream(new BufferedOutputStream(GetOutputStream(), HdfsConstants
				.SmallBufferSize));
		}

		private long Elapsed()
		{
			return Time.MonotonicNow() - opStartTime;
		}

		/// <summary>Utility function for sending a response.</summary>
		/// <param name="status">status message to write</param>
		/// <param name="message">message to send to the client or other DN</param>
		/// <exception cref="System.IO.IOException"/>
		private void SendResponse(DataTransferProtos.Status status, string message)
		{
			WriteResponse(status, message, GetOutputStream());
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteResponse(DataTransferProtos.Status status, string message
			, OutputStream @out)
		{
			DataTransferProtos.BlockOpResponseProto.Builder response = DataTransferProtos.BlockOpResponseProto
				.NewBuilder().SetStatus(status);
			if (message != null)
			{
				response.SetMessage(message);
			}
			((DataTransferProtos.BlockOpResponseProto)response.Build()).WriteDelimitedTo(@out
				);
			@out.Flush();
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteSuccessWithChecksumInfo(BlockSender blockSender, DataOutputStream
			 @out)
		{
			DataTransferProtos.ReadOpChecksumInfoProto ckInfo = ((DataTransferProtos.ReadOpChecksumInfoProto
				)DataTransferProtos.ReadOpChecksumInfoProto.NewBuilder().SetChecksum(DataTransferProtoUtil
				.ToProto(blockSender.GetChecksum())).SetChunkOffset(blockSender.GetOffset()).Build
				());
			DataTransferProtos.BlockOpResponseProto response = ((DataTransferProtos.BlockOpResponseProto
				)DataTransferProtos.BlockOpResponseProto.NewBuilder().SetStatus(DataTransferProtos.Status
				.Success).SetReadOpChecksumInfo(ckInfo).Build());
			response.WriteDelimitedTo(@out);
			@out.Flush();
		}

		private void IncrDatanodeNetworkErrors()
		{
			datanode.IncrDatanodeNetworkErrors(remoteAddressWithoutPort);
		}

		/// <summary>Wait until the BP is registered, upto the configured amount of time.</summary>
		/// <remarks>
		/// Wait until the BP is registered, upto the configured amount of time.
		/// Throws an exception if times out, which should fail the client request.
		/// </remarks>
		/// <param name="the">requested block</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckAndWaitForBP(ExtendedBlock block)
		{
			string bpId = block.GetBlockPoolId();
			// The registration is only missing in relatively short time window.
			// Optimistically perform this first.
			try
			{
				datanode.GetDNRegistrationForBP(bpId);
				return;
			}
			catch (IOException)
			{
			}
			// not registered
			// retry
			long bpReadyTimeout = dnConf.GetBpReadyTimeout();
			StopWatch sw = new StopWatch();
			sw.Start();
			while (sw.Now(TimeUnit.Seconds) <= bpReadyTimeout)
			{
				try
				{
					datanode.GetDNRegistrationForBP(bpId);
					return;
				}
				catch (IOException)
				{
				}
				// not registered
				// sleep before trying again
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
					throw new IOException("Interrupted while serving request. Aborting.");
				}
			}
			// failed to obtain registration.
			throw new IOException("Not ready to serve the block pool, " + bpId + ".");
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckAccess(OutputStream @out, bool reply, ExtendedBlock blk, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> t, OP op, BlockTokenSecretManager.AccessMode mode)
		{
			CheckAndWaitForBP(blk);
			if (datanode.isBlockTokenEnabled)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Checking block access token for block '" + blk.GetBlockId() + "' with mode '"
						 + mode + "'");
				}
				try
				{
					datanode.blockPoolTokenSecretManager.CheckAccess(t, null, blk, mode);
				}
				catch (SecretManager.InvalidToken e)
				{
					try
					{
						if (reply)
						{
							DataTransferProtos.BlockOpResponseProto.Builder resp = DataTransferProtos.BlockOpResponseProto
								.NewBuilder().SetStatus(DataTransferProtos.Status.ErrorAccessToken);
							if (mode == BlockTokenSecretManager.AccessMode.Write)
							{
								DatanodeRegistration dnR = datanode.GetDNRegistrationForBP(blk.GetBlockPoolId());
								// NB: Unconditionally using the xfer addr w/o hostname
								resp.SetFirstBadLink(dnR.GetXferAddr());
							}
							((DataTransferProtos.BlockOpResponseProto)resp.Build()).WriteDelimitedTo(@out);
							@out.Flush();
						}
						Log.Warn("Block token verification failed: op=" + op + ", remoteAddress=" + remoteAddress
							 + ", message=" + e.GetLocalizedMessage());
						throw;
					}
					finally
					{
						IOUtils.CloseStream(@out);
					}
				}
			}
		}
	}
}
