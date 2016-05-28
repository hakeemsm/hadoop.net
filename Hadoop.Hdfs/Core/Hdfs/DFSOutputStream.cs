using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Cache;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>DFSOutputStream creates files from a stream of bytes.</summary>
	/// <remarks>
	/// DFSOutputStream creates files from a stream of bytes.
	/// The client application writes data that is cached internally by
	/// this stream. Data is broken up into packets, each packet is
	/// typically 64K in size. A packet comprises of chunks. Each chunk
	/// is typically 512 bytes and has an associated checksum with it.
	/// When a client application fills up the currentPacket, it is
	/// enqueued into dataQueue.  The DataStreamer thread picks up
	/// packets from the dataQueue, sends it to the first datanode in
	/// the pipeline and moves it from the dataQueue to the ackQueue.
	/// The ResponseProcessor receives acks from the datanodes. When an
	/// successful ack for a packet is received from all datanodes, the
	/// ResponseProcessor removes the corresponding packet from the
	/// ackQueue.
	/// In case of error, all outstanding packets and moved from
	/// ackQueue. A new pipeline is setup by eliminating the bad
	/// datanode from the original pipeline. The DataStreamer now
	/// starts sending packets from the dataQueue.
	/// </remarks>
	public class DFSOutputStream : FSOutputSummer, Syncable, CanSetDropBehind
	{
		private readonly long dfsclientSlowLogThresholdMs;

		/// <summary>
		/// Number of times to retry creating a file when there are transient
		/// errors (typically related to encryption zones and KeyProvider operations).
		/// </summary>
		[VisibleForTesting]
		internal const int CreateRetryCount = 10;

		[VisibleForTesting]
		internal static CryptoProtocolVersion[] SupportedCryptoVersions = CryptoProtocolVersion
			.Supported();

		private readonly DFSClient dfsClient;

		private readonly ByteArrayManager byteArrayManager;

		private Socket s;

		private volatile bool closed = false;

		private string src;

		private readonly long fileId;

		private readonly long blockSize;

		/// <summary>Only for DataTransferProtocol.writeBlock(..)</summary>
		private readonly DataChecksum checksum4WriteBlock;

		private readonly int bytesPerChecksum;

		private readonly List<DFSPacket> dataQueue = new List<DFSPacket>();

		private readonly List<DFSPacket> ackQueue = new List<DFSPacket>();

		private DFSPacket currentPacket = null;

		private DFSOutputStream.DataStreamer streamer;

		private long currentSeqno = 0;

		private long lastQueuedSeqno = -1;

		private long lastAckedSeqno = -1;

		private long bytesCurBlock = 0;

		private int packetSize = 0;

		private int chunksPerPacket = 0;

		private readonly AtomicReference<IOException> lastException = new AtomicReference
			<IOException>();

		private long artificialSlowdown = 0;

		private long lastFlushOffset = 0;

		private readonly AtomicBoolean persistBlocks = new AtomicBoolean(false);

		private volatile bool appendChunk = false;

		private long initialFileSize = 0;

		private readonly Progressable progress;

		private readonly short blockReplication;

		private bool shouldSyncBlock = false;

		private readonly AtomicReference<CachingStrategy> cachingStrategy;

		private bool failPacket = false;

		private FileEncryptionInfo fileEncryptionInfo;

		private static readonly BlockStoragePolicySuite blockStoragePolicySuite = BlockStoragePolicySuite
			.CreateDefaultSuite();

		// closed is accessed by different threads under different locks.
		// both dataQueue and ackQueue are protected by dataQueue lock
		// bytes written in current block
		// write packet size, not including the header.
		// offset when flush was invoked
		//persist blocks on namenode
		// appending to existing partial block
		// at time of file open
		// replication factor of file
		// force blocks to disk upon close
		/// <summary>
		/// Use
		/// <see cref="Org.Apache.Hadoop.Hdfs.Util.ByteArrayManager"/>
		/// to create buffer for non-heartbeat packets.
		/// </summary>
		/// <exception cref="System.Threading.ThreadInterruptedException"/>
		private DFSPacket CreatePacket(int packetSize, int chunksPerPkt, long offsetInBlock
			, long seqno, bool lastPacketInBlock)
		{
			byte[] buf;
			int bufferSize = PacketHeader.PktMaxHeaderLen + packetSize;
			try
			{
				buf = byteArrayManager.NewByteArray(bufferSize);
			}
			catch (Exception ie)
			{
				ThreadInterruptedException iioe = new ThreadInterruptedException("seqno=" + seqno
					);
				Sharpen.Extensions.InitCause(iioe, ie);
				throw iioe;
			}
			return new DFSPacket(buf, chunksPerPkt, offsetInBlock, seqno, GetChecksumSize(), 
				lastPacketInBlock);
		}

		/// <summary>
		/// For heartbeat packets, create buffer directly by new byte[]
		/// since heartbeats should not be blocked.
		/// </summary>
		/// <exception cref="System.Threading.ThreadInterruptedException"/>
		private DFSPacket CreateHeartbeatPacket()
		{
			byte[] buf = new byte[PacketHeader.PktMaxHeaderLen];
			return new DFSPacket(buf, 0, 0, DFSPacket.HeartBeatSeqno, GetChecksumSize(), false
				);
		}

		internal class DataStreamer : Daemon
		{
			private volatile bool streamerClosed = false;

			private volatile ExtendedBlock block;

			private Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> accessToken;

			private DataOutputStream blockStream;

			private DataInputStream blockReplyStream;

			private DFSOutputStream.DataStreamer.ResponseProcessor response = null;

			private volatile DatanodeInfo[] nodes = null;

			private volatile StorageType[] storageTypes = null;

			private volatile string[] storageIDs = null;

			private sealed class _RemovalListener_242 : RemovalListener<DatanodeInfo, DatanodeInfo
				>
			{
				public _RemovalListener_242()
				{
				}

				//
				// The DataStreamer class is responsible for sending data packets to the
				// datanodes in the pipeline. It retrieves a new blockid and block locations
				// from the namenode, and starts streaming packets to the pipeline of
				// Datanodes. Every packet has a sequence number associated with
				// it. When all the packets for a block are sent out and acks for each
				// if them are received, the DataStreamer closes the current block.
				//
				// its length is number of bytes acked
				// list of targets for current block
				public void OnRemoval(RemovalNotification<DatanodeInfo, DatanodeInfo> notification
					)
				{
					DFSClient.Log.Info("Removing node " + notification.Key + " from the excluded nodes list"
						);
				}
			}

			private sealed class _CacheLoader_250 : CacheLoader<DatanodeInfo, DatanodeInfo>
			{
				public _CacheLoader_250()
				{
				}

				/// <exception cref="System.Exception"/>
				public override DatanodeInfo Load(DatanodeInfo key)
				{
					return key;
				}
			}

			private readonly LoadingCache<DatanodeInfo, DatanodeInfo> excludedNodes = CacheBuilder
				.NewBuilder().ExpireAfterWrite(this._enclosing.dfsClient.GetConf().excludedNodesCacheExpiry
				, TimeUnit.Milliseconds).RemovalListener(new _RemovalListener_242()).Build(new _CacheLoader_250
				());

			private string[] favoredNodes;

			internal volatile bool hasError = false;

			internal volatile int errorIndex = -1;

			internal AtomicInteger restartingNodeIndex = new AtomicInteger(-1);

			private long restartDeadline = 0;

			private BlockConstructionStage stage;

			private long bytesSent = 0;

			private readonly bool isLazyPersistFile;

			/// <summary>Nodes have been used in the pipeline before and have failed.</summary>
			private readonly IList<DatanodeInfo> failed = new AList<DatanodeInfo>();

			/// <summary>The last ack sequence number before pipeline failure.</summary>
			private long lastAckedSeqnoBeforeFailure = -1;

			private int pipelineRecoveryCount = 0;

			/// <summary>Has the current block been hflushed?</summary>
			private bool isHflushed = false;

			/// <summary>Append on an existing block?</summary>
			private readonly bool isAppend;

			private DataStreamer(DFSOutputStream _enclosing, HdfsFileStatus stat, ExtendedBlock
				 block)
			{
				this._enclosing = _enclosing;
				// Restarting node index
				// Deadline of DN restart
				// block construction stage
				// number of bytes that've been sent
				this.isAppend = false;
				this.isLazyPersistFile = DFSOutputStream.IsLazyPersist(stat);
				this.block = block;
				this.stage = BlockConstructionStage.PipelineSetupCreate;
			}

			/// <summary>Construct a data streamer for appending to the last partial block</summary>
			/// <param name="lastBlock">last block of the file to be appended</param>
			/// <param name="stat">status of the file to be appended</param>
			/// <param name="bytesPerChecksum">number of bytes per checksum</param>
			/// <exception cref="System.IO.IOException">if error occurs</exception>
			private DataStreamer(DFSOutputStream _enclosing, LocatedBlock lastBlock, HdfsFileStatus
				 stat, int bytesPerChecksum)
			{
				this._enclosing = _enclosing;
				this.isAppend = true;
				this.stage = BlockConstructionStage.PipelineSetupAppend;
				this.block = lastBlock.GetBlock();
				this.bytesSent = this.block.GetNumBytes();
				this.accessToken = lastBlock.GetBlockToken();
				this.isLazyPersistFile = DFSOutputStream.IsLazyPersist(stat);
				long usedInLastBlock = stat.GetLen() % this._enclosing.blockSize;
				int freeInLastBlock = (int)(this._enclosing.blockSize - usedInLastBlock);
				// calculate the amount of free space in the pre-existing 
				// last crc chunk
				int usedInCksum = (int)(stat.GetLen() % bytesPerChecksum);
				int freeInCksum = bytesPerChecksum - usedInCksum;
				// if there is space in the last block, then we have to 
				// append to that block
				if (freeInLastBlock == this._enclosing.blockSize)
				{
					throw new IOException("The last block for file " + this._enclosing.src + " is full."
						);
				}
				if (usedInCksum > 0 && freeInCksum > 0)
				{
					// if there is space in the last partial chunk, then 
					// setup in such a way that the next packet will have only 
					// one chunk that fills up the partial chunk.
					//
					this._enclosing.ComputePacketChunkSize(0, freeInCksum);
					this._enclosing.SetChecksumBufSize(freeInCksum);
					this._enclosing.appendChunk = true;
				}
				else
				{
					// if the remaining space in the block is smaller than 
					// that expected size of of a packet, then create 
					// smaller size packet.
					//
					this._enclosing.ComputePacketChunkSize(Math.Min(this._enclosing.dfsClient.GetConf
						().writePacketSize, freeInLastBlock), bytesPerChecksum);
				}
				// setup pipeline to append to the last block XXX retries??
				this.SetPipeline(lastBlock);
				this.errorIndex = -1;
				// no errors yet.
				if (this.nodes.Length < 1)
				{
					throw new IOException("Unable to retrieve blocks locations " + " for last block "
						 + this.block + "of file " + this._enclosing.src);
				}
			}

			private void SetPipeline(LocatedBlock lb)
			{
				this.SetPipeline(lb.GetLocations(), lb.GetStorageTypes(), lb.GetStorageIDs());
			}

			private void SetPipeline(DatanodeInfo[] nodes, StorageType[] storageTypes, string
				[] storageIDs)
			{
				this.nodes = nodes;
				this.storageTypes = storageTypes;
				this.storageIDs = storageIDs;
			}

			private void SetFavoredNodes(string[] favoredNodes)
			{
				this.favoredNodes = favoredNodes;
			}

			/// <summary>Initialize for data streaming</summary>
			private void InitDataStreaming()
			{
				this.SetName("DataStreamer for file " + this._enclosing.src + " block " + this.block
					);
				this.response = new DFSOutputStream.DataStreamer.ResponseProcessor(this, this.nodes
					);
				this.response.Start();
				this.stage = BlockConstructionStage.DataStreaming;
			}

			private void EndBlock()
			{
				if (DFSClient.Log.IsDebugEnabled())
				{
					DFSClient.Log.Debug("Closing old block " + this.block);
				}
				this.SetName("DataStreamer for file " + this._enclosing.src);
				this.CloseResponder();
				this.CloseStream();
				this.SetPipeline(null, null, null);
				this.stage = BlockConstructionStage.PipelineSetupCreate;
			}

			/*
			* streamer thread is the only thread that opens streams to datanode,
			* and closes them. Any error recovery is also done by this thread.
			*/
			public override void Run()
			{
				long lastPacket = Time.MonotonicNow();
				TraceScope scope = NullScope.Instance;
				while (!this.streamerClosed && this._enclosing.dfsClient.clientRunning)
				{
					// if the Responder encountered an error, shutdown Responder
					if (this.hasError && this.response != null)
					{
						try
						{
							this.response.Close();
							this.response.Join();
							this.response = null;
						}
						catch (Exception e)
						{
							DFSClient.Log.Warn("Caught exception ", e);
						}
					}
					DFSPacket one;
					try
					{
						// process datanode IO errors if any
						bool doSleep = false;
						if (this.hasError && (this.errorIndex >= 0 || this.restartingNodeIndex.Get() >= 0
							))
						{
							doSleep = this.ProcessDatanodeError();
						}
						lock (this._enclosing.dataQueue)
						{
							// wait for a packet to be sent.
							long now = Time.MonotonicNow();
							while ((!this.streamerClosed && !this.hasError && this._enclosing.dfsClient.clientRunning
								 && this._enclosing.dataQueue.Count == 0 && (this.stage != BlockConstructionStage
								.DataStreaming || this.stage == BlockConstructionStage.DataStreaming && now - lastPacket
								 < this._enclosing.dfsClient.GetConf().socketTimeout / 2)) || doSleep)
							{
								long timeout = this._enclosing.dfsClient.GetConf().socketTimeout / 2 - (now - lastPacket
									);
								timeout = timeout <= 0 ? 1000 : timeout;
								timeout = (this.stage == BlockConstructionStage.DataStreaming) ? timeout : 1000;
								try
								{
									Sharpen.Runtime.Wait(this._enclosing.dataQueue, timeout);
								}
								catch (Exception e)
								{
									DFSClient.Log.Warn("Caught exception ", e);
								}
								doSleep = false;
								now = Time.MonotonicNow();
							}
							if (this.streamerClosed || this.hasError || !this._enclosing.dfsClient.clientRunning)
							{
								continue;
							}
							// get packet to be sent.
							if (this._enclosing.dataQueue.IsEmpty())
							{
								one = this._enclosing.CreateHeartbeatPacket();
								System.Diagnostics.Debug.Assert(one != null);
							}
							else
							{
								one = this._enclosing.dataQueue.GetFirst();
								// regular data packet
								long[] parents = one.GetTraceParents();
								if (parents.Length > 0)
								{
									scope = Trace.StartSpan("dataStreamer", new TraceInfo(0, parents[0]));
								}
							}
						}
						// TODO: use setParents API once it's available from HTrace 3.2
						//                scope = Trace.startSpan("dataStreamer", Sampler.ALWAYS);
						//                scope.getSpan().setParents(parents);
						// get new block from namenode.
						if (this.stage == BlockConstructionStage.PipelineSetupCreate)
						{
							if (DFSClient.Log.IsDebugEnabled())
							{
								DFSClient.Log.Debug("Allocating new block");
							}
							this.SetPipeline(this.NextBlockOutputStream());
							this.InitDataStreaming();
						}
						else
						{
							if (this.stage == BlockConstructionStage.PipelineSetupAppend)
							{
								if (DFSClient.Log.IsDebugEnabled())
								{
									DFSClient.Log.Debug("Append to block " + this.block);
								}
								this.SetupPipelineForAppendOrRecovery();
								this.InitDataStreaming();
							}
						}
						long lastByteOffsetInBlock = one.GetLastByteOffsetBlock();
						if (lastByteOffsetInBlock > this._enclosing.blockSize)
						{
							throw new IOException("BlockSize " + this._enclosing.blockSize + " is smaller than data size. "
								 + " Offset of packet in block " + lastByteOffsetInBlock + " Aborting file " + this
								._enclosing.src);
						}
						if (one.IsLastPacketInBlock())
						{
							// wait for all data packets have been successfully acked
							lock (this._enclosing.dataQueue)
							{
								while (!this.streamerClosed && !this.hasError && this._enclosing.ackQueue.Count !=
									 0 && this._enclosing.dfsClient.clientRunning)
								{
									try
									{
										// wait for acks to arrive from datanodes
										Sharpen.Runtime.Wait(this._enclosing.dataQueue, 1000);
									}
									catch (Exception e)
									{
										DFSClient.Log.Warn("Caught exception ", e);
									}
								}
							}
							if (this.streamerClosed || this.hasError || !this._enclosing.dfsClient.clientRunning)
							{
								continue;
							}
							this.stage = BlockConstructionStage.PipelineClose;
						}
						// send the packet
						Span span = null;
						lock (this._enclosing.dataQueue)
						{
							// move packet from dataQueue to ackQueue
							if (!one.IsHeartbeatPacket())
							{
								span = scope.Detach();
								one.SetTraceSpan(span);
								this._enclosing.dataQueue.RemoveFirst();
								this._enclosing.ackQueue.AddLast(one);
								Sharpen.Runtime.NotifyAll(this._enclosing.dataQueue);
							}
						}
						if (DFSClient.Log.IsDebugEnabled())
						{
							DFSClient.Log.Debug("DataStreamer block " + this.block + " sending packet " + one
								);
						}
						// write out data to remote datanode
						TraceScope writeScope = Trace.StartSpan("writeTo", span);
						try
						{
							one.WriteTo(this.blockStream);
							this.blockStream.Flush();
						}
						catch (IOException e)
						{
							// HDFS-3398 treat primary DN is down since client is unable to 
							// write to primary DN. If a failed or restarting node has already
							// been recorded by the responder, the following call will have no 
							// effect. Pipeline recovery can handle only one node error at a
							// time. If the primary node fails again during the recovery, it
							// will be taken out then.
							this.TryMarkPrimaryDatanodeFailed();
							throw;
						}
						finally
						{
							writeScope.Close();
						}
						lastPacket = Time.MonotonicNow();
						// update bytesSent
						long tmpBytesSent = one.GetLastByteOffsetBlock();
						if (this.bytesSent < tmpBytesSent)
						{
							this.bytesSent = tmpBytesSent;
						}
						if (this.streamerClosed || this.hasError || !this._enclosing.dfsClient.clientRunning)
						{
							continue;
						}
						// Is this block full?
						if (one.IsLastPacketInBlock())
						{
							// wait for the close packet has been acked
							lock (this._enclosing.dataQueue)
							{
								while (!this.streamerClosed && !this.hasError && this._enclosing.ackQueue.Count !=
									 0 && this._enclosing.dfsClient.clientRunning)
								{
									Sharpen.Runtime.Wait(this._enclosing.dataQueue, 1000);
								}
							}
							// wait for acks to arrive from datanodes
							if (this.streamerClosed || this.hasError || !this._enclosing.dfsClient.clientRunning)
							{
								continue;
							}
							this.EndBlock();
						}
						if (this._enclosing.progress != null)
						{
							this._enclosing.progress.Progress();
						}
						// This is used by unit test to trigger race conditions.
						if (this._enclosing.artificialSlowdown != 0 && this._enclosing.dfsClient.clientRunning)
						{
							Sharpen.Thread.Sleep(this._enclosing.artificialSlowdown);
						}
					}
					catch (Exception e)
					{
						// Log warning if there was a real error.
						if (this.restartingNodeIndex.Get() == -1)
						{
							DFSClient.Log.Warn("DataStreamer Exception", e);
						}
						if (e is IOException)
						{
							this.SetLastException((IOException)e);
						}
						else
						{
							this.SetLastException(new IOException("DataStreamer Exception: ", e));
						}
						this.hasError = true;
						if (this.errorIndex == -1 && this.restartingNodeIndex.Get() == -1)
						{
							// Not a datanode issue
							this.streamerClosed = true;
						}
					}
					finally
					{
						scope.Close();
					}
				}
				this.CloseInternal();
			}

			private void CloseInternal()
			{
				this.CloseResponder();
				// close and join
				this.CloseStream();
				this.streamerClosed = true;
				this._enclosing.SetClosed();
				lock (this._enclosing.dataQueue)
				{
					Sharpen.Runtime.NotifyAll(this._enclosing.dataQueue);
				}
			}

			/*
			* close both streamer and DFSOutputStream, should be called only
			* by an external thread and only after all data to be sent has
			* been flushed to datanode.
			*
			* Interrupt this data streamer if force is true
			*
			* @param force if this data stream is forced to be closed
			*/
			internal virtual void Close(bool force)
			{
				this.streamerClosed = true;
				lock (this._enclosing.dataQueue)
				{
					Sharpen.Runtime.NotifyAll(this._enclosing.dataQueue);
				}
				if (force)
				{
					this.Interrupt();
				}
			}

			private void CloseResponder()
			{
				if (this.response != null)
				{
					try
					{
						this.response.Close();
						this.response.Join();
					}
					catch (Exception e)
					{
						DFSClient.Log.Warn("Caught exception ", e);
					}
					finally
					{
						this.response = null;
					}
				}
			}

			private void CloseStream()
			{
				if (this.blockStream != null)
				{
					try
					{
						this.blockStream.Close();
					}
					catch (IOException e)
					{
						this.SetLastException(e);
					}
					finally
					{
						this.blockStream = null;
					}
				}
				if (this.blockReplyStream != null)
				{
					try
					{
						this.blockReplyStream.Close();
					}
					catch (IOException e)
					{
						this.SetLastException(e);
					}
					finally
					{
						this.blockReplyStream = null;
					}
				}
				if (null != this._enclosing.s)
				{
					try
					{
						this._enclosing.s.Close();
					}
					catch (IOException e)
					{
						this.SetLastException(e);
					}
					finally
					{
						this._enclosing.s = null;
					}
				}
			}

			// The following synchronized methods are used whenever 
			// errorIndex or restartingNodeIndex is set. This is because
			// check & set needs to be atomic. Simply reading variables
			// does not require a synchronization. When responder is
			// not running (e.g. during pipeline recovery), there is no
			// need to use these methods.
			/// <summary>Set the error node index.</summary>
			/// <remarks>Set the error node index. Called by responder</remarks>
			internal virtual void SetErrorIndex(int idx)
			{
				lock (this)
				{
					this.errorIndex = idx;
				}
			}

			/// <summary>Set the restarting node index.</summary>
			/// <remarks>Set the restarting node index. Called by responder</remarks>
			internal virtual void SetRestartingNodeIndex(int idx)
			{
				lock (this)
				{
					this.restartingNodeIndex.Set(idx);
					// If the data streamer has already set the primary node
					// bad, clear it. It is likely that the write failed due to
					// the DN shutdown. Even if it was a real failure, the pipeline
					// recovery will take care of it.
					this.errorIndex = -1;
				}
			}

			/// <summary>
			/// This method is used when no explicit error report was received,
			/// but something failed.
			/// </summary>
			/// <remarks>
			/// This method is used when no explicit error report was received,
			/// but something failed. When the primary node is a suspect or
			/// unsure about the cause, the primary node is marked as failed.
			/// </remarks>
			internal virtual void TryMarkPrimaryDatanodeFailed()
			{
				lock (this)
				{
					// There should be no existing error and no ongoing restart.
					if ((this.errorIndex == -1) && (this.restartingNodeIndex.Get() == -1))
					{
						this.errorIndex = 0;
					}
				}
			}

			/// <summary>Examine whether it is worth waiting for a node to restart.</summary>
			/// <param name="index">the node index</param>
			internal virtual bool ShouldWaitForRestart(int index)
			{
				// Only one node in the pipeline.
				if (this.nodes.Length == 1)
				{
					return true;
				}
				// Is it a local node?
				IPAddress addr = null;
				try
				{
					addr = Sharpen.Extensions.GetAddressByName(this.nodes[index].GetIpAddr());
				}
				catch (UnknownHostException)
				{
					// we are passing an ip address. this should not happen.
					System.Diagnostics.Debug.Assert(false);
				}
				if (addr != null && NetUtils.IsLocalAddress(addr))
				{
					return true;
				}
				return false;
			}

			private class ResponseProcessor : Daemon
			{
				private volatile bool responderClosed = false;

				private DatanodeInfo[] targets = null;

				private bool isLastPacketInBlock = false;

				internal ResponseProcessor(DataStreamer _enclosing, DatanodeInfo[] targets)
				{
					this._enclosing = _enclosing;
					//
					// Processes responses from the datanodes.  A packet is removed
					// from the ackQueue when its response arrives.
					//
					this.targets = targets;
				}

				public override void Run()
				{
					this.SetName("ResponseProcessor for block " + this._enclosing.block);
					PipelineAck ack = new PipelineAck();
					TraceScope scope = NullScope.Instance;
					while (!this.responderClosed && this._enclosing._enclosing.dfsClient.clientRunning
						 && !this.isLastPacketInBlock)
					{
						// process responses from datanodes.
						try
						{
							// read an ack from the pipeline
							long begin = Time.MonotonicNow();
							ack.ReadFields(this._enclosing.blockReplyStream);
							long duration = Time.MonotonicNow() - begin;
							if (duration > this._enclosing._enclosing.dfsclientSlowLogThresholdMs && ack.GetSeqno
								() != DFSPacket.HeartBeatSeqno)
							{
								DFSClient.Log.Warn("Slow ReadProcessor read fields took " + duration + "ms (threshold="
									 + this._enclosing._enclosing.dfsclientSlowLogThresholdMs + "ms); ack: " + ack +
									 ", targets: " + Arrays.AsList(this.targets));
							}
							else
							{
								if (DFSClient.Log.IsDebugEnabled())
								{
									DFSClient.Log.Debug("DFSClient " + ack);
								}
							}
							long seqno = ack.GetSeqno();
							// processes response status from datanodes.
							for (int i = ack.GetNumOfReplies() - 1; i >= 0 && this._enclosing._enclosing.dfsClient
								.clientRunning; i--)
							{
								DataTransferProtos.Status reply = PipelineAck.GetStatusFromHeader(ack.GetHeaderFlag
									(i));
								// Restart will not be treated differently unless it is
								// the local node or the only one in the pipeline.
								if (PipelineAck.IsRestartOOBStatus(reply) && this._enclosing.ShouldWaitForRestart
									(i))
								{
									this._enclosing.restartDeadline = this._enclosing._enclosing.dfsClient.GetConf().
										datanodeRestartTimeout + Time.MonotonicNow();
									this._enclosing.SetRestartingNodeIndex(i);
									string message = "A datanode is restarting: " + this.targets[i];
									DFSClient.Log.Info(message);
									throw new IOException(message);
								}
								// node error
								if (reply != DataTransferProtos.Status.Success)
								{
									this._enclosing.SetErrorIndex(i);
									// first bad datanode
									throw new IOException("Bad response " + reply + " for block " + this._enclosing.block
										 + " from datanode " + this.targets[i]);
								}
							}
							System.Diagnostics.Debug.Assert(seqno != PipelineAck.UnkownSeqno, "Ack for unknown seqno should be a failed ack: "
								 + ack);
							if (seqno == DFSPacket.HeartBeatSeqno)
							{
								// a heartbeat ack
								continue;
							}
							// a success ack for a data packet
							DFSPacket one;
							lock (this._enclosing._enclosing.dataQueue)
							{
								one = this._enclosing._enclosing.ackQueue.GetFirst();
							}
							if (one.GetSeqno() != seqno)
							{
								throw new IOException("ResponseProcessor: Expecting seqno " + " for block " + this
									._enclosing.block + one.GetSeqno() + " but received " + seqno);
							}
							this.isLastPacketInBlock = one.IsLastPacketInBlock();
							// Fail the packet write for testing in order to force a
							// pipeline recovery.
							if (DFSClientFaultInjector.Get().FailPacket() && this.isLastPacketInBlock)
							{
								this._enclosing._enclosing.failPacket = true;
								throw new IOException("Failing the last packet for testing.");
							}
							// update bytesAcked
							this._enclosing.block.SetNumBytes(one.GetLastByteOffsetBlock());
							lock (this._enclosing._enclosing.dataQueue)
							{
								scope = Trace.ContinueSpan(one.GetTraceSpan());
								one.SetTraceSpan(null);
								this._enclosing._enclosing.lastAckedSeqno = seqno;
								this._enclosing._enclosing.ackQueue.RemoveFirst();
								Sharpen.Runtime.NotifyAll(this._enclosing._enclosing.dataQueue);
								one.ReleaseBuffer(this._enclosing._enclosing.byteArrayManager);
							}
						}
						catch (Exception e)
						{
							if (!this.responderClosed)
							{
								if (e is IOException)
								{
									this._enclosing.SetLastException((IOException)e);
								}
								this._enclosing.hasError = true;
								// If no explicit error report was received, mark the primary
								// node as failed.
								this._enclosing.TryMarkPrimaryDatanodeFailed();
								lock (this._enclosing._enclosing.dataQueue)
								{
									Sharpen.Runtime.NotifyAll(this._enclosing._enclosing.dataQueue);
								}
								if (this._enclosing.restartingNodeIndex.Get() == -1)
								{
									DFSClient.Log.Warn("DFSOutputStream ResponseProcessor exception " + " for block "
										 + this._enclosing.block, e);
								}
								this.responderClosed = true;
							}
						}
						finally
						{
							scope.Close();
						}
					}
				}

				internal virtual void Close()
				{
					this.responderClosed = true;
					this.Interrupt();
				}

				private readonly DataStreamer _enclosing;
			}

			// If this stream has encountered any errors so far, shutdown 
			// threads and mark stream as closed. Returns true if we should
			// sleep for a while after returning from this call.
			//
			/// <exception cref="System.IO.IOException"/>
			private bool ProcessDatanodeError()
			{
				if (this.response != null)
				{
					DFSClient.Log.Info("Error Recovery for " + this.block + " waiting for responder to exit. "
						);
					return true;
				}
				this.CloseStream();
				// move packets from ack queue to front of the data queue
				lock (this._enclosing.dataQueue)
				{
					this._enclosing.dataQueue.AddRange(0, this._enclosing.ackQueue);
					this._enclosing.ackQueue.Clear();
				}
				// Record the new pipeline failure recovery.
				if (this.lastAckedSeqnoBeforeFailure != this._enclosing.lastAckedSeqno)
				{
					this.lastAckedSeqnoBeforeFailure = this._enclosing.lastAckedSeqno;
					this.pipelineRecoveryCount = 1;
				}
				else
				{
					// If we had to recover the pipeline five times in a row for the
					// same packet, this client likely has corrupt data or corrupting
					// during transmission.
					if (++this.pipelineRecoveryCount > 5)
					{
						DFSClient.Log.Warn("Error recovering pipeline for writing " + this.block + ". Already retried 5 times for the same packet."
							);
						this._enclosing.lastException.Set(new IOException("Failing write. Tried pipeline "
							 + "recovery 5 times without success."));
						this.streamerClosed = true;
						return false;
					}
				}
				bool doSleep = this.SetupPipelineForAppendOrRecovery();
				if (!this.streamerClosed && this._enclosing.dfsClient.clientRunning)
				{
					if (this.stage == BlockConstructionStage.PipelineClose)
					{
						// If we had an error while closing the pipeline, we go through a fast-path
						// where the BlockReceiver does not run. Instead, the DataNode just finalizes
						// the block immediately during the 'connect ack' process. So, we want to pull
						// the end-of-block packet from the dataQueue, since we don't actually have
						// a true pipeline to send it over.
						//
						// We also need to set lastAckedSeqno to the end-of-block Packet's seqno, so that
						// a client waiting on close() will be aware that the flush finished.
						lock (this._enclosing.dataQueue)
						{
							DFSPacket endOfBlockPacket = this._enclosing.dataQueue.Remove();
							// remove the end of block packet
							Span span = endOfBlockPacket.GetTraceSpan();
							if (span != null)
							{
								// Close any trace span associated with this Packet
								TraceScope scope = Trace.ContinueSpan(span);
								scope.Close();
							}
							System.Diagnostics.Debug.Assert(endOfBlockPacket.IsLastPacketInBlock());
							System.Diagnostics.Debug.Assert(this._enclosing.lastAckedSeqno == endOfBlockPacket
								.GetSeqno() - 1);
							this._enclosing.lastAckedSeqno = endOfBlockPacket.GetSeqno();
							Sharpen.Runtime.NotifyAll(this._enclosing.dataQueue);
						}
						this.EndBlock();
					}
					else
					{
						this.InitDataStreaming();
					}
				}
				return doSleep;
			}

			private void SetHflush()
			{
				this.isHflushed = true;
			}

			/// <exception cref="System.IO.IOException"/>
			private int FindNewDatanode(DatanodeInfo[] original)
			{
				if (this.nodes.Length != original.Length + 1)
				{
					throw new IOException(new StringBuilder().Append("Failed to replace a bad datanode on the existing pipeline "
						).Append("due to no more good datanodes being available to try. ").Append("(Nodes: current="
						).Append(Arrays.AsList(this.nodes)).Append(", original=").Append(Arrays.AsList(original
						)).Append("). ").Append("The current failed datanode replacement policy is ").Append
						(this._enclosing.dfsClient.dtpReplaceDatanodeOnFailure).Append(", and ").Append(
						"a client may configure this via '").Append(DFSConfigKeys.DfsClientWriteReplaceDatanodeOnFailurePolicyKey
						).Append("' in its configuration.").ToString());
				}
				for (int i = 0; i < this.nodes.Length; i++)
				{
					int j = 0;
					for (; j < original.Length && !this.nodes[i].Equals(original[j]); j++)
					{
					}
					if (j == original.Length)
					{
						return i;
					}
				}
				throw new IOException("Failed: new datanode not found: nodes=" + Arrays.AsList(this
					.nodes) + ", original=" + Arrays.AsList(original));
			}

			/// <exception cref="System.IO.IOException"/>
			private void AddDatanode2ExistingPipeline()
			{
				if (DataTransferProtocol.Log.IsDebugEnabled())
				{
					DataTransferProtocol.Log.Debug("lastAckedSeqno = " + this._enclosing.lastAckedSeqno
						);
				}
				/*
				* Is data transfer necessary?  We have the following cases.
				*
				* Case 1: Failure in Pipeline Setup
				* - Append
				*    + Transfer the stored replica, which may be a RBW or a finalized.
				* - Create
				*    + If no data, then no transfer is required.
				*    + If there are data written, transfer RBW. This case may happens
				*      when there are streaming failure earlier in this pipeline.
				*
				* Case 2: Failure in Streaming
				* - Append/Create:
				*    + transfer RBW
				*
				* Case 3: Failure in Close
				* - Append/Create:
				*    + no transfer, let NameNode replicates the block.
				*/
				if (!this.isAppend && this._enclosing.lastAckedSeqno < 0 && this.stage == BlockConstructionStage
					.PipelineSetupCreate)
				{
					//no data have been written
					return;
				}
				else
				{
					if (this.stage == BlockConstructionStage.PipelineClose || this.stage == BlockConstructionStage
						.PipelineCloseRecovery)
					{
						//pipeline is closing
						return;
					}
				}
				int tried = 0;
				DatanodeInfo[] original = this.nodes;
				StorageType[] originalTypes = this.storageTypes;
				string[] originalIDs = this.storageIDs;
				IOException caughtException = null;
				AList<DatanodeInfo> exclude = new AList<DatanodeInfo>(this.failed);
				while (tried < 3)
				{
					LocatedBlock lb;
					//get a new datanode
					lb = this._enclosing.dfsClient.namenode.GetAdditionalDatanode(this._enclosing.src
						, this._enclosing.fileId, this.block, this.nodes, this.storageIDs, Sharpen.Collections.ToArray
						(exclude, new DatanodeInfo[exclude.Count]), 1, this._enclosing.dfsClient.clientName
						);
					// a new node was allocated by the namenode. Update nodes.
					this.SetPipeline(lb);
					//find the new datanode
					int d = this.FindNewDatanode(original);
					//transfer replica. pick a source from the original nodes
					DatanodeInfo src = original[tried % original.Length];
					DatanodeInfo[] targets = new DatanodeInfo[] { this.nodes[d] };
					StorageType[] targetStorageTypes = new StorageType[] { this.storageTypes[d] };
					try
					{
						this.Transfer(src, targets, targetStorageTypes, lb.GetBlockToken());
					}
					catch (IOException ioe)
					{
						DFSClient.Log.Warn("Error transferring data from " + src + " to " + this.nodes[d]
							 + ": " + ioe.Message);
						caughtException = ioe;
						// add the allocated node to the exclude list.
						exclude.AddItem(this.nodes[d]);
						this.SetPipeline(original, originalTypes, originalIDs);
						tried++;
						continue;
					}
					return;
				}
				// finished successfully
				// All retries failed
				throw (caughtException != null) ? caughtException : new IOException("Failed to add a node"
					);
			}

			/// <exception cref="System.IO.IOException"/>
			private void Transfer(DatanodeInfo src, DatanodeInfo[] targets, StorageType[] targetStorageTypes
				, Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> blockToken)
			{
				//transfer replica to the new datanode
				Socket sock = null;
				DataOutputStream @out = null;
				DataInputStream @in = null;
				try
				{
					sock = DFSOutputStream.CreateSocketForPipeline(src, 2, this._enclosing.dfsClient);
					long writeTimeout = this._enclosing.dfsClient.GetDatanodeWriteTimeout(2);
					// transfer timeout multiplier based on the transfer size
					// One per 200 packets = 12.8MB. Minimum is 2.
					int multi = 2 + (int)(this.bytesSent / this._enclosing.dfsClient.GetConf().writePacketSize
						) / 200;
					long readTimeout = this._enclosing.dfsClient.GetDatanodeReadTimeout(multi);
					OutputStream unbufOut = NetUtils.GetOutputStream(sock, writeTimeout);
					InputStream unbufIn = NetUtils.GetInputStream(sock, readTimeout);
					IOStreamPair saslStreams = this._enclosing.dfsClient.saslClient.SocketSend(sock, 
						unbufOut, unbufIn, this._enclosing.dfsClient, blockToken, src);
					unbufOut = saslStreams.@out;
					unbufIn = saslStreams.@in;
					@out = new DataOutputStream(new BufferedOutputStream(unbufOut, HdfsConstants.SmallBufferSize
						));
					@in = new DataInputStream(unbufIn);
					//send the TRANSFER_BLOCK request
					new Sender(@out).TransferBlock(this.block, blockToken, this._enclosing.dfsClient.
						clientName, targets, targetStorageTypes);
					@out.Flush();
					//ack
					DataTransferProtos.BlockOpResponseProto response = DataTransferProtos.BlockOpResponseProto
						.ParseFrom(PBHelper.VintPrefixed(@in));
					if (DataTransferProtos.Status.Success != response.GetStatus())
					{
						throw new IOException("Failed to add a datanode");
					}
				}
				finally
				{
					IOUtils.CloseStream(@in);
					IOUtils.CloseStream(@out);
					IOUtils.CloseSocket(sock);
				}
			}

			/// <summary>
			/// Open a DataOutputStream to a DataNode pipeline so that
			/// it can be written to.
			/// </summary>
			/// <remarks>
			/// Open a DataOutputStream to a DataNode pipeline so that
			/// it can be written to.
			/// This happens when a file is appended or data streaming fails
			/// It keeps on trying until a pipeline is setup
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			private bool SetupPipelineForAppendOrRecovery()
			{
				// check number of datanodes
				if (this.nodes == null || this.nodes.Length == 0)
				{
					string msg = "Could not get block locations. " + "Source file \"" + this._enclosing
						.src + "\" - Aborting...";
					DFSClient.Log.Warn(msg);
					this.SetLastException(new IOException(msg));
					this.streamerClosed = true;
					return false;
				}
				bool success = false;
				long newGS = 0L;
				while (!success && !this.streamerClosed && this._enclosing.dfsClient.clientRunning
					)
				{
					// Sleep before reconnect if a dn is restarting.
					// This process will be repeated until the deadline or the datanode
					// starts back up.
					if (this.restartingNodeIndex.Get() >= 0)
					{
						// 4 seconds or the configured deadline period, whichever is shorter.
						// This is the retry interval and recovery will be retried in this
						// interval until timeout or success.
						long delay = Math.Min(this._enclosing.dfsClient.GetConf().datanodeRestartTimeout, 
							4000L);
						try
						{
							Sharpen.Thread.Sleep(delay);
						}
						catch (Exception)
						{
							this._enclosing.lastException.Set(new IOException("Interrupted while waiting for "
								 + "datanode to restart. " + this.nodes[this.restartingNodeIndex.Get()]));
							this.streamerClosed = true;
							return false;
						}
					}
					bool isRecovery = this.hasError;
					// remove bad datanode from list of datanodes.
					// If errorIndex was not set (i.e. appends), then do not remove 
					// any datanodes
					// 
					if (this.errorIndex >= 0)
					{
						StringBuilder pipelineMsg = new StringBuilder();
						for (int j = 0; j < this.nodes.Length; j++)
						{
							pipelineMsg.Append(this.nodes[j]);
							if (j < this.nodes.Length - 1)
							{
								pipelineMsg.Append(", ");
							}
						}
						if (this.nodes.Length <= 1)
						{
							this._enclosing.lastException.Set(new IOException("All datanodes " + pipelineMsg 
								+ " are bad. Aborting..."));
							this.streamerClosed = true;
							return false;
						}
						DFSClient.Log.Warn("Error Recovery for block " + this.block + " in pipeline " + pipelineMsg
							 + ": bad datanode " + this.nodes[this.errorIndex]);
						this.failed.AddItem(this.nodes[this.errorIndex]);
						DatanodeInfo[] newnodes = new DatanodeInfo[this.nodes.Length - 1];
						DFSOutputStream.Arraycopy(this.nodes, newnodes, this.errorIndex);
						StorageType[] newStorageTypes = new StorageType[newnodes.Length];
						DFSOutputStream.Arraycopy(this.storageTypes, newStorageTypes, this.errorIndex);
						string[] newStorageIDs = new string[newnodes.Length];
						DFSOutputStream.Arraycopy(this.storageIDs, newStorageIDs, this.errorIndex);
						this.SetPipeline(newnodes, newStorageTypes, newStorageIDs);
						// Just took care of a node error while waiting for a node restart
						if (this.restartingNodeIndex.Get() >= 0)
						{
							// If the error came from a node further away than the restarting
							// node, the restart must have been complete.
							if (this.errorIndex > this.restartingNodeIndex.Get())
							{
								this.restartingNodeIndex.Set(-1);
							}
							else
							{
								if (this.errorIndex < this.restartingNodeIndex.Get())
								{
									// the node index has shifted.
									this.restartingNodeIndex.DecrementAndGet();
								}
								else
								{
									// this shouldn't happen...
									System.Diagnostics.Debug.Assert(false);
								}
							}
						}
						if (this.restartingNodeIndex.Get() == -1)
						{
							this.hasError = false;
						}
						this._enclosing.lastException.Set(null);
						this.errorIndex = -1;
					}
					// Check if replace-datanode policy is satisfied.
					if (this._enclosing.dfsClient.dtpReplaceDatanodeOnFailure.Satisfy(this._enclosing
						.blockReplication, this.nodes, this.isAppend, this.isHflushed))
					{
						try
						{
							this.AddDatanode2ExistingPipeline();
						}
						catch (IOException ioe)
						{
							if (!this._enclosing.dfsClient.dtpReplaceDatanodeOnFailure.IsBestEffort())
							{
								throw;
							}
							DFSClient.Log.Warn("Failed to replace datanode." + " Continue with the remaining datanodes since "
								 + DFSConfigKeys.DfsClientWriteReplaceDatanodeOnFailureBestEffortKey + " is set to true."
								, ioe);
						}
					}
					// get a new generation stamp and an access token
					LocatedBlock lb = this._enclosing.dfsClient.namenode.UpdateBlockForPipeline(this.
						block, this._enclosing.dfsClient.clientName);
					newGS = lb.GetBlock().GetGenerationStamp();
					this.accessToken = lb.GetBlockToken();
					// set up the pipeline again with the remaining nodes
					if (this._enclosing.failPacket)
					{
						// for testing
						success = this.CreateBlockOutputStream(this.nodes, this.storageTypes, newGS, isRecovery
							);
						this._enclosing.failPacket = false;
						try
						{
							// Give DNs time to send in bad reports. In real situations,
							// good reports should follow bad ones, if client committed
							// with those nodes.
							Sharpen.Thread.Sleep(2000);
						}
						catch (Exception)
						{
						}
					}
					else
					{
						success = this.CreateBlockOutputStream(this.nodes, this.storageTypes, newGS, isRecovery
							);
					}
					if (this.restartingNodeIndex.Get() >= 0)
					{
						System.Diagnostics.Debug.Assert(this.hasError == true);
						// check errorIndex set above
						if (this.errorIndex == this.restartingNodeIndex.Get())
						{
							// ignore, if came from the restarting node
							this.errorIndex = -1;
						}
						// still within the deadline
						if (Time.MonotonicNow() < this.restartDeadline)
						{
							continue;
						}
						// with in the deadline
						// expired. declare the restarting node dead
						this.restartDeadline = 0;
						int expiredNodeIndex = this.restartingNodeIndex.Get();
						this.restartingNodeIndex.Set(-1);
						DFSClient.Log.Warn("Datanode did not restart in time: " + this.nodes[expiredNodeIndex
							]);
						// Mark the restarting node as failed. If there is any other failed
						// node during the last pipeline construction attempt, it will not be
						// overwritten/dropped. In this case, the restarting node will get
						// excluded in the following attempt, if it still does not come up.
						if (this.errorIndex == -1)
						{
							this.errorIndex = expiredNodeIndex;
						}
					}
				}
				// From this point on, normal pipeline recovery applies.
				// while
				if (success)
				{
					// update pipeline at the namenode
					ExtendedBlock newBlock = new ExtendedBlock(this.block.GetBlockPoolId(), this.block
						.GetBlockId(), this.block.GetNumBytes(), newGS);
					this._enclosing.dfsClient.namenode.UpdatePipeline(this._enclosing.dfsClient.clientName
						, this.block, newBlock, this.nodes, this.storageIDs);
					// update client side generation stamp
					this.block = newBlock;
				}
				return false;
			}

			// do not sleep, continue processing
			/// <summary>Open a DataOutputStream to a DataNode so that it can be written to.</summary>
			/// <remarks>
			/// Open a DataOutputStream to a DataNode so that it can be written to.
			/// This happens when a file is created and each time a new block is allocated.
			/// Must get block ID and the IDs of the destinations from the namenode.
			/// Returns the list of target datanodes.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			private LocatedBlock NextBlockOutputStream()
			{
				LocatedBlock lb = null;
				DatanodeInfo[] nodes = null;
				StorageType[] storageTypes = null;
				int count = this._enclosing.dfsClient.GetConf().nBlockWriteRetry;
				bool success = false;
				ExtendedBlock oldBlock = this.block;
				do
				{
					this.hasError = false;
					this._enclosing.lastException.Set(null);
					this.errorIndex = -1;
					success = false;
					DatanodeInfo[] excluded = Sharpen.Collections.ToArray(((ImmutableSet<DatanodeInfo
						>)this.excludedNodes.GetAllPresent(this.excludedNodes.AsMap().Keys).Keys), new DatanodeInfo
						[0]);
					this.block = oldBlock;
					lb = this.LocateFollowingBlock(excluded.Length > 0 ? excluded : null);
					this.block = lb.GetBlock();
					this.block.SetNumBytes(0);
					this.bytesSent = 0;
					this.accessToken = lb.GetBlockToken();
					nodes = lb.GetLocations();
					storageTypes = lb.GetStorageTypes();
					//
					// Connect to first DataNode in the list.
					//
					success = this.CreateBlockOutputStream(nodes, storageTypes, 0L, false);
					if (!success)
					{
						DFSClient.Log.Info("Abandoning " + this.block);
						this._enclosing.dfsClient.namenode.AbandonBlock(this.block, this._enclosing.fileId
							, this._enclosing.src, this._enclosing.dfsClient.clientName);
						this.block = null;
						DFSClient.Log.Info("Excluding datanode " + nodes[this.errorIndex]);
						this.excludedNodes.Put(nodes[this.errorIndex], nodes[this.errorIndex]);
					}
				}
				while (!success && --count >= 0);
				if (!success)
				{
					throw new IOException("Unable to create new block.");
				}
				return lb;
			}

			// connects to the first datanode in the pipeline
			// Returns true if success, otherwise return failure.
			//
			private bool CreateBlockOutputStream(DatanodeInfo[] nodes, StorageType[] nodeStorageTypes
				, long newGS, bool recoveryFlag)
			{
				if (nodes.Length == 0)
				{
					DFSClient.Log.Info("nodes are empty for write pipeline of block " + this.block);
					return false;
				}
				DataTransferProtos.Status pipelineStatus = DataTransferProtos.Status.Success;
				string firstBadLink = string.Empty;
				bool checkRestart = false;
				if (DFSClient.Log.IsDebugEnabled())
				{
					for (int i = 0; i < nodes.Length; i++)
					{
						DFSClient.Log.Debug("pipeline = " + nodes[i]);
					}
				}
				// persist blocks on namenode on next flush
				this._enclosing.persistBlocks.Set(true);
				int refetchEncryptionKey = 1;
				while (true)
				{
					bool result = false;
					DataOutputStream @out = null;
					try
					{
						System.Diagnostics.Debug.Assert(null == this._enclosing.s, "Previous socket unclosed"
							);
						System.Diagnostics.Debug.Assert(null == this.blockReplyStream, "Previous blockReplyStream unclosed"
							);
						this._enclosing.s = DFSOutputStream.CreateSocketForPipeline(nodes[0], nodes.Length
							, this._enclosing.dfsClient);
						long writeTimeout = this._enclosing.dfsClient.GetDatanodeWriteTimeout(nodes.Length
							);
						OutputStream unbufOut = NetUtils.GetOutputStream(this._enclosing.s, writeTimeout);
						InputStream unbufIn = NetUtils.GetInputStream(this._enclosing.s);
						IOStreamPair saslStreams = this._enclosing.dfsClient.saslClient.SocketSend(this._enclosing
							.s, unbufOut, unbufIn, this._enclosing.dfsClient, this.accessToken, nodes[0]);
						unbufOut = saslStreams.@out;
						unbufIn = saslStreams.@in;
						@out = new DataOutputStream(new BufferedOutputStream(unbufOut, HdfsConstants.SmallBufferSize
							));
						this.blockReplyStream = new DataInputStream(unbufIn);
						//
						// Xmit header info to datanode
						//
						BlockConstructionStage bcs = recoveryFlag ? this.stage.GetRecoveryStage() : this.
							stage;
						// We cannot change the block length in 'block' as it counts the number
						// of bytes ack'ed.
						ExtendedBlock blockCopy = new ExtendedBlock(this.block);
						blockCopy.SetNumBytes(this._enclosing.blockSize);
						bool[] targetPinnings = this.GetPinnings(nodes, true);
						// send the request
						new Sender(@out).WriteBlock(blockCopy, nodeStorageTypes[0], this.accessToken, this
							._enclosing.dfsClient.clientName, nodes, nodeStorageTypes, null, bcs, nodes.Length
							, this.block.GetNumBytes(), this.bytesSent, newGS, this._enclosing.checksum4WriteBlock
							, this._enclosing.cachingStrategy.Get(), this.isLazyPersistFile, (targetPinnings
							 == null ? false : targetPinnings[0]), targetPinnings);
						// receive ack for connect
						DataTransferProtos.BlockOpResponseProto resp = DataTransferProtos.BlockOpResponseProto
							.ParseFrom(PBHelper.VintPrefixed(this.blockReplyStream));
						pipelineStatus = resp.GetStatus();
						firstBadLink = resp.GetFirstBadLink();
						// Got an restart OOB ack.
						// If a node is already restarting, this status is not likely from
						// the same node. If it is from a different node, it is not
						// from the local datanode. Thus it is safe to treat this as a
						// regular node error.
						if (PipelineAck.IsRestartOOBStatus(pipelineStatus) && this.restartingNodeIndex.Get
							() == -1)
						{
							checkRestart = true;
							throw new IOException("A datanode is restarting.");
						}
						string logInfo = "ack with firstBadLink as " + firstBadLink;
						DataTransferProtoUtil.CheckBlockOpStatus(resp, logInfo);
						System.Diagnostics.Debug.Assert(null == this.blockStream, "Previous blockStream unclosed"
							);
						this.blockStream = @out;
						result = true;
						// success
						this.restartingNodeIndex.Set(-1);
						this.hasError = false;
					}
					catch (IOException ie)
					{
						if (this.restartingNodeIndex.Get() == -1)
						{
							DFSClient.Log.Info("Exception in createBlockOutputStream", ie);
						}
						if (ie is InvalidEncryptionKeyException && refetchEncryptionKey > 0)
						{
							DFSClient.Log.Info("Will fetch a new encryption key and retry, " + "encryption key was invalid when connecting to "
								 + nodes[0] + " : " + ie);
							// The encryption key used is invalid.
							refetchEncryptionKey--;
							this._enclosing.dfsClient.ClearDataEncryptionKey();
							// Don't close the socket/exclude this node just yet. Try again with
							// a new encryption key.
							continue;
						}
						// find the datanode that matches
						if (firstBadLink.Length != 0)
						{
							for (int i = 0; i < nodes.Length; i++)
							{
								// NB: Unconditionally using the xfer addr w/o hostname
								if (firstBadLink.Equals(nodes[i].GetXferAddr()))
								{
									this.errorIndex = i;
									break;
								}
							}
						}
						else
						{
							System.Diagnostics.Debug.Assert(checkRestart == false);
							this.errorIndex = 0;
						}
						// Check whether there is a restart worth waiting for.
						if (checkRestart && this.ShouldWaitForRestart(this.errorIndex))
						{
							this.restartDeadline = this._enclosing.dfsClient.GetConf().datanodeRestartTimeout
								 + Time.MonotonicNow();
							this.restartingNodeIndex.Set(this.errorIndex);
							this.errorIndex = -1;
							DFSClient.Log.Info("Waiting for the datanode to be restarted: " + nodes[this.restartingNodeIndex
								.Get()]);
						}
						this.hasError = true;
						this.SetLastException(ie);
						result = false;
					}
					finally
					{
						// error
						if (!result)
						{
							IOUtils.CloseSocket(this._enclosing.s);
							this._enclosing.s = null;
							IOUtils.CloseStream(@out);
							@out = null;
							IOUtils.CloseStream(this.blockReplyStream);
							this.blockReplyStream = null;
						}
					}
					return result;
				}
			}

			private bool[] GetPinnings(DatanodeInfo[] nodes, bool shouldLog)
			{
				if (this.favoredNodes == null)
				{
					return null;
				}
				else
				{
					bool[] pinnings = new bool[nodes.Length];
					HashSet<string> favoredSet = new HashSet<string>(Arrays.AsList(this.favoredNodes)
						);
					for (int i = 0; i < nodes.Length; i++)
					{
						pinnings[i] = favoredSet.Remove(nodes[i].GetXferAddrWithHostname());
						if (DFSClient.Log.IsDebugEnabled())
						{
							DFSClient.Log.Debug(nodes[i].GetXferAddrWithHostname() + " was chosen by name node (favored="
								 + pinnings[i] + ").");
						}
					}
					if (shouldLog && !favoredSet.IsEmpty())
					{
						// There is one or more favored nodes that were not allocated.
						DFSClient.Log.Warn("These favored nodes were specified but not chosen: " + favoredSet
							 + " Specified favored nodes: " + Arrays.ToString(this.favoredNodes));
					}
					return pinnings;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private LocatedBlock LocateFollowingBlock(DatanodeInfo[] excludedNodes)
			{
				int retries = this._enclosing.dfsClient.GetConf().nBlockWriteLocateFollowingRetry;
				long sleeptime = 400;
				while (true)
				{
					long localstart = Time.MonotonicNow();
					while (true)
					{
						try
						{
							return this._enclosing.dfsClient.namenode.AddBlock(this._enclosing.src, this._enclosing
								.dfsClient.clientName, this.block, excludedNodes, this._enclosing.fileId, this.favoredNodes
								);
						}
						catch (RemoteException e)
						{
							IOException ue = e.UnwrapRemoteException(typeof(FileNotFoundException), typeof(AccessControlException
								), typeof(NSQuotaExceededException), typeof(DSQuotaExceededException), typeof(UnresolvedPathException
								));
							if (ue != e)
							{
								throw ue;
							}
							// no need to retry these exceptions
							if (typeof(NotReplicatedYetException).FullName.Equals(e.GetClassName()))
							{
								if (retries == 0)
								{
									throw;
								}
								else
								{
									--retries;
									DFSClient.Log.Info("Exception while adding a block", e);
									long elapsed = Time.MonotonicNow() - localstart;
									if (elapsed > 5000)
									{
										DFSClient.Log.Info("Waiting for replication for " + (elapsed / 1000) + " seconds"
											);
									}
									try
									{
										DFSClient.Log.Warn("NotReplicatedYetException sleeping " + this._enclosing.src + 
											" retries left " + retries);
										Sharpen.Thread.Sleep(sleeptime);
										sleeptime *= 2;
									}
									catch (Exception ie)
									{
										DFSClient.Log.Warn("Caught exception ", ie);
									}
								}
							}
							else
							{
								throw;
							}
						}
					}
				}
			}

			internal virtual ExtendedBlock GetBlock()
			{
				return this.block;
			}

			internal virtual DatanodeInfo[] GetNodes()
			{
				return this.nodes;
			}

			internal virtual Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> GetBlockToken
				()
			{
				return this.accessToken;
			}

			private void SetLastException(IOException e)
			{
				this._enclosing.lastException.CompareAndSet(null, e);
			}

			private readonly DFSOutputStream _enclosing;
		}

		/// <summary>Create a socket for a write pipeline</summary>
		/// <param name="first">the first datanode</param>
		/// <param name="length">the pipeline length</param>
		/// <param name="client">client</param>
		/// <returns>the socket connected to the first datanode</returns>
		/// <exception cref="System.IO.IOException"/>
		internal static Socket CreateSocketForPipeline(DatanodeInfo first, int length, DFSClient
			 client)
		{
			string dnAddr = first.GetXferAddr(client.GetConf().connectToDnViaHostname);
			if (DFSClient.Log.IsDebugEnabled())
			{
				DFSClient.Log.Debug("Connecting to datanode " + dnAddr);
			}
			IPEndPoint isa = NetUtils.CreateSocketAddr(dnAddr);
			Socket sock = client.socketFactory.CreateSocket();
			int timeout = client.GetDatanodeReadTimeout(length);
			NetUtils.Connect(sock, isa, client.GetRandomLocalInterfaceAddr(), client.GetConf(
				).socketTimeout);
			sock.ReceiveTimeout = timeout;
			sock.SetSendBufferSize(HdfsConstants.DefaultDataSocketSize);
			if (DFSClient.Log.IsDebugEnabled())
			{
				DFSClient.Log.Debug("Send buf size " + sock.GetSendBufferSize());
			}
			return sock;
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void CheckClosed()
		{
			if (IsClosed())
			{
				IOException e = lastException.Get();
				throw e != null ? e : new ClosedChannelException();
			}
		}

		//
		// returns the list of targets, if any, that is being currently used.
		//
		[VisibleForTesting]
		public virtual DatanodeInfo[] GetPipeline()
		{
			lock (this)
			{
				if (streamer == null)
				{
					return null;
				}
				DatanodeInfo[] currentNodes = streamer.GetNodes();
				if (currentNodes == null)
				{
					return null;
				}
				DatanodeInfo[] value = new DatanodeInfo[currentNodes.Length];
				for (int i = 0; i < currentNodes.Length; i++)
				{
					value[i] = currentNodes[i];
				}
				return value;
			}
		}

		/// <returns>
		/// the object for computing checksum.
		/// The type is NULL if checksum is not computed.
		/// </returns>
		private static DataChecksum GetChecksum4Compute(DataChecksum checksum, HdfsFileStatus
			 stat)
		{
			if (IsLazyPersist(stat) && stat.GetReplication() == 1)
			{
				// do not compute checksum for writing to single replica to memory
				return DataChecksum.NewDataChecksum(DataChecksum.Type.Null, checksum.GetBytesPerChecksum
					());
			}
			return checksum;
		}

		/// <exception cref="System.IO.IOException"/>
		private DFSOutputStream(DFSClient dfsClient, string src, Progressable progress, HdfsFileStatus
			 stat, DataChecksum checksum)
			: base(GetChecksum4Compute(checksum, stat))
		{
			this.dfsClient = dfsClient;
			this.src = src;
			this.fileId = stat.GetFileId();
			this.blockSize = stat.GetBlockSize();
			this.blockReplication = stat.GetReplication();
			this.fileEncryptionInfo = stat.GetFileEncryptionInfo();
			this.progress = progress;
			this.cachingStrategy = new AtomicReference<CachingStrategy>(dfsClient.GetDefaultWriteCachingStrategy
				());
			if ((progress != null) && DFSClient.Log.IsDebugEnabled())
			{
				DFSClient.Log.Debug("Set non-null progress callback on DFSOutputStream " + src);
			}
			this.bytesPerChecksum = checksum.GetBytesPerChecksum();
			if (bytesPerChecksum <= 0)
			{
				throw new HadoopIllegalArgumentException("Invalid value: bytesPerChecksum = " + bytesPerChecksum
					 + " <= 0");
			}
			if (blockSize % bytesPerChecksum != 0)
			{
				throw new HadoopIllegalArgumentException("Invalid values: " + DFSConfigKeys.DfsBytesPerChecksumKey
					 + " (=" + bytesPerChecksum + ") must divide block size (=" + blockSize + ").");
			}
			this.checksum4WriteBlock = checksum;
			this.dfsclientSlowLogThresholdMs = dfsClient.GetConf().dfsclientSlowIoWarningThresholdMs;
			this.byteArrayManager = dfsClient.GetClientContext().GetByteArrayManager();
		}

		/// <summary>Construct a new output stream for creating a file.</summary>
		/// <exception cref="System.IO.IOException"/>
		private DFSOutputStream(DFSClient dfsClient, string src, HdfsFileStatus stat, EnumSet
			<CreateFlag> flag, Progressable progress, DataChecksum checksum, string[] favoredNodes
			)
			: this(dfsClient, src, progress, stat, checksum)
		{
			this.shouldSyncBlock = flag.Contains(CreateFlag.SyncBlock);
			ComputePacketChunkSize(dfsClient.GetConf().writePacketSize, bytesPerChecksum);
			streamer = new DFSOutputStream.DataStreamer(this, stat, null);
			if (favoredNodes != null && favoredNodes.Length != 0)
			{
				streamer.SetFavoredNodes(favoredNodes);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static DFSOutputStream NewStreamForCreate(DFSClient dfsClient, string src
			, FsPermission masked, EnumSet<CreateFlag> flag, bool createParent, short replication
			, long blockSize, Progressable progress, int buffersize, DataChecksum checksum, 
			string[] favoredNodes)
		{
			TraceScope scope = dfsClient.GetPathTraceScope("newStreamForCreate", src);
			try
			{
				HdfsFileStatus stat = null;
				// Retry the create if we get a RetryStartFileException up to a maximum
				// number of times
				bool shouldRetry = true;
				int retryCount = CreateRetryCount;
				while (shouldRetry)
				{
					shouldRetry = false;
					try
					{
						stat = dfsClient.namenode.Create(src, masked, dfsClient.clientName, new EnumSetWritable
							<CreateFlag>(flag), createParent, replication, blockSize, SupportedCryptoVersions
							);
						break;
					}
					catch (RemoteException re)
					{
						IOException e = re.UnwrapRemoteException(typeof(AccessControlException), typeof(DSQuotaExceededException
							), typeof(FileAlreadyExistsException), typeof(FileNotFoundException), typeof(ParentNotDirectoryException
							), typeof(NSQuotaExceededException), typeof(RetryStartFileException), typeof(SafeModeException
							), typeof(UnresolvedPathException), typeof(SnapshotAccessControlException), typeof(
							UnknownCryptoProtocolVersionException));
						if (e is RetryStartFileException)
						{
							if (retryCount > 0)
							{
								shouldRetry = true;
								retryCount--;
							}
							else
							{
								throw new IOException("Too many retries because of encryption" + " zone operations"
									, e);
							}
						}
						else
						{
							throw e;
						}
					}
				}
				Preconditions.CheckNotNull(stat, "HdfsFileStatus should not be null!");
				DFSOutputStream @out = new DFSOutputStream(dfsClient, src, stat, flag, progress, 
					checksum, favoredNodes);
				@out.Start();
				return @out;
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Construct a new output stream for append.</summary>
		/// <exception cref="System.IO.IOException"/>
		private DFSOutputStream(DFSClient dfsClient, string src, EnumSet<CreateFlag> flags
			, Progressable progress, LocatedBlock lastBlock, HdfsFileStatus stat, DataChecksum
			 checksum)
			: this(dfsClient, src, progress, stat, checksum)
		{
			initialFileSize = stat.GetLen();
			// length of file when opened
			this.shouldSyncBlock = flags.Contains(CreateFlag.SyncBlock);
			bool toNewBlock = flags.Contains(CreateFlag.NewBlock);
			// The last partial block of the file has to be filled.
			if (!toNewBlock && lastBlock != null)
			{
				// indicate that we are appending to an existing block
				bytesCurBlock = lastBlock.GetBlockSize();
				streamer = new DFSOutputStream.DataStreamer(this, lastBlock, stat, bytesPerChecksum
					);
			}
			else
			{
				ComputePacketChunkSize(dfsClient.GetConf().writePacketSize, bytesPerChecksum);
				streamer = new DFSOutputStream.DataStreamer(this, stat, lastBlock != null ? lastBlock
					.GetBlock() : null);
			}
			this.fileEncryptionInfo = stat.GetFileEncryptionInfo();
		}

		/// <exception cref="System.IO.IOException"/>
		internal static DFSOutputStream NewStreamForAppend(DFSClient dfsClient, string src
			, EnumSet<CreateFlag> flags, int bufferSize, Progressable progress, LocatedBlock
			 lastBlock, HdfsFileStatus stat, DataChecksum checksum, string[] favoredNodes)
		{
			TraceScope scope = dfsClient.GetPathTraceScope("newStreamForAppend", src);
			try
			{
				DFSOutputStream @out = new DFSOutputStream(dfsClient, src, flags, progress, lastBlock
					, stat, checksum);
				if (favoredNodes != null && favoredNodes.Length != 0)
				{
					@out.streamer.SetFavoredNodes(favoredNodes);
				}
				@out.Start();
				return @out;
			}
			finally
			{
				scope.Close();
			}
		}

		private static bool IsLazyPersist(HdfsFileStatus stat)
		{
			BlockStoragePolicy p = blockStoragePolicySuite.GetPolicy(HdfsConstants.MemoryStoragePolicyName
				);
			return p != null && stat.GetStoragePolicy() == p.GetId();
		}

		private void ComputePacketChunkSize(int psize, int csize)
		{
			int bodySize = psize - PacketHeader.PktMaxHeaderLen;
			int chunkSize = csize + GetChecksumSize();
			chunksPerPacket = Math.Max(bodySize / chunkSize, 1);
			packetSize = chunkSize * chunksPerPacket;
			if (DFSClient.Log.IsDebugEnabled())
			{
				DFSClient.Log.Debug("computePacketChunkSize: src=" + src + ", chunkSize=" + chunkSize
					 + ", chunksPerPacket=" + chunksPerPacket + ", packetSize=" + packetSize);
			}
		}

		private void QueueCurrentPacket()
		{
			lock (dataQueue)
			{
				if (currentPacket == null)
				{
					return;
				}
				currentPacket.AddTraceParent(Trace.CurrentSpan());
				dataQueue.AddLast(currentPacket);
				lastQueuedSeqno = currentPacket.GetSeqno();
				if (DFSClient.Log.IsDebugEnabled())
				{
					DFSClient.Log.Debug("Queued packet " + currentPacket.GetSeqno());
				}
				currentPacket = null;
				Sharpen.Runtime.NotifyAll(dataQueue);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WaitAndQueueCurrentPacket()
		{
			lock (dataQueue)
			{
				try
				{
					// If queue is full, then wait till we have enough space
					bool firstWait = true;
					try
					{
						while (!IsClosed() && dataQueue.Count + ackQueue.Count > dfsClient.GetConf().writeMaxPackets
							)
						{
							if (firstWait)
							{
								Span span = Trace.CurrentSpan();
								if (span != null)
								{
									span.AddTimelineAnnotation("dataQueue.wait");
								}
								firstWait = false;
							}
							try
							{
								Sharpen.Runtime.Wait(dataQueue);
							}
							catch (Exception)
							{
								// If we get interrupted while waiting to queue data, we still need to get rid
								// of the current packet. This is because we have an invariant that if
								// currentPacket gets full, it will get queued before the next writeChunk.
								//
								// Rather than wait around for space in the queue, we should instead try to
								// return to the caller as soon as possible, even though we slightly overrun
								// the MAX_PACKETS length.
								Sharpen.Thread.CurrentThread().Interrupt();
								break;
							}
						}
					}
					finally
					{
						Span span = Trace.CurrentSpan();
						if ((span != null) && (!firstWait))
						{
							span.AddTimelineAnnotation("end.wait");
						}
					}
					CheckClosed();
					QueueCurrentPacket();
				}
				catch (ClosedChannelException)
				{
				}
			}
		}

		// @see FSOutputSummer#writeChunk()
		/// <exception cref="System.IO.IOException"/>
		protected override void WriteChunk(byte[] b, int offset, int len, byte[] checksum
			, int ckoff, int cklen)
		{
			lock (this)
			{
				TraceScope scope = dfsClient.GetPathTraceScope("DFSOutputStream#writeChunk", src);
				try
				{
					WriteChunkImpl(b, offset, len, checksum, ckoff, cklen);
				}
				finally
				{
					scope.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteChunkImpl(byte[] b, int offset, int len, byte[] checksum, int ckoff
			, int cklen)
		{
			lock (this)
			{
				dfsClient.CheckOpen();
				CheckClosed();
				if (len > bytesPerChecksum)
				{
					throw new IOException("writeChunk() buffer size is " + len + " is larger than supported  bytesPerChecksum "
						 + bytesPerChecksum);
				}
				if (cklen != 0 && cklen != GetChecksumSize())
				{
					throw new IOException("writeChunk() checksum size is supposed to be " + GetChecksumSize
						() + " but found to be " + cklen);
				}
				if (currentPacket == null)
				{
					currentPacket = CreatePacket(packetSize, chunksPerPacket, bytesCurBlock, currentSeqno
						++, false);
					if (DFSClient.Log.IsDebugEnabled())
					{
						DFSClient.Log.Debug("DFSClient writeChunk allocating new packet seqno=" + currentPacket
							.GetSeqno() + ", src=" + src + ", packetSize=" + packetSize + ", chunksPerPacket="
							 + chunksPerPacket + ", bytesCurBlock=" + bytesCurBlock);
					}
				}
				currentPacket.WriteChecksum(checksum, ckoff, cklen);
				currentPacket.WriteData(b, offset, len);
				currentPacket.IncNumChunks();
				bytesCurBlock += len;
				// If packet is full, enqueue it for transmission
				//
				if (currentPacket.GetNumChunks() == currentPacket.GetMaxChunks() || bytesCurBlock
					 == blockSize)
				{
					if (DFSClient.Log.IsDebugEnabled())
					{
						DFSClient.Log.Debug("DFSClient writeChunk packet full seqno=" + currentPacket.GetSeqno
							() + ", src=" + src + ", bytesCurBlock=" + bytesCurBlock + ", blockSize=" + blockSize
							 + ", appendChunk=" + appendChunk);
					}
					WaitAndQueueCurrentPacket();
					// If the reopened file did not end at chunk boundary and the above
					// write filled up its partial chunk. Tell the summer to generate full 
					// crc chunks from now on.
					if (appendChunk && bytesCurBlock % bytesPerChecksum == 0)
					{
						appendChunk = false;
						ResetChecksumBufSize();
					}
					if (!appendChunk)
					{
						int psize = Math.Min((int)(blockSize - bytesCurBlock), dfsClient.GetConf().writePacketSize
							);
						ComputePacketChunkSize(psize, bytesPerChecksum);
					}
					//
					// if encountering a block boundary, send an empty packet to 
					// indicate the end of block and reset bytesCurBlock.
					//
					if (bytesCurBlock == blockSize)
					{
						currentPacket = CreatePacket(0, 0, bytesCurBlock, currentSeqno++, true);
						currentPacket.SetSyncBlock(shouldSyncBlock);
						WaitAndQueueCurrentPacket();
						bytesCurBlock = 0;
						lastFlushOffset = 0;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public virtual void Sync()
		{
			Hflush();
		}

		/// <summary>Flushes out to all replicas of the block.</summary>
		/// <remarks>
		/// Flushes out to all replicas of the block. The data is in the buffers
		/// of the DNs but not necessarily in the DN's OS buffers.
		/// It is a synchronous operation. When it returns,
		/// it guarantees that flushed data become visible to new readers.
		/// It is not guaranteed that data has been flushed to
		/// persistent store on the datanode.
		/// Block allocations are persisted on namenode.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Hflush()
		{
			TraceScope scope = dfsClient.GetPathTraceScope("hflush", src);
			try
			{
				FlushOrSync(false, EnumSet.NoneOf<HdfsDataOutputStream.SyncFlag>());
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Hsync()
		{
			TraceScope scope = dfsClient.GetPathTraceScope("hsync", src);
			try
			{
				FlushOrSync(true, EnumSet.NoneOf<HdfsDataOutputStream.SyncFlag>());
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>
		/// The expected semantics is all data have flushed out to all replicas
		/// and all replicas have done posix fsync equivalent - ie the OS has
		/// flushed it to the disk device (but the disk may have it in its cache).
		/// </summary>
		/// <remarks>
		/// The expected semantics is all data have flushed out to all replicas
		/// and all replicas have done posix fsync equivalent - ie the OS has
		/// flushed it to the disk device (but the disk may have it in its cache).
		/// Note that only the current block is flushed to the disk device.
		/// To guarantee durable sync across block boundaries the stream should
		/// be created with
		/// <see cref="Org.Apache.Hadoop.FS.CreateFlag.SyncBlock"/>
		/// .
		/// </remarks>
		/// <param name="syncFlags">
		/// Indicate the semantic of the sync. Currently used to specify
		/// whether or not to update the block length in NameNode.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Hsync(EnumSet<HdfsDataOutputStream.SyncFlag> syncFlags)
		{
			TraceScope scope = dfsClient.GetPathTraceScope("hsync", src);
			try
			{
				FlushOrSync(true, syncFlags);
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Flush/Sync buffered data to DataNodes.</summary>
		/// <param name="isSync">
		/// Whether or not to require all replicas to flush data to the disk
		/// device
		/// </param>
		/// <param name="syncFlags">
		/// Indicate extra detailed semantic of the flush/sync. Currently
		/// mainly used to specify whether or not to update the file length in
		/// the NameNode
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		private void FlushOrSync(bool isSync, EnumSet<HdfsDataOutputStream.SyncFlag> syncFlags
			)
		{
			dfsClient.CheckOpen();
			CheckClosed();
			try
			{
				long toWaitFor;
				long lastBlockLength = -1L;
				bool updateLength = syncFlags.Contains(HdfsDataOutputStream.SyncFlag.UpdateLength
					);
				bool endBlock = syncFlags.Contains(HdfsDataOutputStream.SyncFlag.EndBlock);
				lock (this)
				{
					// flush checksum buffer, but keep checksum buffer intact if we do not
					// need to end the current block
					int numKept = FlushBuffer(!endBlock, true);
					// bytesCurBlock potentially incremented if there was buffered data
					if (DFSClient.Log.IsDebugEnabled())
					{
						DFSClient.Log.Debug("DFSClient flush():" + " bytesCurBlock=" + bytesCurBlock + " lastFlushOffset="
							 + lastFlushOffset + " createNewBlock=" + endBlock);
					}
					// Flush only if we haven't already flushed till this offset.
					if (lastFlushOffset != bytesCurBlock)
					{
						System.Diagnostics.Debug.Assert(bytesCurBlock > lastFlushOffset);
						// record the valid offset of this flush
						lastFlushOffset = bytesCurBlock;
						if (isSync && currentPacket == null && !endBlock)
						{
							// Nothing to send right now,
							// but sync was requested.
							// Send an empty packet if we do not end the block right now
							currentPacket = CreatePacket(packetSize, chunksPerPacket, bytesCurBlock, currentSeqno
								++, false);
						}
					}
					else
					{
						if (isSync && bytesCurBlock > 0 && !endBlock)
						{
							// Nothing to send right now,
							// and the block was partially written,
							// and sync was requested.
							// So send an empty sync packet if we do not end the block right now
							currentPacket = CreatePacket(packetSize, chunksPerPacket, bytesCurBlock, currentSeqno
								++, false);
						}
						else
						{
							if (currentPacket != null)
							{
								// just discard the current packet since it is already been sent.
								currentPacket.ReleaseBuffer(byteArrayManager);
								currentPacket = null;
							}
						}
					}
					if (currentPacket != null)
					{
						currentPacket.SetSyncBlock(isSync);
						WaitAndQueueCurrentPacket();
					}
					if (endBlock && bytesCurBlock > 0)
					{
						// Need to end the current block, thus send an empty packet to
						// indicate this is the end of the block and reset bytesCurBlock
						currentPacket = CreatePacket(0, 0, bytesCurBlock, currentSeqno++, true);
						currentPacket.SetSyncBlock(shouldSyncBlock || isSync);
						WaitAndQueueCurrentPacket();
						bytesCurBlock = 0;
						lastFlushOffset = 0;
					}
					else
					{
						// Restore state of stream. Record the last flush offset
						// of the last full chunk that was flushed.
						bytesCurBlock -= numKept;
					}
					toWaitFor = lastQueuedSeqno;
				}
				// end synchronized
				WaitForAckedSeqno(toWaitFor);
				// update the block length first time irrespective of flag
				if (updateLength || persistBlocks.Get())
				{
					lock (this)
					{
						if (streamer != null && streamer.block != null)
						{
							lastBlockLength = streamer.block.GetNumBytes();
						}
					}
				}
				// If 1) any new blocks were allocated since the last flush, or 2) to
				// update length in NN is required, then persist block locations on
				// namenode.
				if (persistBlocks.GetAndSet(false) || updateLength)
				{
					try
					{
						dfsClient.namenode.Fsync(src, fileId, dfsClient.clientName, lastBlockLength);
					}
					catch (IOException ioe)
					{
						DFSClient.Log.Warn("Unable to persist blocks in hflush for " + src, ioe);
						// If we got an error here, it might be because some other thread called
						// close before our hflush completed. In that case, we should throw an
						// exception that the stream is closed.
						CheckClosed();
						// If we aren't closed but failed to sync, we should expose that to the
						// caller.
						throw;
					}
				}
				lock (this)
				{
					if (streamer != null)
					{
						streamer.SetHflush();
					}
				}
			}
			catch (ThreadInterruptedException interrupt)
			{
				// This kind of error doesn't mean that the stream itself is broken - just the
				// flushing thread got interrupted. So, we shouldn't close down the writer,
				// but instead just propagate the error
				throw;
			}
			catch (IOException e)
			{
				DFSClient.Log.Warn("Error while syncing", e);
				lock (this)
				{
					if (!IsClosed())
					{
						lastException.Set(new IOException("IOException flush: " + e));
						CloseThreads(true);
					}
				}
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"use Org.Apache.Hadoop.Hdfs.Client.HdfsDataOutputStream.GetCurrentBlockReplication() ."
			)]
		public virtual int GetNumCurrentReplicas()
		{
			lock (this)
			{
				return GetCurrentBlockReplication();
			}
		}

		/// <summary>
		/// Note that this is not a public API;
		/// use
		/// <see cref="Org.Apache.Hadoop.Hdfs.Client.HdfsDataOutputStream.GetCurrentBlockReplication()
		/// 	"/>
		/// instead.
		/// </summary>
		/// <returns>the number of valid replicas of the current block</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual int GetCurrentBlockReplication()
		{
			lock (this)
			{
				dfsClient.CheckOpen();
				CheckClosed();
				if (streamer == null)
				{
					return blockReplication;
				}
				// no pipeline, return repl factor of file
				DatanodeInfo[] currentNodes = streamer.GetNodes();
				if (currentNodes == null)
				{
					return blockReplication;
				}
				// no pipeline, return repl factor of file
				return currentNodes.Length;
			}
		}

		/// <summary>
		/// Waits till all existing data is flushed and confirmations
		/// received from datanodes.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void FlushInternal()
		{
			long toWaitFor;
			lock (this)
			{
				dfsClient.CheckOpen();
				CheckClosed();
				//
				// If there is data in the current buffer, send it across
				//
				QueueCurrentPacket();
				toWaitFor = lastQueuedSeqno;
			}
			WaitForAckedSeqno(toWaitFor);
		}

		/// <exception cref="System.IO.IOException"/>
		private void WaitForAckedSeqno(long seqno)
		{
			TraceScope scope = Trace.StartSpan("waitForAckedSeqno", Sampler.Never);
			try
			{
				if (DFSClient.Log.IsDebugEnabled())
				{
					DFSClient.Log.Debug("Waiting for ack for: " + seqno);
				}
				long begin = Time.MonotonicNow();
				try
				{
					lock (dataQueue)
					{
						while (!IsClosed())
						{
							CheckClosed();
							if (lastAckedSeqno >= seqno)
							{
								break;
							}
							try
							{
								Sharpen.Runtime.Wait(dataQueue, 1000);
							}
							catch (Exception)
							{
								// when we receive an ack, we notify on
								// dataQueue
								throw new ThreadInterruptedException("Interrupted while waiting for data to be acknowledged by pipeline"
									);
							}
						}
					}
					CheckClosed();
				}
				catch (ClosedChannelException)
				{
				}
				long duration = Time.MonotonicNow() - begin;
				if (duration > dfsclientSlowLogThresholdMs)
				{
					DFSClient.Log.Warn("Slow waitForAckedSeqno took " + duration + "ms (threshold=" +
						 dfsclientSlowLogThresholdMs + "ms)");
				}
			}
			finally
			{
				scope.Close();
			}
		}

		private void Start()
		{
			lock (this)
			{
				streamer.Start();
			}
		}

		/// <summary>
		/// Aborts this output stream and releases any system
		/// resources associated with this stream.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void Abort()
		{
			lock (this)
			{
				if (IsClosed())
				{
					return;
				}
				streamer.SetLastException(new IOException("Lease timeout of " + (dfsClient.GetHdfsTimeout
					() / 1000) + " seconds expired."));
				CloseThreads(true);
			}
			dfsClient.EndFileLease(fileId);
		}

		internal virtual bool IsClosed()
		{
			return closed;
		}

		internal virtual void SetClosed()
		{
			closed = true;
			lock (dataQueue)
			{
				ReleaseBuffer(dataQueue, byteArrayManager);
				ReleaseBuffer(ackQueue, byteArrayManager);
			}
		}

		private static void ReleaseBuffer(IList<DFSPacket> packets, ByteArrayManager bam)
		{
			foreach (DFSPacket p in packets)
			{
				p.ReleaseBuffer(bam);
			}
			packets.Clear();
		}

		// shutdown datastreamer and responseprocessor threads.
		// interrupt datastreamer if force is true
		/// <exception cref="System.IO.IOException"/>
		private void CloseThreads(bool force)
		{
			try
			{
				streamer.Close(force);
				streamer.Join();
				if (s != null)
				{
					s.Close();
				}
			}
			catch (Exception)
			{
				throw new IOException("Failed to shutdown streamer");
			}
			finally
			{
				streamer = null;
				s = null;
				SetClosed();
			}
		}

		/// <summary>
		/// Closes this output stream and releases any system
		/// resources associated with this stream.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				TraceScope scope = dfsClient.GetPathTraceScope("DFSOutputStream#close", src);
				try
				{
					CloseImpl();
				}
				finally
				{
					scope.Close();
				}
			}
			dfsClient.EndFileLease(fileId);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CloseImpl()
		{
			lock (this)
			{
				if (IsClosed())
				{
					IOException e = lastException.GetAndSet(null);
					if (e == null)
					{
						return;
					}
					else
					{
						throw e;
					}
				}
				try
				{
					FlushBuffer();
					// flush from all upper layers
					if (currentPacket != null)
					{
						WaitAndQueueCurrentPacket();
					}
					if (bytesCurBlock != 0)
					{
						// send an empty packet to mark the end of the block
						currentPacket = CreatePacket(0, 0, bytesCurBlock, currentSeqno++, true);
						currentPacket.SetSyncBlock(shouldSyncBlock);
					}
					FlushInternal();
					// flush all data to Datanodes
					// get last block before destroying the streamer
					ExtendedBlock lastBlock = streamer.GetBlock();
					CloseThreads(false);
					TraceScope scope = Trace.StartSpan("completeFile", Sampler.Never);
					try
					{
						CompleteFile(lastBlock);
					}
					finally
					{
						scope.Close();
					}
				}
				catch (ClosedChannelException)
				{
				}
				finally
				{
					SetClosed();
				}
			}
		}

		// should be called holding (this) lock since setTestFilename() may 
		// be called during unit tests
		/// <exception cref="System.IO.IOException"/>
		private void CompleteFile(ExtendedBlock last)
		{
			long localstart = Time.MonotonicNow();
			long localTimeout = 400;
			bool fileComplete = false;
			int retries = dfsClient.GetConf().nBlockWriteLocateFollowingRetry;
			while (!fileComplete)
			{
				fileComplete = dfsClient.namenode.Complete(src, dfsClient.clientName, last, fileId
					);
				if (!fileComplete)
				{
					int hdfsTimeout = dfsClient.GetHdfsTimeout();
					if (!dfsClient.clientRunning || (hdfsTimeout > 0 && localstart + hdfsTimeout < Time
						.MonotonicNow()))
					{
						string msg = "Unable to close file because dfsclient " + " was unable to contact the HDFS servers."
							 + " clientRunning " + dfsClient.clientRunning + " hdfsTimeout " + hdfsTimeout;
						DFSClient.Log.Info(msg);
						throw new IOException(msg);
					}
					try
					{
						if (retries == 0)
						{
							throw new IOException("Unable to close file because the last block" + " does not have enough number of replicas."
								);
						}
						retries--;
						Sharpen.Thread.Sleep(localTimeout);
						localTimeout *= 2;
						if (Time.MonotonicNow() - localstart > 5000)
						{
							DFSClient.Log.Info("Could not complete " + src + " retrying...");
						}
					}
					catch (Exception ie)
					{
						DFSClient.Log.Warn("Caught exception ", ie);
					}
				}
			}
		}

		[VisibleForTesting]
		public virtual void SetArtificialSlowdown(long period)
		{
			artificialSlowdown = period;
		}

		[VisibleForTesting]
		public virtual void SetChunksPerPacket(int value)
		{
			lock (this)
			{
				chunksPerPacket = Math.Min(chunksPerPacket, value);
				packetSize = (bytesPerChecksum + GetChecksumSize()) * chunksPerPacket;
			}
		}

		internal virtual void SetTestFilename(string newname)
		{
			lock (this)
			{
				src = newname;
			}
		}

		/// <summary>Returns the size of a file as it was when this stream was opened</summary>
		public virtual long GetInitialLen()
		{
			return initialFileSize;
		}

		/// <returns>the FileEncryptionInfo for this stream, or null if not encrypted.</returns>
		public virtual FileEncryptionInfo GetFileEncryptionInfo()
		{
			return fileEncryptionInfo;
		}

		/// <summary>Returns the access token currently used by streamer, for testing only</summary>
		internal virtual Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> GetBlockToken
			()
		{
			lock (this)
			{
				return streamer.GetBlockToken();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetDropBehind(bool dropBehind)
		{
			CachingStrategy prevStrategy;
			CachingStrategy nextStrategy;
			do
			{
				// CachingStrategy is immutable.  So build a new CachingStrategy with the
				// modifications we want, and compare-and-swap it in.
				prevStrategy = this.cachingStrategy.Get();
				nextStrategy = new CachingStrategy.Builder(prevStrategy).SetDropBehind(dropBehind
					).Build();
			}
			while (!this.cachingStrategy.CompareAndSet(prevStrategy, nextStrategy));
		}

		[VisibleForTesting]
		internal virtual ExtendedBlock GetBlock()
		{
			return streamer.GetBlock();
		}

		[VisibleForTesting]
		public virtual long GetFileId()
		{
			return fileId;
		}

		private static void Arraycopy<T>(T[] srcs, T[] dsts, int skipIndex)
		{
			System.Array.Copy(srcs, 0, dsts, 0, skipIndex);
			System.Array.Copy(srcs, skipIndex + 1, dsts, skipIndex, dsts.Length - skipIndex);
		}
	}
}
