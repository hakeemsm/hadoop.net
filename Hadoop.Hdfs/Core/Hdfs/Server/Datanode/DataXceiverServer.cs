using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Server used for receiving/sending a block of data.</summary>
	/// <remarks>
	/// Server used for receiving/sending a block of data.
	/// This is created to listen for requests from clients or
	/// other DataNodes.  This small server does not use the
	/// Hadoop IPC mechanism.
	/// </remarks>
	internal class DataXceiverServer : Runnable
	{
		public static readonly Log Log = DataNode.Log;

		private readonly PeerServer peerServer;

		private readonly DataNode datanode;

		private readonly Dictionary<Peer, Sharpen.Thread> peers = new Dictionary<Peer, Sharpen.Thread
			>();

		private readonly Dictionary<Peer, DataXceiver> peersXceiver = new Dictionary<Peer
			, DataXceiver>();

		private bool closed = false;

		/// <summary>Maximal number of concurrent xceivers per node.</summary>
		/// <remarks>
		/// Maximal number of concurrent xceivers per node.
		/// Enforcing the limit is required in order to avoid data-node
		/// running out of memory.
		/// </remarks>
		internal int maxXceiverCount = DFSConfigKeys.DfsDatanodeMaxReceiverThreadsDefault;

		/// <summary>
		/// A manager to make sure that cluster balancing does not
		/// take too much resources.
		/// </summary>
		/// <remarks>
		/// A manager to make sure that cluster balancing does not
		/// take too much resources.
		/// It limits the number of block moves for balancing and
		/// the total amount of bandwidth they can use.
		/// </remarks>
		internal class BlockBalanceThrottler : DataTransferThrottler
		{
			private int numThreads;

			private int maxThreads;

			/// <summary>Constructor</summary>
			/// <param name="bandwidth">Total amount of bandwidth can be used for balancing</param>
			private BlockBalanceThrottler(long bandwidth, int maxThreads)
				: base(bandwidth)
			{
				this.maxThreads = maxThreads;
				Log.Info("Balancing bandwith is " + bandwidth + " bytes/s");
				Log.Info("Number threads for balancing is " + maxThreads);
			}

			/// <summary>Check if the block move can start.</summary>
			/// <remarks>
			/// Check if the block move can start.
			/// Return true if the thread quota is not exceeded and
			/// the counter is incremented; False otherwise.
			/// </remarks>
			internal virtual bool Acquire()
			{
				lock (this)
				{
					if (numThreads >= maxThreads)
					{
						return false;
					}
					numThreads++;
					return true;
				}
			}

			/// <summary>Mark that the move is completed.</summary>
			/// <remarks>Mark that the move is completed. The thread counter is decremented.</remarks>
			internal virtual void Release()
			{
				lock (this)
				{
					numThreads--;
				}
			}
		}

		internal readonly DataXceiverServer.BlockBalanceThrottler balanceThrottler;

		/// <summary>
		/// We need an estimate for block size to check if the disk partition has
		/// enough space.
		/// </summary>
		/// <remarks>
		/// We need an estimate for block size to check if the disk partition has
		/// enough space. Newer clients pass the expected block size to the DataNode.
		/// For older clients we just use the server-side default block size.
		/// </remarks>
		internal readonly long estimateBlockSize;

		internal DataXceiverServer(PeerServer peerServer, Configuration conf, DataNode datanode
			)
		{
			this.peerServer = peerServer;
			this.datanode = datanode;
			this.maxXceiverCount = conf.GetInt(DFSConfigKeys.DfsDatanodeMaxReceiverThreadsKey
				, DFSConfigKeys.DfsDatanodeMaxReceiverThreadsDefault);
			this.estimateBlockSize = conf.GetLongBytes(DFSConfigKeys.DfsBlockSizeKey, DFSConfigKeys
				.DfsBlockSizeDefault);
			//set up parameter for cluster balancing
			this.balanceThrottler = new DataXceiverServer.BlockBalanceThrottler(conf.GetLong(
				DFSConfigKeys.DfsDatanodeBalanceBandwidthpersecKey, DFSConfigKeys.DfsDatanodeBalanceBandwidthpersecDefault
				), conf.GetInt(DFSConfigKeys.DfsDatanodeBalanceMaxNumConcurrentMovesKey, DFSConfigKeys
				.DfsDatanodeBalanceMaxNumConcurrentMovesDefault));
		}

		public virtual void Run()
		{
			Peer peer = null;
			while (datanode.shouldRun && !datanode.shutdownForUpgrade)
			{
				try
				{
					peer = peerServer.Accept();
					// Make sure the xceiver count is not exceeded
					int curXceiverCount = datanode.GetXceiverCount();
					if (curXceiverCount > maxXceiverCount)
					{
						throw new IOException("Xceiver count " + curXceiverCount + " exceeds the limit of concurrent xcievers: "
							 + maxXceiverCount);
					}
					new Daemon(datanode.threadGroup, DataXceiver.Create(peer, datanode, this)).Start(
						);
				}
				catch (SocketTimeoutException)
				{
				}
				catch (AsynchronousCloseException ace)
				{
					// wake up to see if should continue to run
					// another thread closed our listener socket - that's expected during shutdown,
					// but not in other circumstances
					if (datanode.shouldRun && !datanode.shutdownForUpgrade)
					{
						Log.Warn(datanode.GetDisplayName() + ":DataXceiverServer: ", ace);
					}
				}
				catch (IOException ie)
				{
					IOUtils.Cleanup(null, peer);
					Log.Warn(datanode.GetDisplayName() + ":DataXceiverServer: ", ie);
				}
				catch (OutOfMemoryException ie)
				{
					IOUtils.Cleanup(null, peer);
					// DataNode can run out of memory if there is too many transfers.
					// Log the event, Sleep for 30 seconds, other transfers may complete by
					// then.
					Log.Error("DataNode is out of memory. Will retry in 30 seconds.", ie);
					try
					{
						Sharpen.Thread.Sleep(30 * 1000);
					}
					catch (Exception)
					{
					}
				}
				catch (Exception te)
				{
					// ignore
					Log.Error(datanode.GetDisplayName() + ":DataXceiverServer: Exiting due to: ", te);
					datanode.shouldRun = false;
				}
			}
			// Close the server to stop reception of more requests.
			try
			{
				peerServer.Close();
				closed = true;
			}
			catch (IOException ie)
			{
				Log.Warn(datanode.GetDisplayName() + " :DataXceiverServer: close exception", ie);
			}
			// if in restart prep stage, notify peers before closing them.
			if (datanode.shutdownForUpgrade)
			{
				RestartNotifyPeers();
				// Each thread needs some time to process it. If a thread needs
				// to send an OOB message to the client, but blocked on network for
				// long time, we need to force its termination.
				Log.Info("Shutting down DataXceiverServer before restart");
				// Allow roughly up to 2 seconds.
				for (int i = 0; GetNumPeers() > 0 && i < 10; i++)
				{
					try
					{
						Sharpen.Thread.Sleep(200);
					}
					catch (Exception)
					{
					}
				}
			}
			// ignore
			// Close all peers.
			CloseAllPeers();
		}

		internal virtual void Kill()
		{
			System.Diagnostics.Debug.Assert((datanode.shouldRun == false || datanode.shutdownForUpgrade
				), "shoudRun should be set to false or restarting should be true" + " before killing"
				);
			try
			{
				this.peerServer.Close();
				this.closed = true;
			}
			catch (IOException ie)
			{
				Log.Warn(datanode.GetDisplayName() + ":DataXceiverServer.kill(): ", ie);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void AddPeer(Peer peer, Sharpen.Thread t, DataXceiver xceiver)
		{
			lock (this)
			{
				if (closed)
				{
					throw new IOException("Server closed.");
				}
				peers[peer] = t;
				peersXceiver[peer] = xceiver;
			}
		}

		internal virtual void ClosePeer(Peer peer)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(peers, peer);
				Sharpen.Collections.Remove(peersXceiver, peer);
				IOUtils.Cleanup(null, peer);
			}
		}

		// Sending OOB to all peers
		public virtual void SendOOBToPeers()
		{
			lock (this)
			{
				if (!datanode.shutdownForUpgrade)
				{
					return;
				}
				foreach (Peer p in peers.Keys)
				{
					try
					{
						peersXceiver[p].SendOOB();
					}
					catch (IOException e)
					{
						Log.Warn("Got error when sending OOB message.", e);
					}
					catch (Exception)
					{
						Log.Warn("Interrupted when sending OOB message.");
					}
				}
			}
		}

		// Notify all peers of the shutdown and restart.
		// datanode.shouldRun should still be true and datanode.restarting should
		// be set true before calling this method.
		internal virtual void RestartNotifyPeers()
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert((datanode.shouldRun == true && datanode.shutdownForUpgrade
					));
				foreach (Sharpen.Thread t in peers.Values)
				{
					// interrupt each and every DataXceiver thread.
					t.Interrupt();
				}
			}
		}

		// Close all peers and clear the map.
		internal virtual void CloseAllPeers()
		{
			lock (this)
			{
				Log.Info("Closing all peers.");
				foreach (Peer p in peers.Keys)
				{
					IOUtils.Cleanup(Log, p);
				}
				peers.Clear();
				peersXceiver.Clear();
			}
		}

		// Return the number of peers.
		internal virtual int GetNumPeers()
		{
			lock (this)
			{
				return peers.Count;
			}
		}

		// Return the number of peers and DataXceivers.
		[VisibleForTesting]
		internal virtual int GetNumPeersXceiver()
		{
			lock (this)
			{
				return peersXceiver.Count;
			}
		}

		internal virtual void ReleasePeer(Peer peer)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(peers, peer);
				Sharpen.Collections.Remove(peersXceiver, peer);
			}
		}
	}
}
