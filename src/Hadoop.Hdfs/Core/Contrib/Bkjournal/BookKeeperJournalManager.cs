using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Protobuf;
using Org.Apache.Bookkeeper.Client;
using Org.Apache.Bookkeeper.Conf;
using Org.Apache.Bookkeeper.Util;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	/// <summary>
	/// BookKeeper Journal Manager
	/// To use, add the following to hdfs-site.xml.
	/// </summary>
	/// <remarks>
	/// BookKeeper Journal Manager
	/// To use, add the following to hdfs-site.xml.
	/// <pre>
	/// <c>
	/// &lt;property&gt;
	/// &lt;name&gt;dfs.namenode.edits.dir&lt;/name&gt;
	/// &lt;value&gt;bookkeeper://zk1:2181;zk2:2181;zk3:2181/hdfsjournal&lt;/value&gt;
	/// &lt;/property&gt;
	/// &lt;property&gt;
	/// &lt;name&gt;dfs.namenode.edits.journal-plugin.bookkeeper&lt;/name&gt;
	/// &lt;value&gt;org.apache.hadoop.contrib.bkjournal.BookKeeperJournalManager&lt;/value&gt;
	/// &lt;/property&gt;
	/// </c>
	/// </pre>
	/// The URI format for bookkeeper is bookkeeper://[zkEnsemble]/[rootZnode]
	/// [zookkeeper ensemble] is a list of semi-colon separated, zookeeper host:port
	/// pairs. In the example above there are 3 servers, in the ensemble,
	/// zk1, zk2 &amp; zk3, each one listening on port 2181.
	/// [root znode] is the path of the zookeeper znode, under which the editlog
	/// information will be stored.
	/// Other configuration options are:
	/// <ul>
	/// <li><b>dfs.namenode.bookkeeperjournal.output-buffer-size</b>
	/// Number of bytes a bookkeeper journal stream will buffer before
	/// forcing a flush. Default is 1024.</li>
	/// <li><b>dfs.namenode.bookkeeperjournal.ensemble-size</b>
	/// Number of bookkeeper servers in edit log ledger ensembles. This
	/// is the number of bookkeeper servers which need to be available
	/// for the ledger to be writable. Default is 3.</li>
	/// <li><b>dfs.namenode.bookkeeperjournal.quorum-size</b>
	/// Number of bookkeeper servers in the write quorum. This is the
	/// number of bookkeeper servers which must have acknowledged the
	/// write of an entry before it is considered written.
	/// Default is 2.</li>
	/// <li><b>dfs.namenode.bookkeeperjournal.digestPw</b>
	/// Password to use when creating ledgers. </li>
	/// <li><b>dfs.namenode.bookkeeperjournal.zk.session.timeout</b>
	/// Session timeout for Zookeeper client from BookKeeper Journal Manager.
	/// Hadoop recommends that, this value should be less than the ZKFC
	/// session timeout value. Default value is 3000.</li>
	/// </ul>
	/// </remarks>
	public class BookKeeperJournalManager : JournalManager
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Contrib.Bkjournal.BookKeeperJournalManager
			));

		public const string BkjmOutputBufferSize = "dfs.namenode.bookkeeperjournal.output-buffer-size";

		public const int BkjmOutputBufferSizeDefault = 1024;

		public const string BkjmBookkeeperEnsembleSize = "dfs.namenode.bookkeeperjournal.ensemble-size";

		public const int BkjmBookkeeperEnsembleSizeDefault = 3;

		public const string BkjmBookkeeperQuorumSize = "dfs.namenode.bookkeeperjournal.quorum-size";

		public const int BkjmBookkeeperQuorumSizeDefault = 2;

		public const string BkjmBookkeeperDigestPw = "dfs.namenode.bookkeeperjournal.digestPw";

		public const string BkjmBookkeeperDigestPwDefault = string.Empty;

		private const int BkjmLayoutVersion = -1;

		public const string BkjmZkSessionTimeout = "dfs.namenode.bookkeeperjournal.zk.session.timeout";

		public const int BkjmZkSessionTimeoutDefault = 3000;

		private const string BkjmEditInprogress = "inprogress_";

		public const string BkjmZkLedgersAvailablePath = "dfs.namenode.bookkeeperjournal.zk.availablebookies";

		public const string BkjmZkLedgersAvailablePathDefault = "/ledgers/available";

		public const string BkjmBookkeeperSpeculativeReadTimeoutMs = "dfs.namenode.bookkeeperjournal.speculativeReadTimeoutMs";

		public const int BkjmBookkeeperSpeculativeReadTimeoutDefault = 2000;

		public const string BkjmBookkeeperReadEntryTimeoutSec = "dfs.namenode.bookkeeperjournal.readEntryTimeoutSec";

		public const int BkjmBookkeeperReadEntryTimeoutDefault = 5;

		public const string BkjmBookkeeperAckQuorumSize = "dfs.namenode.bookkeeperjournal.ack.quorum-size";

		public const string BkjmBookkeeperAddEntryTimeoutSec = "dfs.namenode.bookkeeperjournal.addEntryTimeoutSec";

		public const int BkjmBookkeeperAddEntryTimeoutDefault = 5;

		private ZooKeeper zkc;

		private readonly Configuration conf;

		private readonly BookKeeper bkc;

		private readonly CurrentInprogress ci;

		private readonly string basePath;

		private readonly string ledgerPath;

		private readonly string versionPath;

		private readonly MaxTxId maxTxId;

		private readonly int ensembleSize;

		private readonly int quorumSize;

		private readonly int ackQuorumSize;

		private readonly int addEntryTimeout;

		private readonly string digestpw;

		private readonly int speculativeReadTimeout;

		private readonly int readEntryTimeout;

		private readonly CountDownLatch zkConnectLatch;

		private readonly NamespaceInfo nsInfo;

		private bool initialized = false;

		private LedgerHandle currentLedger = null;

		/// <summary>Construct a Bookkeeper journal manager.</summary>
		/// <exception cref="System.IO.IOException"/>
		public BookKeeperJournalManager(Configuration conf, URI uri, NamespaceInfo nsInfo
			)
		{
			this.conf = conf;
			this.nsInfo = nsInfo;
			string zkConnect = uri.GetAuthority().Replace(";", ",");
			basePath = uri.GetPath();
			ensembleSize = conf.GetInt(BkjmBookkeeperEnsembleSize, BkjmBookkeeperEnsembleSizeDefault
				);
			quorumSize = conf.GetInt(BkjmBookkeeperQuorumSize, BkjmBookkeeperQuorumSizeDefault
				);
			ackQuorumSize = conf.GetInt(BkjmBookkeeperAckQuorumSize, quorumSize);
			addEntryTimeout = conf.GetInt(BkjmBookkeeperAddEntryTimeoutSec, BkjmBookkeeperAddEntryTimeoutDefault
				);
			speculativeReadTimeout = conf.GetInt(BkjmBookkeeperSpeculativeReadTimeoutMs, BkjmBookkeeperSpeculativeReadTimeoutDefault
				);
			readEntryTimeout = conf.GetInt(BkjmBookkeeperReadEntryTimeoutSec, BkjmBookkeeperReadEntryTimeoutDefault
				);
			ledgerPath = basePath + "/ledgers";
			string maxTxIdPath = basePath + "/maxtxid";
			string currentInprogressNodePath = basePath + "/CurrentInprogress";
			versionPath = basePath + "/version";
			digestpw = conf.Get(BkjmBookkeeperDigestPw, BkjmBookkeeperDigestPwDefault);
			try
			{
				zkConnectLatch = new CountDownLatch(1);
				int bkjmZKSessionTimeout = conf.GetInt(BkjmZkSessionTimeout, BkjmZkSessionTimeoutDefault
					);
				zkc = new ZooKeeper(zkConnect, bkjmZKSessionTimeout, new BookKeeperJournalManager.ZkConnectionWatcher
					(this));
				// Configured zk session timeout + some extra grace period (here
				// BKJM_ZK_SESSION_TIMEOUT_DEFAULT used as grace period)
				int zkConnectionLatchTimeout = bkjmZKSessionTimeout + BkjmZkSessionTimeoutDefault;
				if (!zkConnectLatch.Await(zkConnectionLatchTimeout, TimeUnit.Milliseconds))
				{
					throw new IOException("Error connecting to zookeeper");
				}
				PrepareBookKeeperEnv();
				ClientConfiguration clientConf = new ClientConfiguration();
				clientConf.SetSpeculativeReadTimeout(speculativeReadTimeout);
				clientConf.SetReadEntryTimeout(readEntryTimeout);
				clientConf.SetAddEntryTimeout(addEntryTimeout);
				bkc = new BookKeeper(clientConf, zkc);
			}
			catch (KeeperException e)
			{
				throw new IOException("Error initializing zk", e);
			}
			catch (Exception ie)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
				throw new IOException("Interrupted while initializing bk journal manager", ie);
			}
			ci = new CurrentInprogress(zkc, currentInprogressNodePath);
			maxTxId = new MaxTxId(zkc, maxTxIdPath);
		}

		/// <summary>Pre-creating bookkeeper metadata path in zookeeper.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void PrepareBookKeeperEnv()
		{
			// create bookie available path in zookeeper if it doesn't exists
			string zkAvailablePath = conf.Get(BkjmZkLedgersAvailablePath, BkjmZkLedgersAvailablePathDefault
				);
			CountDownLatch zkPathLatch = new CountDownLatch(1);
			AtomicBoolean success = new AtomicBoolean(false);
			AsyncCallback.StringCallback callback = new _StringCallback_255(zkAvailablePath, 
				success, zkPathLatch);
			ZkUtils.AsyncCreateFullPathOptimistic(zkc, zkAvailablePath, new byte[0], ZooDefs.Ids
				.OpenAclUnsafe, CreateMode.Persistent, callback, null);
			try
			{
				if (!zkPathLatch.Await(zkc.GetSessionTimeout(), TimeUnit.Milliseconds) || !success
					.Get())
				{
					throw new IOException("Couldn't create bookie available path :" + zkAvailablePath
						 + ", timed out " + zkc.GetSessionTimeout() + " millis");
				}
			}
			catch (Exception e)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
				throw new IOException("Interrupted when creating the bookie available path : " + 
					zkAvailablePath, e);
			}
		}

		private sealed class _StringCallback_255 : AsyncCallback.StringCallback
		{
			public _StringCallback_255(string zkAvailablePath, AtomicBoolean success, CountDownLatch
				 zkPathLatch)
			{
				this.zkAvailablePath = zkAvailablePath;
				this.success = success;
				this.zkPathLatch = zkPathLatch;
			}

			public void ProcessResult(int rc, string path, object ctx, string name)
			{
				if (KeeperException.Code.Ok.IntValue() == rc || KeeperException.Code.Nodeexists.IntValue
					() == rc)
				{
					Org.Apache.Hadoop.Contrib.Bkjournal.BookKeeperJournalManager.Log.Info("Successfully created bookie available path : "
						 + zkAvailablePath);
					success.Set(true);
				}
				else
				{
					KeeperException.Code code = KeeperException.Code.Get(rc);
					Org.Apache.Hadoop.Contrib.Bkjournal.BookKeeperJournalManager.Log.Error("Error : "
						 + KeeperException.Create(code, path).Message + ", failed to create bookie available path : "
						 + zkAvailablePath);
				}
				zkPathLatch.CountDown();
			}

			private readonly string zkAvailablePath;

			private readonly AtomicBoolean success;

			private readonly CountDownLatch zkPathLatch;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Format(NamespaceInfo ns)
		{
			try
			{
				// delete old info
				Stat baseStat = null;
				Stat ledgerStat = null;
				if ((baseStat = zkc.Exists(basePath, false)) != null)
				{
					if ((ledgerStat = zkc.Exists(ledgerPath, false)) != null)
					{
						foreach (EditLogLedgerMetadata l in GetLedgerList(true))
						{
							try
							{
								bkc.DeleteLedger(l.GetLedgerId());
							}
							catch (BKException.BKNoSuchLedgerExistsException)
							{
								Log.Warn("Ledger " + l.GetLedgerId() + " does not exist;" + " Cannot delete.");
							}
						}
					}
					ZKUtil.DeleteRecursive(zkc, basePath);
				}
				// should be clean now.
				zkc.Create(basePath, new byte[] { (byte)('0') }, ZooDefs.Ids.OpenAclUnsafe, CreateMode
					.Persistent);
				BKJournalProtos.VersionProto.Builder builder = BKJournalProtos.VersionProto.NewBuilder
					();
				builder.SetNamespaceInfo(PBHelper.Convert(ns)).SetLayoutVersion(BkjmLayoutVersion
					);
				byte[] data = Sharpen.Runtime.GetBytesForString(TextFormat.PrintToString(((BKJournalProtos.VersionProto
					)builder.Build())), Charsets.Utf8);
				zkc.Create(versionPath, data, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Persistent);
				zkc.Create(ledgerPath, new byte[] { (byte)('0') }, ZooDefs.Ids.OpenAclUnsafe, CreateMode
					.Persistent);
			}
			catch (KeeperException ke)
			{
				Log.Error("Error accessing zookeeper to format", ke);
				throw new IOException("Error accessing zookeeper to format", ke);
			}
			catch (Exception ie)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
				throw new IOException("Interrupted during format", ie);
			}
			catch (BKException bke)
			{
				throw new IOException("Error cleaning up ledgers during format", bke);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool HasSomeData()
		{
			try
			{
				return zkc.Exists(basePath, false) != null;
			}
			catch (KeeperException ke)
			{
				throw new IOException("Couldn't contact zookeeper", ke);
			}
			catch (Exception ie)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
				throw new IOException("Interrupted while checking for data", ie);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckEnv()
		{
			lock (this)
			{
				if (!initialized)
				{
					try
					{
						Stat versionStat = zkc.Exists(versionPath, false);
						if (versionStat == null)
						{
							throw new IOException("Environment not initialized. " + "Have you forgotten to format?"
								);
						}
						byte[] d = zkc.GetData(versionPath, false, versionStat);
						BKJournalProtos.VersionProto.Builder builder = BKJournalProtos.VersionProto.NewBuilder
							();
						TextFormat.Merge(new string(d, Charsets.Utf8), builder);
						if (!builder.IsInitialized())
						{
							throw new IOException("Invalid/Incomplete data in znode");
						}
						BKJournalProtos.VersionProto vp = ((BKJournalProtos.VersionProto)builder.Build());
						// There's only one version at the moment
						System.Diagnostics.Debug.Assert(vp.GetLayoutVersion() == BkjmLayoutVersion);
						NamespaceInfo readns = PBHelper.Convert(vp.GetNamespaceInfo());
						if (nsInfo.GetNamespaceID() != readns.GetNamespaceID() || !nsInfo.clusterID.Equals
							(readns.GetClusterID()) || !nsInfo.GetBlockPoolID().Equals(readns.GetBlockPoolID
							()))
						{
							string err = string.Format("Environment mismatch. Running process %s" + ", stored in ZK %s"
								, nsInfo, readns);
							Log.Error(err);
							throw new IOException(err);
						}
						ci.Init();
						initialized = true;
					}
					catch (KeeperException ke)
					{
						throw new IOException("Cannot access ZooKeeper", ke);
					}
					catch (Exception ie)
					{
						Sharpen.Thread.CurrentThread().Interrupt();
						throw new IOException("Interrupted while checking environment", ie);
					}
				}
			}
		}

		/// <summary>Start a new log segment in a BookKeeper ledger.</summary>
		/// <remarks>
		/// Start a new log segment in a BookKeeper ledger.
		/// First ensure that we have the write lock for this journal.
		/// Then create a ledger and stream based on that ledger.
		/// The ledger id is written to the inprogress znode, so that in the
		/// case of a crash, a recovery process can find the ledger we were writing
		/// to when we crashed.
		/// </remarks>
		/// <param name="txId">First transaction id to be written to the stream</param>
		/// <exception cref="System.IO.IOException"/>
		public override EditLogOutputStream StartLogSegment(long txId, int layoutVersion)
		{
			CheckEnv();
			if (txId <= maxTxId.Get())
			{
				throw new IOException("We've already seen " + txId + ". A new stream cannot be created with it"
					);
			}
			try
			{
				string existingInprogressNode = ci.Read();
				if (null != existingInprogressNode && zkc.Exists(existingInprogressNode, false) !=
					 null)
				{
					throw new IOException("Inprogress node already exists");
				}
				if (currentLedger != null)
				{
					// bookkeeper errored on last stream, clean up ledger
					currentLedger.Close();
				}
				currentLedger = bkc.CreateLedger(ensembleSize, quorumSize, ackQuorumSize, BookKeeper.DigestType
					.Mac, Sharpen.Runtime.GetBytesForString(digestpw, Charsets.Utf8));
			}
			catch (BKException bke)
			{
				throw new IOException("Error creating ledger", bke);
			}
			catch (KeeperException ke)
			{
				throw new IOException("Error in zookeeper while creating ledger", ke);
			}
			catch (Exception ie)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
				throw new IOException("Interrupted creating ledger", ie);
			}
			try
			{
				string znodePath = InprogressZNode(txId);
				EditLogLedgerMetadata l = new EditLogLedgerMetadata(znodePath, layoutVersion, currentLedger
					.GetId(), txId);
				/* Write the ledger metadata out to the inprogress ledger znode
				* This can fail if for some reason our write lock has
				* expired (@see WriteLock) and another process has managed to
				* create the inprogress znode.
				* In this case, throw an exception. We don't want to continue
				* as this would lead to a split brain situation.
				*/
				l.Write(zkc, znodePath);
				maxTxId.Store(txId);
				ci.Update(znodePath);
				return new BookKeeperEditLogOutputStream(conf, currentLedger);
			}
			catch (KeeperException ke)
			{
				CleanupLedger(currentLedger);
				throw new IOException("Error storing ledger metadata", ke);
			}
		}

		private void CleanupLedger(LedgerHandle lh)
		{
			try
			{
				long id = currentLedger.GetId();
				currentLedger.Close();
				bkc.DeleteLedger(id);
			}
			catch (BKException bke)
			{
				//log & ignore, an IOException will be thrown soon
				Log.Error("Error closing ledger", bke);
			}
			catch (Exception ie)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
				Log.Warn("Interrupted while closing ledger", ie);
			}
		}

		/// <summary>Finalize a log segment.</summary>
		/// <remarks>
		/// Finalize a log segment. If the journal manager is currently
		/// writing to a ledger, ensure that this is the ledger of the log segment
		/// being finalized.
		/// Otherwise this is the recovery case. In the recovery case, ensure that
		/// the firstTxId of the ledger matches firstTxId for the segment we are
		/// trying to finalize.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void FinalizeLogSegment(long firstTxId, long lastTxId)
		{
			CheckEnv();
			string inprogressPath = InprogressZNode(firstTxId);
			try
			{
				Stat inprogressStat = zkc.Exists(inprogressPath, false);
				if (inprogressStat == null)
				{
					throw new IOException("Inprogress znode " + inprogressPath + " doesn't exist");
				}
				EditLogLedgerMetadata l = EditLogLedgerMetadata.Read(zkc, inprogressPath);
				if (currentLedger != null)
				{
					// normal, non-recovery case
					if (l.GetLedgerId() == currentLedger.GetId())
					{
						try
						{
							currentLedger.Close();
						}
						catch (BKException bke)
						{
							Log.Error("Error closing current ledger", bke);
						}
						currentLedger = null;
					}
					else
					{
						throw new IOException("Active ledger has different ID to inprogress. " + l.GetLedgerId
							() + " found, " + currentLedger.GetId() + " expected");
					}
				}
				if (l.GetFirstTxId() != firstTxId)
				{
					throw new IOException("Transaction id not as expected, " + l.GetFirstTxId() + " found, "
						 + firstTxId + " expected");
				}
				l.FinalizeLedger(lastTxId);
				string finalisedPath = FinalizedLedgerZNode(firstTxId, lastTxId);
				try
				{
					l.Write(zkc, finalisedPath);
				}
				catch (KeeperException.NodeExistsException)
				{
					if (!l.Verify(zkc, finalisedPath))
					{
						throw new IOException("Node " + finalisedPath + " already exists" + " but data doesn't match"
							);
					}
				}
				maxTxId.Store(lastTxId);
				zkc.Delete(inprogressPath, inprogressStat.GetVersion());
				string inprogressPathFromCI = ci.Read();
				if (inprogressPath.Equals(inprogressPathFromCI))
				{
					ci.Clear();
				}
			}
			catch (KeeperException e)
			{
				throw new IOException("Error finalising ledger", e);
			}
			catch (Exception ie)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
				throw new IOException("Error finalising ledger", ie);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SelectInputStreams(ICollection<EditLogInputStream> streams, long
			 fromTxId, bool inProgressOk)
		{
			IList<EditLogLedgerMetadata> currentLedgerList = GetLedgerList(fromTxId, inProgressOk
				);
			try
			{
				BookKeeperEditLogInputStream elis = null;
				foreach (EditLogLedgerMetadata l in currentLedgerList)
				{
					long lastTxId = l.GetLastTxId();
					if (l.IsInProgress())
					{
						lastTxId = RecoverLastTxId(l, false);
					}
					// Check once again, required in case of InProgress and is case of any
					// gap.
					if (fromTxId >= l.GetFirstTxId() && fromTxId <= lastTxId)
					{
						LedgerHandle h;
						if (l.IsInProgress())
						{
							// we don't want to fence the current journal
							h = bkc.OpenLedgerNoRecovery(l.GetLedgerId(), BookKeeper.DigestType.Mac, Sharpen.Runtime.GetBytesForString
								(digestpw, Charsets.Utf8));
						}
						else
						{
							h = bkc.OpenLedger(l.GetLedgerId(), BookKeeper.DigestType.Mac, Sharpen.Runtime.GetBytesForString
								(digestpw, Charsets.Utf8));
						}
						elis = new BookKeeperEditLogInputStream(h, l);
						elis.SkipTo(fromTxId);
					}
					else
					{
						// If mismatches then there might be some gap, so we should not check
						// further.
						return;
					}
					streams.AddItem(elis);
					if (elis.GetLastTxId() == HdfsConstants.InvalidTxid)
					{
						return;
					}
					fromTxId = elis.GetLastTxId() + 1;
				}
			}
			catch (BKException e)
			{
				throw new IOException("Could not open ledger for " + fromTxId, e);
			}
			catch (Exception ie)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
				throw new IOException("Interrupted opening ledger for " + fromTxId, ie);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long GetNumberOfTransactions(long fromTxId, bool inProgressOk)
		{
			long count = 0;
			long expectedStart = 0;
			foreach (EditLogLedgerMetadata l in GetLedgerList(inProgressOk))
			{
				long lastTxId = l.GetLastTxId();
				if (l.IsInProgress())
				{
					lastTxId = RecoverLastTxId(l, false);
					if (lastTxId == HdfsConstants.InvalidTxid)
					{
						break;
					}
				}
				System.Diagnostics.Debug.Assert(lastTxId >= l.GetFirstTxId());
				if (lastTxId < fromTxId)
				{
					continue;
				}
				else
				{
					if (l.GetFirstTxId() <= fromTxId && lastTxId >= fromTxId)
					{
						// we can start in the middle of a segment
						count = (lastTxId - l.GetFirstTxId()) + 1;
						expectedStart = lastTxId + 1;
					}
					else
					{
						if (expectedStart != l.GetFirstTxId())
						{
							if (count == 0)
							{
								throw new JournalManager.CorruptionException("StartTxId " + l.GetFirstTxId() + " is not as expected "
									 + expectedStart + ". Gap in transaction log?");
							}
							else
							{
								break;
							}
						}
						count += (lastTxId - l.GetFirstTxId()) + 1;
						expectedStart = lastTxId + 1;
					}
				}
			}
			return count;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RecoverUnfinalizedSegments()
		{
			CheckEnv();
			lock (this)
			{
				try
				{
					IList<string> children = zkc.GetChildren(ledgerPath, false);
					foreach (string child in children)
					{
						if (!child.StartsWith(BkjmEditInprogress))
						{
							continue;
						}
						string znode = ledgerPath + "/" + child;
						EditLogLedgerMetadata l = EditLogLedgerMetadata.Read(zkc, znode);
						try
						{
							long endTxId = RecoverLastTxId(l, true);
							if (endTxId == HdfsConstants.InvalidTxid)
							{
								Log.Error("Unrecoverable corruption has occurred in segment " + l.ToString() + " at path "
									 + znode + ". Unable to continue recovery.");
								throw new IOException("Unrecoverable corruption," + " please check logs.");
							}
							FinalizeLogSegment(l.GetFirstTxId(), endTxId);
						}
						catch (BookKeeperJournalManager.SegmentEmptyException)
						{
							Log.Warn("Inprogress znode " + child + " refers to a ledger which is empty. This occurs when the NN"
								 + " crashes after opening a segment, but before writing the" + " OP_START_LOG_SEGMENT op. It is safe to delete."
								 + " MetaData [" + l.ToString() + "]");
							// If the max seen transaction is the same as what would
							// have been the first transaction of the failed ledger,
							// decrement it, as that transaction never happened and as
							// such, is _not_ the last seen
							if (maxTxId.Get() == l.GetFirstTxId())
							{
								maxTxId.Reset(maxTxId.Get() - 1);
							}
							zkc.Delete(znode, -1);
						}
					}
				}
				catch (KeeperException.NoNodeException)
				{
				}
				catch (KeeperException ke)
				{
					// nothing to recover, ignore
					throw new IOException("Couldn't get list of inprogress segments", ke);
				}
				catch (Exception ie)
				{
					Sharpen.Thread.CurrentThread().Interrupt();
					throw new IOException("Interrupted getting list of inprogress segments", ie);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void PurgeLogsOlderThan(long minTxIdToKeep)
		{
			CheckEnv();
			foreach (EditLogLedgerMetadata l in GetLedgerList(false))
			{
				if (l.GetLastTxId() < minTxIdToKeep)
				{
					try
					{
						Stat stat = zkc.Exists(l.GetZkPath(), false);
						zkc.Delete(l.GetZkPath(), stat.GetVersion());
						bkc.DeleteLedger(l.GetLedgerId());
					}
					catch (Exception ie)
					{
						Sharpen.Thread.CurrentThread().Interrupt();
						Log.Error("Interrupted while purging " + l, ie);
					}
					catch (BKException bke)
					{
						Log.Error("Couldn't delete ledger from bookkeeper", bke);
					}
					catch (KeeperException ke)
					{
						Log.Error("Error deleting ledger entry in zookeeper", ke);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DiscardSegments(long startTxId)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoPreUpgrade()
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoUpgrade(Storage storage)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetJournalCTime()
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoFinalize()
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool CanRollBack(StorageInfo storage, StorageInfo prevStorage, int
			 targetLayoutVersion)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoRollback()
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			try
			{
				bkc.Close();
				zkc.Close();
			}
			catch (BKException bke)
			{
				throw new IOException("Couldn't close bookkeeper client", bke);
			}
			catch (Exception ie)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
				throw new IOException("Interrupted while closing journal manager", ie);
			}
		}

		/// <summary>Set the amount of memory that this stream should use to buffer edits.</summary>
		/// <remarks>
		/// Set the amount of memory that this stream should use to buffer edits.
		/// Setting this will only affect future output stream. Streams
		/// which have currently be created won't be affected.
		/// </remarks>
		public override void SetOutputBufferCapacity(int size)
		{
			conf.GetInt(BkjmOutputBufferSize, size);
		}

		/// <summary>
		/// Find the id of the last edit log transaction writen to a edit log
		/// ledger.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Contrib.Bkjournal.BookKeeperJournalManager.SegmentEmptyException
		/// 	"/>
		private long RecoverLastTxId(EditLogLedgerMetadata l, bool fence)
		{
			LedgerHandle lh = null;
			try
			{
				if (fence)
				{
					lh = bkc.OpenLedger(l.GetLedgerId(), BookKeeper.DigestType.Mac, Sharpen.Runtime.GetBytesForString
						(digestpw, Charsets.Utf8));
				}
				else
				{
					lh = bkc.OpenLedgerNoRecovery(l.GetLedgerId(), BookKeeper.DigestType.Mac, Sharpen.Runtime.GetBytesForString
						(digestpw, Charsets.Utf8));
				}
			}
			catch (BKException bke)
			{
				throw new IOException("Exception opening ledger for " + l, bke);
			}
			catch (Exception ie)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
				throw new IOException("Interrupted opening ledger for " + l, ie);
			}
			BookKeeperEditLogInputStream @in = null;
			try
			{
				long lastAddConfirmed = lh.GetLastAddConfirmed();
				if (lastAddConfirmed == -1)
				{
					throw new BookKeeperJournalManager.SegmentEmptyException();
				}
				@in = new BookKeeperEditLogInputStream(lh, l, lastAddConfirmed);
				long endTxId = HdfsConstants.InvalidTxid;
				FSEditLogOp op = @in.ReadOp();
				while (op != null)
				{
					if (endTxId == HdfsConstants.InvalidTxid || op.GetTransactionId() == endTxId + 1)
					{
						endTxId = op.GetTransactionId();
					}
					op = @in.ReadOp();
				}
				return endTxId;
			}
			finally
			{
				if (@in != null)
				{
					@in.Close();
				}
			}
		}

		/// <summary>Get a list of all segments in the journal.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual IList<EditLogLedgerMetadata> GetLedgerList(bool inProgressOk)
		{
			return GetLedgerList(-1, inProgressOk);
		}

		/// <exception cref="System.IO.IOException"/>
		private IList<EditLogLedgerMetadata> GetLedgerList(long fromTxId, bool inProgressOk
			)
		{
			IList<EditLogLedgerMetadata> ledgers = new AList<EditLogLedgerMetadata>();
			try
			{
				IList<string> ledgerNames = zkc.GetChildren(ledgerPath, false);
				foreach (string ledgerName in ledgerNames)
				{
					if (!inProgressOk && ledgerName.Contains(BkjmEditInprogress))
					{
						continue;
					}
					string legderMetadataPath = ledgerPath + "/" + ledgerName;
					try
					{
						EditLogLedgerMetadata editLogLedgerMetadata = EditLogLedgerMetadata.Read(zkc, legderMetadataPath
							);
						if (editLogLedgerMetadata.GetLastTxId() != HdfsConstants.InvalidTxid && editLogLedgerMetadata
							.GetLastTxId() < fromTxId)
						{
							// exclude already read closed edits, but include inprogress edits
							// as this will be handled in caller
							continue;
						}
						ledgers.AddItem(editLogLedgerMetadata);
					}
					catch (KeeperException.NoNodeException)
					{
						Log.Warn("ZNode: " + legderMetadataPath + " might have finalized and deleted." + 
							" So ignoring NoNodeException.");
					}
				}
			}
			catch (KeeperException e)
			{
				throw new IOException("Exception reading ledger list from zk", e);
			}
			catch (Exception ie)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
				throw new IOException("Interrupted getting list of ledgers from zk", ie);
			}
			ledgers.Sort(EditLogLedgerMetadata.Comparator);
			return ledgers;
		}

		/// <summary>Get the znode path for a finalize ledger</summary>
		internal virtual string FinalizedLedgerZNode(long startTxId, long endTxId)
		{
			return string.Format("%s/edits_%018d_%018d", ledgerPath, startTxId, endTxId);
		}

		/// <summary>Get the znode path for the inprogressZNode</summary>
		internal virtual string InprogressZNode(long startTxid)
		{
			return ledgerPath + "/inprogress_" + System.Convert.ToString(startTxid, 16);
		}

		[VisibleForTesting]
		internal virtual void SetZooKeeper(ZooKeeper zk)
		{
			this.zkc = zk;
		}

		/// <summary>Simple watcher to notify when zookeeper has connected</summary>
		private class ZkConnectionWatcher : Watcher
		{
			public override void Process(WatchedEvent @event)
			{
				if (Watcher.Event.KeeperState.SyncConnected.Equals(@event.GetState()))
				{
					this._enclosing.zkConnectLatch.CountDown();
				}
			}

			internal ZkConnectionWatcher(BookKeeperJournalManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly BookKeeperJournalManager _enclosing;
		}

		[System.Serializable]
		private class SegmentEmptyException : IOException
		{
		}
	}
}
