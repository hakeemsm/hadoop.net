using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Zookeeper;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	/// <summary>
	/// Utility class for storing the metadata associated
	/// with a single edit log segment, stored in a single ledger
	/// </summary>
	public class EditLogLedgerMetadata
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Contrib.Bkjournal.EditLogLedgerMetadata
			));

		private string zkPath;

		private readonly int dataLayoutVersion;

		private readonly long ledgerId;

		private readonly long firstTxId;

		private long lastTxId;

		private bool inprogress;

		private sealed class _IComparer_51 : IComparer<Org.Apache.Hadoop.Contrib.Bkjournal.EditLogLedgerMetadata
			>
		{
			public _IComparer_51()
			{
			}

			public int Compare(Org.Apache.Hadoop.Contrib.Bkjournal.EditLogLedgerMetadata o1, 
				Org.Apache.Hadoop.Contrib.Bkjournal.EditLogLedgerMetadata o2)
			{
				if (o1.firstTxId < o2.firstTxId)
				{
					return -1;
				}
				else
				{
					if (o1.firstTxId == o2.firstTxId)
					{
						return 0;
					}
					else
					{
						return 1;
					}
				}
			}
		}

		public static readonly IEnumerator Comparator = new _IComparer_51();

		internal EditLogLedgerMetadata(string zkPath, int dataLayoutVersion, long ledgerId
			, long firstTxId)
		{
			this.zkPath = zkPath;
			this.dataLayoutVersion = dataLayoutVersion;
			this.ledgerId = ledgerId;
			this.firstTxId = firstTxId;
			this.lastTxId = HdfsConstants.InvalidTxid;
			this.inprogress = true;
		}

		internal EditLogLedgerMetadata(string zkPath, int dataLayoutVersion, long ledgerId
			, long firstTxId, long lastTxId)
		{
			this.zkPath = zkPath;
			this.dataLayoutVersion = dataLayoutVersion;
			this.ledgerId = ledgerId;
			this.firstTxId = firstTxId;
			this.lastTxId = lastTxId;
			this.inprogress = false;
		}

		internal virtual string GetZkPath()
		{
			return zkPath;
		}

		internal virtual long GetFirstTxId()
		{
			return firstTxId;
		}

		internal virtual long GetLastTxId()
		{
			return lastTxId;
		}

		internal virtual long GetLedgerId()
		{
			return ledgerId;
		}

		internal virtual bool IsInProgress()
		{
			return this.inprogress;
		}

		internal virtual int GetDataLayoutVersion()
		{
			return this.dataLayoutVersion;
		}

		internal virtual void FinalizeLedger(long newLastTxId)
		{
			System.Diagnostics.Debug.Assert(this.lastTxId == HdfsConstants.InvalidTxid);
			this.lastTxId = newLastTxId;
			this.inprogress = false;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException.NoNodeException"/>
		internal static Org.Apache.Hadoop.Contrib.Bkjournal.EditLogLedgerMetadata Read(ZooKeeper
			 zkc, string path)
		{
			try
			{
				byte[] data = zkc.GetData(path, false, null);
				BKJournalProtos.EditLogLedgerProto.Builder builder = BKJournalProtos.EditLogLedgerProto
					.NewBuilder();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Reading " + path + " data: " + new string(data, Charsets.Utf8));
				}
				TextFormat.Merge(new string(data, Charsets.Utf8), builder);
				if (!builder.IsInitialized())
				{
					throw new IOException("Invalid/Incomplete data in znode");
				}
				BKJournalProtos.EditLogLedgerProto ledger = ((BKJournalProtos.EditLogLedgerProto)
					builder.Build());
				int dataLayoutVersion = ledger.GetDataLayoutVersion();
				long ledgerId = ledger.GetLedgerId();
				long firstTxId = ledger.GetFirstTxId();
				if (ledger.HasLastTxId())
				{
					long lastTxId = ledger.GetLastTxId();
					return new Org.Apache.Hadoop.Contrib.Bkjournal.EditLogLedgerMetadata(path, dataLayoutVersion
						, ledgerId, firstTxId, lastTxId);
				}
				else
				{
					return new Org.Apache.Hadoop.Contrib.Bkjournal.EditLogLedgerMetadata(path, dataLayoutVersion
						, ledgerId, firstTxId);
				}
			}
			catch (KeeperException.NoNodeException nne)
			{
				throw;
			}
			catch (KeeperException ke)
			{
				throw new IOException("Error reading from zookeeper", ke);
			}
			catch (Exception ie)
			{
				throw new IOException("Interrupted reading from zookeeper", ie);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException.NodeExistsException"/>
		internal virtual void Write(ZooKeeper zkc, string path)
		{
			this.zkPath = path;
			BKJournalProtos.EditLogLedgerProto.Builder builder = BKJournalProtos.EditLogLedgerProto
				.NewBuilder();
			builder.SetDataLayoutVersion(dataLayoutVersion).SetLedgerId(ledgerId).SetFirstTxId
				(firstTxId);
			if (!inprogress)
			{
				builder.SetLastTxId(lastTxId);
			}
			try
			{
				zkc.Create(path, Sharpen.Runtime.GetBytesForString(TextFormat.PrintToString(((BKJournalProtos.EditLogLedgerProto
					)builder.Build())), Charsets.Utf8), ZooDefs.Ids.OpenAclUnsafe, CreateMode.Persistent
					);
			}
			catch (KeeperException.NodeExistsException nee)
			{
				throw;
			}
			catch (KeeperException e)
			{
				throw new IOException("Error creating ledger znode", e);
			}
			catch (Exception ie)
			{
				throw new IOException("Interrupted creating ledger znode", ie);
			}
		}

		internal virtual bool Verify(ZooKeeper zkc, string path)
		{
			try
			{
				Org.Apache.Hadoop.Contrib.Bkjournal.EditLogLedgerMetadata other = Read(zkc, path);
				if (Log.IsTraceEnabled())
				{
					Log.Trace("Verifying " + this.ToString() + " against " + other);
				}
				return other.Equals(this);
			}
			catch (KeeperException e)
			{
				Log.Error("Couldn't verify data in " + path, e);
				return false;
			}
			catch (IOException ie)
			{
				Log.Error("Couldn't verify data in " + path, ie);
				return false;
			}
		}

		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.Contrib.Bkjournal.EditLogLedgerMetadata))
			{
				return false;
			}
			Org.Apache.Hadoop.Contrib.Bkjournal.EditLogLedgerMetadata ol = (Org.Apache.Hadoop.Contrib.Bkjournal.EditLogLedgerMetadata
				)o;
			return ledgerId == ol.ledgerId && dataLayoutVersion == ol.dataLayoutVersion && firstTxId
				 == ol.firstTxId && lastTxId == ol.lastTxId;
		}

		public override int GetHashCode()
		{
			int hash = 1;
			hash = hash * 31 + (int)ledgerId;
			hash = hash * 31 + (int)firstTxId;
			hash = hash * 31 + (int)lastTxId;
			hash = hash * 31 + dataLayoutVersion;
			return hash;
		}

		public override string ToString()
		{
			return "[LedgerId:" + ledgerId + ", firstTxId:" + firstTxId + ", lastTxId:" + lastTxId
				 + ", dataLayoutVersion:" + dataLayoutVersion + "]";
		}
	}
}
