using System;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	/// <summary>
	/// Utility class for storing and reading
	/// the max seen txid in zookeeper
	/// </summary>
	internal class MaxTxId
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Contrib.Bkjournal.MaxTxId
			));

		private readonly ZooKeeper zkc;

		private readonly string path;

		private Stat currentStat;

		internal MaxTxId(ZooKeeper zkc, string path)
		{
			this.zkc = zkc;
			this.path = path;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Store(long maxTxId)
		{
			lock (this)
			{
				long currentMax = Get();
				if (currentMax < maxTxId)
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace("Setting maxTxId to " + maxTxId);
					}
					Reset(maxTxId);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Reset(long maxTxId)
		{
			lock (this)
			{
				try
				{
					BKJournalProtos.MaxTxIdProto.Builder builder = BKJournalProtos.MaxTxIdProto.NewBuilder
						().SetTxId(maxTxId);
					byte[] data = Sharpen.Runtime.GetBytesForString(TextFormat.PrintToString(((BKJournalProtos.MaxTxIdProto
						)builder.Build())), Charsets.Utf8);
					if (currentStat != null)
					{
						currentStat = zkc.SetData(path, data, currentStat.GetVersion());
					}
					else
					{
						zkc.Create(path, data, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Persistent);
					}
				}
				catch (KeeperException e)
				{
					throw new IOException("Error writing max tx id", e);
				}
				catch (Exception e)
				{
					throw new IOException("Interrupted while writing max tx id", e);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long Get()
		{
			lock (this)
			{
				try
				{
					currentStat = zkc.Exists(path, false);
					if (currentStat == null)
					{
						return 0;
					}
					else
					{
						byte[] bytes = zkc.GetData(path, false, currentStat);
						BKJournalProtos.MaxTxIdProto.Builder builder = BKJournalProtos.MaxTxIdProto.NewBuilder
							();
						TextFormat.Merge(new string(bytes, Charsets.Utf8), builder);
						if (!builder.IsInitialized())
						{
							throw new IOException("Invalid/Incomplete data in znode");
						}
						return ((BKJournalProtos.MaxTxIdProto)builder.Build()).GetTxId();
					}
				}
				catch (KeeperException e)
				{
					throw new IOException("Error reading the max tx id from zk", e);
				}
				catch (Exception ie)
				{
					throw new IOException("Interrupted while reading thr max tx id", ie);
				}
			}
		}
	}
}
