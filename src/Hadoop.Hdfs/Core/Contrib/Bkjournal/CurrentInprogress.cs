using System;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	/// <summary>Distributed write permission lock, using ZooKeeper.</summary>
	/// <remarks>
	/// Distributed write permission lock, using ZooKeeper. Read the version number
	/// and return the current inprogress node path available in CurrentInprogress
	/// path. If it exist, caller can treat that some other client already operating
	/// on it. Then caller can take action. If there is no inprogress node exist,
	/// then caller can treat that there is no client operating on it. Later same
	/// caller should update the his newly created inprogress node path. At this
	/// point, if some other activities done on this node, version number might
	/// change, so update will fail. So, this read, update api will ensure that there
	/// is only node can continue further after checking with CurrentInprogress.
	/// </remarks>
	internal class CurrentInprogress
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Contrib.Bkjournal.CurrentInprogress
			));

		private readonly ZooKeeper zkc;

		private readonly string currentInprogressNode;

		private volatile int versionNumberForPermission = -1;

		private readonly string hostName = Sharpen.Runtime.GetLocalHost().ToString();

		/// <exception cref="System.IO.IOException"/>
		internal CurrentInprogress(ZooKeeper zkc, string lockpath)
		{
			this.currentInprogressNode = lockpath;
			this.zkc = zkc;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Init()
		{
			try
			{
				Stat isCurrentInprogressNodeExists = zkc.Exists(currentInprogressNode, false);
				if (isCurrentInprogressNodeExists == null)
				{
					try
					{
						zkc.Create(currentInprogressNode, null, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Persistent
							);
					}
					catch (KeeperException.NodeExistsException e)
					{
						// Node might created by other process at the same time. Ignore it.
						if (Log.IsDebugEnabled())
						{
							Log.Debug(currentInprogressNode + " already created by other process.", e);
						}
					}
				}
			}
			catch (KeeperException e)
			{
				throw new IOException("Exception accessing Zookeeper", e);
			}
			catch (Exception ie)
			{
				throw new IOException("Interrupted accessing Zookeeper", ie);
			}
		}

		/// <summary>Update the path with prepending version number and hostname</summary>
		/// <param name="path">- to be updated in zookeeper</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void Update(string path)
		{
			BKJournalProtos.CurrentInprogressProto.Builder builder = BKJournalProtos.CurrentInprogressProto
				.NewBuilder();
			builder.SetPath(path).SetHostname(hostName);
			string content = TextFormat.PrintToString(((BKJournalProtos.CurrentInprogressProto
				)builder.Build()));
			try
			{
				zkc.SetData(this.currentInprogressNode, Sharpen.Runtime.GetBytesForString(content
					, Charsets.Utf8), this.versionNumberForPermission);
			}
			catch (KeeperException e)
			{
				throw new IOException("Exception when setting the data " + "[" + content + "] to CurrentInprogress. "
					, e);
			}
			catch (Exception e)
			{
				throw new IOException("Interrupted while setting the data " + "[" + content + "] to CurrentInprogress"
					, e);
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Updated data[" + content + "] to CurrentInprogress");
			}
		}

		/// <summary>
		/// Read the CurrentInprogress node data from Zookeeper and also get the znode
		/// version number.
		/// </summary>
		/// <remarks>
		/// Read the CurrentInprogress node data from Zookeeper and also get the znode
		/// version number. Return the 3rd field from the data. i.e saved path with
		/// #update api
		/// </remarks>
		/// <returns>available inprogress node path. returns null if not available.</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual string Read()
		{
			Stat stat = new Stat();
			byte[] data = null;
			try
			{
				data = zkc.GetData(this.currentInprogressNode, false, stat);
			}
			catch (KeeperException e)
			{
				throw new IOException("Exception while reading the data from " + currentInprogressNode
					, e);
			}
			catch (Exception e)
			{
				throw new IOException("Interrupted while reading data from " + currentInprogressNode
					, e);
			}
			this.versionNumberForPermission = stat.GetVersion();
			if (data != null)
			{
				BKJournalProtos.CurrentInprogressProto.Builder builder = BKJournalProtos.CurrentInprogressProto
					.NewBuilder();
				TextFormat.Merge(new string(data, Charsets.Utf8), builder);
				if (!builder.IsInitialized())
				{
					throw new IOException("Invalid/Incomplete data in znode");
				}
				return ((BKJournalProtos.CurrentInprogressProto)builder.Build()).GetPath();
			}
			else
			{
				Log.Debug("No data available in CurrentInprogress");
			}
			return null;
		}

		/// <summary>Clear the CurrentInprogress node data</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void Clear()
		{
			try
			{
				zkc.SetData(this.currentInprogressNode, null, versionNumberForPermission);
			}
			catch (KeeperException e)
			{
				throw new IOException("Exception when setting the data to CurrentInprogress node"
					, e);
			}
			catch (Exception e)
			{
				throw new IOException("Interrupted when setting the data to CurrentInprogress node"
					, e);
			}
			Log.Debug("Cleared the data from CurrentInprogress");
		}
	}
}
