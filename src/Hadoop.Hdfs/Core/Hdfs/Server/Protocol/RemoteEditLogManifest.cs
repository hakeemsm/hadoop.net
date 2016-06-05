using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>An enumeration of logs available on a remote NameNode.</summary>
	public class RemoteEditLogManifest
	{
		private IList<RemoteEditLog> logs;

		public RemoteEditLogManifest()
		{
		}

		public RemoteEditLogManifest(IList<RemoteEditLog> logs)
		{
			this.logs = logs;
			CheckState();
		}

		/// <summary>
		/// Check that the logs are non-overlapping sequences of transactions,
		/// in sorted order.
		/// </summary>
		/// <remarks>
		/// Check that the logs are non-overlapping sequences of transactions,
		/// in sorted order. They do not need to be contiguous.
		/// </remarks>
		/// <exception cref="System.InvalidOperationException">if incorrect</exception>
		private void CheckState()
		{
			Preconditions.CheckNotNull(logs);
			RemoteEditLog prev = null;
			foreach (RemoteEditLog log in logs)
			{
				if (prev != null)
				{
					if (log.GetStartTxId() <= prev.GetEndTxId())
					{
						throw new InvalidOperationException("Invalid log manifest (log " + log + " overlaps "
							 + prev + ")\n" + this);
					}
				}
				prev = log;
			}
		}

		public virtual IList<RemoteEditLog> GetLogs()
		{
			return Sharpen.Collections.UnmodifiableList(logs);
		}

		public override string ToString()
		{
			return "[" + Joiner.On(", ").Join(logs) + "]";
		}
	}
}
