using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Interface used to abstract over classes which manage edit logs that may need
	/// to be purged.
	/// </summary>
	internal interface LogsPurgeable
	{
		/// <summary>
		/// Remove all edit logs with transaction IDs lower than the given transaction
		/// ID.
		/// </summary>
		/// <param name="minTxIdToKeep">the lowest transaction ID that should be retained</param>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		void PurgeLogsOlderThan(long minTxIdToKeep);

		/// <summary>Get a list of edit log input streams.</summary>
		/// <remarks>
		/// Get a list of edit log input streams.  The list will start with the
		/// stream that contains fromTxnId, and continue until the end of the journal
		/// being managed.
		/// </remarks>
		/// <param name="fromTxId">the first transaction id we want to read</param>
		/// <param name="inProgressOk">whether or not in-progress streams should be returned</param>
		/// <exception cref="System.IO.IOException">
		/// if the underlying storage has an error or is otherwise
		/// inaccessible
		/// </exception>
		void SelectInputStreams(ICollection<EditLogInputStream> streams, long fromTxId, bool
			 inProgressOk);
	}
}
