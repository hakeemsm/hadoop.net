using System;
using System.IO;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// A JournalManager is responsible for managing a single place of storing
	/// edit logs.
	/// </summary>
	/// <remarks>
	/// A JournalManager is responsible for managing a single place of storing
	/// edit logs. It may correspond to multiple files, a backup node, etc.
	/// Even when the actual underlying storage is rolled, or failed and restored,
	/// each conceptual place of storage corresponds to exactly one instance of
	/// this class, which is created when the EditLog is first opened.
	/// </remarks>
	public abstract class JournalManager : IDisposable, LogsPurgeable, Storage.FormatConfirmable
	{
		/// <summary>
		/// Format the underlying storage, removing any previously
		/// stored data.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Format(NamespaceInfo ns);

		/// <summary>
		/// Begin writing to a new segment of the log stream, which starts at
		/// the given transaction ID.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract EditLogOutputStream StartLogSegment(long txId, int layoutVersion);

		/// <summary>
		/// Mark the log segment that spans from firstTxId to lastTxId
		/// as finalized and complete.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void FinalizeLogSegment(long firstTxId, long lastTxId);

		/// <summary>Set the amount of memory that this stream should use to buffer edits</summary>
		public abstract void SetOutputBufferCapacity(int size);

		/// <summary>Recover segments which have not been finalized.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RecoverUnfinalizedSegments();

		/// <summary>
		/// Perform any steps that must succeed across all JournalManagers involved in
		/// an upgrade before proceeding onto the actual upgrade stage.
		/// </summary>
		/// <remarks>
		/// Perform any steps that must succeed across all JournalManagers involved in
		/// an upgrade before proceeding onto the actual upgrade stage. If a call to
		/// any JM's doPreUpgrade method fails, then doUpgrade will not be called for
		/// any JM.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void DoPreUpgrade();

		/// <summary>Perform the actual upgrade of the JM.</summary>
		/// <remarks>
		/// Perform the actual upgrade of the JM. After this is completed, the NN can
		/// begin to use the new upgraded metadata. This metadata may later be either
		/// finalized or rolled back to the previous state.
		/// </remarks>
		/// <param name="storage">info about the new upgraded versions.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void DoUpgrade(Storage storage);

		/// <summary>Finalize the upgrade.</summary>
		/// <remarks>
		/// Finalize the upgrade. JMs should purge any state that they had been keeping
		/// around during the upgrade process. After this is completed, rollback is no
		/// longer allowed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void DoFinalize();

		/// <summary>
		/// Return true if this JM can roll back to the previous storage state, false
		/// otherwise.
		/// </summary>
		/// <remarks>
		/// Return true if this JM can roll back to the previous storage state, false
		/// otherwise. The NN will refuse to run the rollback operation unless at least
		/// one JM or fsimage storage directory can roll back.
		/// </remarks>
		/// <param name="storage">the storage info for the current state</param>
		/// <param name="prevStorage">the storage info for the previous (unupgraded) state</param>
		/// <param name="targetLayoutVersion">the layout version we intend to roll back to</param>
		/// <returns>true if this JM can roll back, false otherwise.</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool CanRollBack(StorageInfo storage, StorageInfo prevStorage, int
			 targetLayoutVersion);

		/// <summary>Perform the rollback to the previous FS state.</summary>
		/// <remarks>
		/// Perform the rollback to the previous FS state. JMs which do not need to
		/// roll back their state should just return without error.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void DoRollback();

		/// <returns>the CTime of the journal manager.</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract long GetJournalCTime();

		/// <summary>Discard the segments whose first txid is &gt;= the given txid.</summary>
		/// <param name="startTxId">
		/// The given txid should be right at the segment boundary,
		/// i.e., it should be the first txid of some segment, if segment corresponding
		/// to the txid exists.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void DiscardSegments(long startTxId);

		/// <summary>Close the journal manager, freeing any resources it may hold.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Close();

		/// <summary>
		/// Indicate that a journal is cannot be used to load a certain range of
		/// edits.
		/// </summary>
		/// <remarks>
		/// Indicate that a journal is cannot be used to load a certain range of
		/// edits.
		/// This exception occurs in the case of a gap in the transactions, or a
		/// corrupt edit file.
		/// </remarks>
		[System.Serializable]
		public class CorruptionException : IOException
		{
			internal const long serialVersionUID = -4687802717006172702L;

			public CorruptionException(string reason)
				: base(reason)
			{
			}
		}
	}

	public static class JournalManagerConstants
	{
	}
}
