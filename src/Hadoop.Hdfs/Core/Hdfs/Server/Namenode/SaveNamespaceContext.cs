using System.Collections.Generic;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Context for an ongoing SaveNamespace operation.</summary>
	/// <remarks>
	/// Context for an ongoing SaveNamespace operation. This class
	/// allows cancellation, and also is responsible for accumulating
	/// failed storage directories.
	/// </remarks>
	public class SaveNamespaceContext
	{
		private readonly FSNamesystem sourceNamesystem;

		private readonly long txid;

		private readonly IList<Storage.StorageDirectory> errorSDs = Sharpen.Collections.SynchronizedList
			(new AList<Storage.StorageDirectory>());

		private readonly Canceler canceller;

		private readonly CountDownLatch completionLatch = new CountDownLatch(1);

		internal SaveNamespaceContext(FSNamesystem sourceNamesystem, long txid, Canceler 
			canceller)
		{
			this.sourceNamesystem = sourceNamesystem;
			this.txid = txid;
			this.canceller = canceller;
		}

		internal virtual FSNamesystem GetSourceNamesystem()
		{
			return sourceNamesystem;
		}

		internal virtual long GetTxId()
		{
			return txid;
		}

		internal virtual void ReportErrorOnStorageDirectory(Storage.StorageDirectory sd)
		{
			errorSDs.AddItem(sd);
		}

		internal virtual IList<Storage.StorageDirectory> GetErrorSDs()
		{
			return errorSDs;
		}

		internal virtual void MarkComplete()
		{
			Preconditions.CheckState(completionLatch.GetCount() == 1, "Context already completed!"
				);
			completionLatch.CountDown();
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SaveNamespaceCancelledException
		/// 	"/>
		public virtual void CheckCancelled()
		{
			if (canceller.IsCancelled())
			{
				throw new SaveNamespaceCancelledException(canceller.GetCancellationReason());
			}
		}
	}
}
