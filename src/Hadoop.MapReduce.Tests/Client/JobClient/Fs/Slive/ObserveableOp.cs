using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Operation which wraps a given operation and allows an observer to be notified
	/// when the operation is about to start and when the operation has finished
	/// </summary>
	internal class ObserveableOp : Operation
	{
		/// <summary>
		/// The observation interface which class that wish to monitor starting and
		/// ending events must implement.
		/// </summary>
		internal interface Observer
		{
			void NotifyStarting(Operation op);

			void NotifyFinished(Operation op);
		}

		private Operation op;

		private ObserveableOp.Observer observer;

		internal ObserveableOp(Operation op, ObserveableOp.Observer observer)
			: base(op.GetType(), op.GetConfig(), op.GetRandom())
		{
			this.op = op;
			this.observer = observer;
		}

		/// <summary>Proxy to underlying operation toString()</summary>
		public override string ToString()
		{
			return op.ToString();
		}

		internal override IList<OperationOutput> Run(FileSystem fs)
		{
			// Operation
			IList<OperationOutput> result = null;
			try
			{
				if (observer != null)
				{
					observer.NotifyStarting(op);
				}
				result = op.Run(fs);
			}
			finally
			{
				if (observer != null)
				{
					observer.NotifyFinished(op);
				}
			}
			return result;
		}
	}
}
