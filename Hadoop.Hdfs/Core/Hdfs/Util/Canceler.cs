using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>
	/// Provides a simple interface where one thread can mark an operation
	/// for cancellation, and another thread can poll for whether the
	/// cancellation has occurred.
	/// </summary>
	public class Canceler
	{
		/// <summary>
		/// If the operation has been canceled, set to the reason why
		/// it has been canceled (eg standby moving to active)
		/// </summary>
		private volatile string cancelReason = null;

		/// <summary>Requests that the current operation be canceled if it is still running.</summary>
		/// <remarks>
		/// Requests that the current operation be canceled if it is still running.
		/// This does not block until the cancellation is successful.
		/// </remarks>
		/// <param name="reason">the reason why cancellation is requested</param>
		public virtual void Cancel(string reason)
		{
			this.cancelReason = reason;
		}

		public virtual bool IsCancelled()
		{
			return cancelReason != null;
		}

		public virtual string GetCancellationReason()
		{
			return cancelReason;
		}
	}
}
