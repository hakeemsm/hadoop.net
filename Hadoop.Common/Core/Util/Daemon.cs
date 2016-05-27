using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// A thread that has called
	/// <see cref="Sharpen.Thread.SetDaemon(bool)"></see>
	/// with true.
	/// </summary>
	public class Daemon : Sharpen.Thread
	{
		/// <summary>
		/// Provide a factory for named daemon threads,
		/// for use in ExecutorServices constructors
		/// </summary>
		public class DaemonFactory : Daemon, ThreadFactory
		{
			// always a daemon
			public virtual Sharpen.Thread NewThread(Runnable runnable)
			{
				return new Daemon(runnable);
			}
		}

		internal Runnable runnable = null;

		/// <summary>Construct a daemon thread.</summary>
		public Daemon()
			: base()
		{
			{
				SetDaemon(true);
			}
		}

		/// <summary>Construct a daemon thread.</summary>
		public Daemon(Runnable runnable)
			: base(runnable)
		{
			{
				SetDaemon(true);
			}
			this.runnable = runnable;
			this.SetName(((object)runnable).ToString());
		}

		/// <summary>Construct a daemon thread to be part of a specified thread group.</summary>
		public Daemon(ThreadGroup group, Runnable runnable)
			: base(group, runnable)
		{
			{
				SetDaemon(true);
			}
			this.runnable = runnable;
			this.SetName(((object)runnable).ToString());
		}

		public virtual Runnable GetRunnable()
		{
			return runnable;
		}
	}
}
