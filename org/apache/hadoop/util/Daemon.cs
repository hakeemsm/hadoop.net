using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>
	/// A thread that has called
	/// <see cref="java.lang.Thread.setDaemon(bool)"></see>
	/// with true.
	/// </summary>
	public class Daemon : java.lang.Thread
	{
		/// <summary>
		/// Provide a factory for named daemon threads,
		/// for use in ExecutorServices constructors
		/// </summary>
		public class DaemonFactory : org.apache.hadoop.util.Daemon, java.util.concurrent.ThreadFactory
		{
			// always a daemon
			public virtual java.lang.Thread newThread(java.lang.Runnable runnable)
			{
				return new org.apache.hadoop.util.Daemon(runnable);
			}
		}

		internal java.lang.Runnable runnable = null;

		/// <summary>Construct a daemon thread.</summary>
		public Daemon()
			: base()
		{
			{
				setDaemon(true);
			}
		}

		/// <summary>Construct a daemon thread.</summary>
		public Daemon(java.lang.Runnable runnable)
			: base(runnable)
		{
			{
				setDaemon(true);
			}
			this.runnable = runnable;
			this.setName(((object)runnable).ToString());
		}

		/// <summary>Construct a daemon thread to be part of a specified thread group.</summary>
		public Daemon(java.lang.ThreadGroup group, java.lang.Runnable runnable)
			: base(group, runnable)
		{
			{
				setDaemon(true);
			}
			this.runnable = runnable;
			this.setName(((object)runnable).ToString());
		}

		public virtual java.lang.Runnable getRunnable()
		{
			return runnable;
		}
	}
}
