using System;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// Helper class to shutdown
	/// <see cref="Thread"/>
	/// s and
	/// <see cref="ExecutorService"/>
	/// s.
	/// </summary>
	public class ShutdownThreadsHelper
	{
		private static Log Log = LogFactory.GetLog(typeof(ShutdownThreadsHelper));

		[VisibleForTesting]
		internal const int ShutdownWaitMs = 3000;

		/// <param name="thread">
		/// 
		/// <see cref="Thread">to be shutdown</see>
		/// </param>
		/// <returns>
		/// <tt>true</tt> if the thread is successfully interrupted,
		/// <tt>false</tt> otherwise
		/// </returns>
		/// <exception cref="System.Exception"/>
		public static bool ShutdownThread(Thread thread)
		{
			return ShutdownThread(thread, ShutdownWaitMs);
		}

		/// <param name="thread">
		/// 
		/// <see cref="Thread">to be shutdown</see>
		/// </param>
		/// <param name="timeoutInMilliSeconds">
		/// time to wait for thread to join after being
		/// interrupted
		/// </param>
		/// <returns>
		/// <tt>true</tt> if the thread is successfully interrupted,
		/// <tt>false</tt> otherwise
		/// </returns>
		/// <exception cref="System.Exception"/>
		public static bool ShutdownThread(Thread thread, long timeoutInMilliSeconds
			)
		{
			if (thread == null)
			{
				return true;
			}
			try
			{
				thread.Interrupt();
				thread.Join(timeoutInMilliSeconds);
				return true;
			}
			catch (Exception)
			{
				Log.Warn("Interrupted while shutting down thread - " + thread.GetName());
				return false;
			}
		}

		/// <param name="service">
		/// 
		/// <see cref="ExecutorService">to be shutdown</see>
		/// </param>
		/// <returns>
		/// <tt>true</tt> if the service is terminated,
		/// <tt>false</tt> otherwise
		/// </returns>
		/// <exception cref="System.Exception"/>
		public static bool ShutdownExecutorService(ExecutorService service)
		{
			return ShutdownExecutorService(service, ShutdownWaitMs);
		}

		/// <param name="service">
		/// 
		/// <see cref="ExecutorService">to be shutdown</see>
		/// </param>
		/// <param name="timeoutInMs">
		/// time to wait for
		/// <see cref="ExecutorService.AwaitTermination(long, TimeUnit)"/>
		/// calls in milli seconds.
		/// </param>
		/// <returns>
		/// <tt>true</tt> if the service is terminated,
		/// <tt>false</tt> otherwise
		/// </returns>
		/// <exception cref="System.Exception"/>
		public static bool ShutdownExecutorService(ExecutorService service, long timeoutInMs
			)
		{
			if (service == null)
			{
				return true;
			}
			service.Shutdown();
			if (!service.AwaitTermination(timeoutInMs, TimeUnit.Milliseconds))
			{
				service.ShutdownNow();
				return service.AwaitTermination(timeoutInMs, TimeUnit.Milliseconds);
			}
			else
			{
				return true;
			}
		}
	}
}
