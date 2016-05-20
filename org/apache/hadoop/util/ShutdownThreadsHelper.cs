using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>
	/// Helper class to shutdown
	/// <see cref="java.lang.Thread"/>
	/// s and
	/// <see cref="java.util.concurrent.ExecutorService"/>
	/// s.
	/// </summary>
	public class ShutdownThreadsHelper
	{
		private static org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.ShutdownThreadsHelper
			)));

		[com.google.common.annotations.VisibleForTesting]
		internal const int SHUTDOWN_WAIT_MS = 3000;

		/// <param name="thread">
		/// 
		/// <see cref="java.lang.Thread">to be shutdown</see>
		/// </param>
		/// <returns>
		/// <tt>true</tt> if the thread is successfully interrupted,
		/// <tt>false</tt> otherwise
		/// </returns>
		/// <exception cref="System.Exception"/>
		public static bool shutdownThread(java.lang.Thread thread)
		{
			return shutdownThread(thread, SHUTDOWN_WAIT_MS);
		}

		/// <param name="thread">
		/// 
		/// <see cref="java.lang.Thread">to be shutdown</see>
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
		public static bool shutdownThread(java.lang.Thread thread, long timeoutInMilliSeconds
			)
		{
			if (thread == null)
			{
				return true;
			}
			try
			{
				thread.interrupt();
				thread.join(timeoutInMilliSeconds);
				return true;
			}
			catch (System.Exception)
			{
				LOG.warn("Interrupted while shutting down thread - " + thread.getName());
				return false;
			}
		}

		/// <param name="service">
		/// 
		/// <see cref="java.util.concurrent.ExecutorService">to be shutdown</see>
		/// </param>
		/// <returns>
		/// <tt>true</tt> if the service is terminated,
		/// <tt>false</tt> otherwise
		/// </returns>
		/// <exception cref="System.Exception"/>
		public static bool shutdownExecutorService(java.util.concurrent.ExecutorService service
			)
		{
			return shutdownExecutorService(service, SHUTDOWN_WAIT_MS);
		}

		/// <param name="service">
		/// 
		/// <see cref="java.util.concurrent.ExecutorService">to be shutdown</see>
		/// </param>
		/// <param name="timeoutInMs">
		/// time to wait for
		/// <see cref="java.util.concurrent.ExecutorService.awaitTermination(long, java.util.concurrent.TimeUnit)
		/// 	"/>
		/// calls in milli seconds.
		/// </param>
		/// <returns>
		/// <tt>true</tt> if the service is terminated,
		/// <tt>false</tt> otherwise
		/// </returns>
		/// <exception cref="System.Exception"/>
		public static bool shutdownExecutorService(java.util.concurrent.ExecutorService service
			, long timeoutInMs)
		{
			if (service == null)
			{
				return true;
			}
			service.shutdown();
			if (!service.awaitTermination(timeoutInMs, java.util.concurrent.TimeUnit.MILLISECONDS
				))
			{
				service.shutdownNow();
				return service.awaitTermination(timeoutInMs, java.util.concurrent.TimeUnit.MILLISECONDS
					);
			}
			else
			{
				return true;
			}
		}
	}
}
