using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>Service plug-in interface.</summary>
	/// <remarks>
	/// Service plug-in interface.
	/// Service plug-ins may be used to expose functionality of datanodes or
	/// namenodes using arbitrary RPC protocols. Plug-ins are instantiated by the
	/// service instance, and are notified of service life-cycle events using the
	/// methods defined by this class.
	/// Service plug-ins are started after the service instance is started, and
	/// stopped before the service instance is stopped.
	/// </remarks>
	public interface ServicePlugin : java.io.Closeable
	{
		/// <summary>This method is invoked when the service instance has been started.</summary>
		/// <param name="service">The service instance invoking this method</param>
		void start(object service);

		/// <summary>This method is invoked when the service instance is about to be shut down.
		/// 	</summary>
		void stop();
	}
}
