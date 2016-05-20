using Sharpen;

namespace org.apache.hadoop.metrics2
{
	/// <summary>The metrics system interface</summary>
	public abstract class MetricsSystem : org.apache.hadoop.metrics2.MetricsSystemMXBean
	{
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public abstract org.apache.hadoop.metrics2.MetricsSystem init(string prefix);

		/// <summary>Register a metrics source</summary>
		/// <?/>
		/// <param name="source">object to register</param>
		/// <param name="name">
		/// of the source. Must be unique or null (then extracted from
		/// the annotations of the source object.)
		/// </param>
		/// <param name="desc">the description of the source (or null. See above.)</param>
		/// <returns>the source object</returns>
		/// <exception>MetricsException</exception>
		public abstract T register<T>(string name, string desc, T source);

		/// <summary>Unregister a metrics source</summary>
		/// <param name="name">of the source. This is the name you use to call register()</param>
		public abstract void unregisterSource(string name);

		/// <summary>Register a metrics source (deriving name and description from the object)
		/// 	</summary>
		/// <?/>
		/// <param name="source">object to register</param>
		/// <returns>the source object</returns>
		/// <exception>MetricsException</exception>
		public virtual T register<T>(T source)
		{
			return register(null, null, source);
		}

		/// <param name="name">of the metrics source</param>
		/// <returns>the metrics source (potentially wrapped) object</returns>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public abstract org.apache.hadoop.metrics2.MetricsSource getSource(string name);

		/// <summary>Register a metrics sink</summary>
		/// <?/>
		/// <param name="sink">to register</param>
		/// <param name="name">of the sink. Must be unique.</param>
		/// <param name="desc">the description of the sink</param>
		/// <returns>the sink</returns>
		/// <exception>MetricsException</exception>
		public abstract T register<T>(string name, string desc, T sink)
			where T : org.apache.hadoop.metrics2.MetricsSink;

		/// <summary>Register a callback interface for JMX events</summary>
		/// <param name="callback">the callback object implementing the MBean interface.</param>
		public abstract void register(org.apache.hadoop.metrics2.MetricsSystem.Callback callback
			);

		/// <summary>Requests an immediate publish of all metrics from sources to sinks.</summary>
		/// <remarks>
		/// Requests an immediate publish of all metrics from sources to sinks.
		/// This is a "soft" request: the expectation is that a best effort will be
		/// done to synchronously snapshot the metrics from all the sources and put
		/// them in all the sinks (including flushing the sinks) before returning to
		/// the caller. If this can't be accomplished in reasonable time it's OK to
		/// return to the caller before everything is done.
		/// </remarks>
		public abstract void publishMetricsNow();

		/// <summary>
		/// Shutdown the metrics system completely (usually during server shutdown.)
		/// The MetricsSystemMXBean will be unregistered.
		/// </summary>
		/// <returns>true if shutdown completed</returns>
		public abstract bool shutdown();

		/// <summary>The metrics system callback interface (needed for proxies.)</summary>
		public interface Callback
		{
			/// <summary>Called before start()</summary>
			void preStart();

			/// <summary>Called after start()</summary>
			void postStart();

			/// <summary>Called before stop()</summary>
			void preStop();

			/// <summary>Called after stop()</summary>
			void postStop();
		}

		/// <summary>Convenient abstract class for implementing callback interface</summary>
		public abstract class AbstractCallback : org.apache.hadoop.metrics2.MetricsSystem.Callback
		{
			public virtual void preStart()
			{
			}

			public virtual void postStart()
			{
			}

			public virtual void preStop()
			{
			}

			public virtual void postStop()
			{
			}
		}

		public abstract string currentConfig();

		public abstract void start();

		public abstract void startMetricsMBeans();

		public abstract void stop();

		public abstract void stopMetricsMBeans();
	}
}
