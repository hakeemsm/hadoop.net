using Org.Apache.Hadoop.Classification;


namespace Org.Apache.Hadoop.Metrics2
{
	/// <summary>The metrics system interface</summary>
	public abstract class MetricsSystem : MetricsSystemMXBean
	{
		[InterfaceAudience.Private]
		public abstract MetricsSystem Init(string prefix);

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
		public abstract T Register<T>(string name, string desc, T source);

		/// <summary>Unregister a metrics source</summary>
		/// <param name="name">of the source. This is the name you use to call register()</param>
		public abstract void UnregisterSource(string name);

		/// <summary>Register a metrics source (deriving name and description from the object)
		/// 	</summary>
		/// <?/>
		/// <param name="source">object to register</param>
		/// <returns>the source object</returns>
		/// <exception>MetricsException</exception>
		public virtual T Register<T>(T source)
		{
			return Register(null, null, source);
		}

		/// <param name="name">of the metrics source</param>
		/// <returns>the metrics source (potentially wrapped) object</returns>
		[InterfaceAudience.Private]
		public abstract MetricsSource GetSource(string name);

		/// <summary>Register a metrics sink</summary>
		/// <?/>
		/// <param name="sink">to register</param>
		/// <param name="name">of the sink. Must be unique.</param>
		/// <param name="desc">the description of the sink</param>
		/// <returns>the sink</returns>
		/// <exception>MetricsException</exception>
		public abstract T Register<T>(string name, string desc, T sink)
			where T : MetricsSink;

		/// <summary>Register a callback interface for JMX events</summary>
		/// <param name="callback">the callback object implementing the MBean interface.</param>
		public abstract void Register(MetricsSystem.Callback callback);

		/// <summary>Requests an immediate publish of all metrics from sources to sinks.</summary>
		/// <remarks>
		/// Requests an immediate publish of all metrics from sources to sinks.
		/// This is a "soft" request: the expectation is that a best effort will be
		/// done to synchronously snapshot the metrics from all the sources and put
		/// them in all the sinks (including flushing the sinks) before returning to
		/// the caller. If this can't be accomplished in reasonable time it's OK to
		/// return to the caller before everything is done.
		/// </remarks>
		public abstract void PublishMetricsNow();

		/// <summary>
		/// Shutdown the metrics system completely (usually during server shutdown.)
		/// The MetricsSystemMXBean will be unregistered.
		/// </summary>
		/// <returns>true if shutdown completed</returns>
		public abstract bool Shutdown();

		/// <summary>The metrics system callback interface (needed for proxies.)</summary>
		public interface Callback
		{
			/// <summary>Called before start()</summary>
			void PreStart();

			/// <summary>Called after start()</summary>
			void PostStart();

			/// <summary>Called before stop()</summary>
			void PreStop();

			/// <summary>Called after stop()</summary>
			void PostStop();
		}

		/// <summary>Convenient abstract class for implementing callback interface</summary>
		public abstract class AbstractCallback : MetricsSystem.Callback
		{
			public virtual void PreStart()
			{
			}

			public virtual void PostStart()
			{
			}

			public virtual void PreStop()
			{
			}

			public virtual void PostStop()
			{
			}
		}

		public abstract string CurrentConfig();

		public abstract void Start();

		public abstract void StartMetricsMBeans();

		public abstract void Stop();

		public abstract void StopMetricsMBeans();
	}
}
