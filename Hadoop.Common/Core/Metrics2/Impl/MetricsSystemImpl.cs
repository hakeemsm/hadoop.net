using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Reflection;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Javax.Management;
using Org.Apache.Commons.Configuration;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Math3.Util;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>A base class for metrics system singletons</summary>
	public class MetricsSystemImpl : MetricsSystem, MetricsSource
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Metrics2.Impl.MetricsSystemImpl
			));

		internal const string MsName = "MetricsSystem";

		internal const string MsStatsName = MsName + ",sub=Stats";

		internal const string MsStatsDesc = "Metrics system metrics";

		internal const string MsControlName = MsName + ",sub=Control";

		internal const string MsInitModeKey = "hadoop.metrics.init.mode";

		internal enum InitMode
		{
			Normal,
			Standby
		}

		private readonly IDictionary<string, MetricsSourceAdapter> sources;

		private readonly IDictionary<string, MetricsSource> allSources;

		private readonly IDictionary<string, MetricsSinkAdapter> sinks;

		private readonly IDictionary<string, MetricsSink> allSinks;

		private readonly IList<MetricsSystem.Callback> callbacks;

		private readonly IDictionary<string, MetricsSystem.Callback> namedCallbacks;

		private readonly MetricsCollectorImpl collector;

		private readonly MetricsRegistry registry = new MetricsRegistry(MsName);

		internal MutableStat snapshotStat;

		internal MutableStat publishStat;

		internal MutableCounterLong droppedPubAll;

		private readonly IList<MetricsTag> injectedTags;

		private string prefix;

		private MetricsFilter sourceFilter;

		private MetricsConfig config;

		private IDictionary<string, MetricsConfig> sourceConfigs;

		private IDictionary<string, MetricsConfig> sinkConfigs;

		private bool monitoring = false;

		private Timer timer;

		private int period;

		private long logicalTime;

		private ObjectName mbeanName;

		private bool publishSelfMetrics = true;

		private MetricsSourceAdapter sysSource;

		private int refCount = 0;

		/// <summary>Construct the metrics system</summary>
		/// <param name="prefix">for the system</param>
		public MetricsSystemImpl(string prefix)
		{
			// The callback list is used by register(Callback callback), while
			// the callback map is used by register(String name, String desc, T sink)
			// Things that are changed by init()/start()/stop()
			// seconds
			// number of timer invocations * period
			// for mini cluster mode
			this.prefix = prefix;
			allSources = Maps.NewHashMap();
			sources = Maps.NewLinkedHashMap();
			allSinks = Maps.NewHashMap();
			sinks = Maps.NewLinkedHashMap();
			sourceConfigs = Maps.NewHashMap();
			sinkConfigs = Maps.NewHashMap();
			callbacks = Lists.NewArrayList();
			namedCallbacks = Maps.NewHashMap();
			injectedTags = Lists.NewArrayList();
			collector = new MetricsCollectorImpl();
			if (prefix != null)
			{
				// prefix could be null for default ctor, which requires init later
				InitSystemMBean();
			}
		}

		/// <summary>Construct the system but not initializing (read config etc.) it.</summary>
		public MetricsSystemImpl()
			: this(null)
		{
		}

		/// <summary>Initialized the metrics system with a prefix.</summary>
		/// <param name="prefix">the system will look for configs with the prefix</param>
		/// <returns>the metrics system object itself</returns>
		public override MetricsSystem Init(string prefix)
		{
			lock (this)
			{
				if (monitoring && !DefaultMetricsSystem.InMiniClusterMode())
				{
					Log.Warn(this.prefix + " metrics system already initialized!");
					return this;
				}
				this.prefix = Preconditions.CheckNotNull(prefix, "prefix");
				++refCount;
				if (monitoring)
				{
					// in mini cluster mode
					Log.Info(this.prefix + " metrics system started (again)");
					return this;
				}
				switch (InitMode())
				{
					case MetricsSystemImpl.InitMode.Normal:
					{
						try
						{
							Start();
						}
						catch (MetricsConfigException e)
						{
							// Configuration errors (e.g., typos) should not be fatal.
							// We can always start the metrics system later via JMX.
							Log.Warn("Metrics system not started: " + e.Message);
							Log.Debug("Stacktrace: ", e);
						}
						break;
					}

					case MetricsSystemImpl.InitMode.Standby:
					{
						Log.Info(prefix + " metrics system started in standby mode");
						break;
					}
				}
				InitSystemMBean();
				return this;
			}
		}

		public override void Start()
		{
			lock (this)
			{
				Preconditions.CheckNotNull(prefix, "prefix");
				if (monitoring)
				{
					Log.Warn(prefix + " metrics system already started!", new MetricsException("Illegal start"
						));
					return;
				}
				foreach (MetricsSystem.Callback cb in callbacks)
				{
					cb.PreStart();
				}
				foreach (MetricsSystem.Callback cb_1 in namedCallbacks.Values)
				{
					cb_1.PreStart();
				}
				Configure(prefix);
				StartTimer();
				monitoring = true;
				Log.Info(prefix + " metrics system started");
				foreach (MetricsSystem.Callback cb_2 in callbacks)
				{
					cb_2.PostStart();
				}
				foreach (MetricsSystem.Callback cb_3 in namedCallbacks.Values)
				{
					cb_3.PostStart();
				}
			}
		}

		public override void Stop()
		{
			lock (this)
			{
				if (!monitoring && !DefaultMetricsSystem.InMiniClusterMode())
				{
					Log.Warn(prefix + " metrics system not yet started!", new MetricsException("Illegal stop"
						));
					return;
				}
				if (!monitoring)
				{
					// in mini cluster mode
					Log.Info(prefix + " metrics system stopped (again)");
					return;
				}
				foreach (MetricsSystem.Callback cb in callbacks)
				{
					cb.PreStop();
				}
				foreach (MetricsSystem.Callback cb_1 in namedCallbacks.Values)
				{
					cb_1.PreStop();
				}
				Log.Info("Stopping " + prefix + " metrics system...");
				StopTimer();
				StopSources();
				StopSinks();
				ClearConfigs();
				monitoring = false;
				Log.Info(prefix + " metrics system stopped.");
				foreach (MetricsSystem.Callback cb_2 in callbacks)
				{
					cb_2.PostStop();
				}
				foreach (MetricsSystem.Callback cb_3 in namedCallbacks.Values)
				{
					cb_3.PostStop();
				}
			}
		}

		public override T Register<T>(string name, string desc, T source)
		{
			lock (this)
			{
				MetricsSourceBuilder sb = MetricsAnnotations.NewSourceBuilder(source);
				MetricsSource s = sb.Build();
				MetricsInfo si = sb.Info();
				string name2 = name == null ? si.Name() : name;
				string finalDesc = desc == null ? si.Description() : desc;
				string finalName = DefaultMetricsSystem.SourceName(name2, !monitoring);
				// be friendly to non-metrics tests
				allSources[finalName] = s;
				Log.Debug(finalName + ", " + finalDesc);
				if (monitoring)
				{
					RegisterSource(finalName, finalDesc, s);
				}
				// We want to re-register the source to pick up new config when the
				// metrics system restarts.
				Register(finalName, new _AbstractCallback_238(this, finalName, finalDesc, s));
				return source;
			}
		}

		private sealed class _AbstractCallback_238 : MetricsSystem.AbstractCallback
		{
			public _AbstractCallback_238(MetricsSystemImpl _enclosing, string finalName, string
				 finalDesc, MetricsSource s)
			{
				this._enclosing = _enclosing;
				this.finalName = finalName;
				this.finalDesc = finalDesc;
				this.s = s;
			}

			public override void PostStart()
			{
				this._enclosing.RegisterSource(finalName, finalDesc, s);
			}

			private readonly MetricsSystemImpl _enclosing;

			private readonly string finalName;

			private readonly string finalDesc;

			private readonly MetricsSource s;
		}

		public override void UnregisterSource(string name)
		{
			lock (this)
			{
				if (sources.Contains(name))
				{
					sources[name].Stop();
					Sharpen.Collections.Remove(sources, name);
				}
				if (allSources.Contains(name))
				{
					Sharpen.Collections.Remove(allSources, name);
				}
				if (namedCallbacks.Contains(name))
				{
					Sharpen.Collections.Remove(namedCallbacks, name);
				}
			}
		}

		internal virtual void RegisterSource(string name, string desc, MetricsSource source
			)
		{
			lock (this)
			{
				Preconditions.CheckNotNull(config, "config");
				MetricsConfig conf = sourceConfigs[name];
				MetricsSourceAdapter sa = conf != null ? new MetricsSourceAdapter(prefix, name, desc
					, source, injectedTags, period, conf) : new MetricsSourceAdapter(prefix, name, desc
					, source, injectedTags, period, ((MetricsConfig)config.Subset(SourceKey)));
				sources[name] = sa;
				sa.Start();
				Log.Debug("Registered source " + name);
			}
		}

		public override T Register<T>(string name, string description, T sink)
		{
			lock (this)
			{
				Log.Debug(name + ", " + description);
				if (allSinks.Contains(name))
				{
					Log.Warn("Sink " + name + " already exists!");
					return sink;
				}
				allSinks[name] = sink;
				if (config != null)
				{
					RegisterSink(name, description, sink);
				}
				// We want to re-register the sink to pick up new config
				// when the metrics system restarts.
				Register(name, new _AbstractCallback_287(this, name, description, sink));
				return sink;
			}
		}

		private sealed class _AbstractCallback_287 : MetricsSystem.AbstractCallback
		{
			public _AbstractCallback_287(MetricsSystemImpl _enclosing, string name, string description
				, T sink)
			{
				this._enclosing = _enclosing;
				this.name = name;
				this.description = description;
				this.sink = sink;
			}

			public override void PostStart()
			{
				this._enclosing.Register(name, description, sink);
			}

			private readonly MetricsSystemImpl _enclosing;

			private readonly string name;

			private readonly string description;

			private readonly T sink;
		}

		internal virtual void RegisterSink(string name, string desc, MetricsSink sink)
		{
			lock (this)
			{
				Preconditions.CheckNotNull(config, "config");
				MetricsConfig conf = sinkConfigs[name];
				MetricsSinkAdapter sa = conf != null ? NewSink(name, desc, sink, conf) : NewSink(
					name, desc, sink, ((MetricsConfig)config.Subset(SinkKey)));
				sinks[name] = sa;
				sa.Start();
				Log.Info("Registered sink " + name);
			}
		}

		public override void Register(MetricsSystem.Callback callback)
		{
			lock (this)
			{
				callbacks.AddItem((MetricsSystem.Callback)GetProxyForCallback(callback));
			}
		}

		private void Register(string name, MetricsSystem.Callback callback)
		{
			lock (this)
			{
				namedCallbacks[name] = (MetricsSystem.Callback)GetProxyForCallback(callback);
			}
		}

		private object GetProxyForCallback(MetricsSystem.Callback callback)
		{
			return Proxy.NewProxyInstance(callback.GetType().GetClassLoader(), new Type[] { typeof(
				MetricsSystem.Callback) }, new _InvocationHandler_317(callback));
		}

		private sealed class _InvocationHandler_317 : InvocationHandler
		{
			public _InvocationHandler_317(MetricsSystem.Callback callback)
			{
				this.callback = callback;
			}

			/// <exception cref="System.Exception"/>
			public object Invoke(object proxy, MethodInfo method, object[] args)
			{
				try
				{
					return method.Invoke(callback, args);
				}
				catch (Exception e)
				{
					// These are not considered fatal.
					Org.Apache.Hadoop.Metrics2.Impl.MetricsSystemImpl.Log.Warn("Caught exception in callback "
						 + method.Name, e);
				}
				return null;
			}

			private readonly MetricsSystem.Callback callback;
		}

		public override void StartMetricsMBeans()
		{
			lock (this)
			{
				foreach (MetricsSourceAdapter sa in sources.Values)
				{
					sa.StartMBeans();
				}
			}
		}

		public override void StopMetricsMBeans()
		{
			lock (this)
			{
				foreach (MetricsSourceAdapter sa in sources.Values)
				{
					sa.StopMBeans();
				}
			}
		}

		public override string CurrentConfig()
		{
			lock (this)
			{
				PropertiesConfiguration saver = new PropertiesConfiguration();
				StringWriter writer = new StringWriter();
				saver.Copy(config);
				try
				{
					saver.Save(writer);
				}
				catch (Exception e)
				{
					throw new MetricsConfigException("Error stringify config", e);
				}
				return writer.ToString();
			}
		}

		private void StartTimer()
		{
			lock (this)
			{
				if (timer != null)
				{
					Log.Warn(prefix + " metrics system timer already started!");
					return;
				}
				logicalTime = 0;
				long millis = period * 1000;
				timer = new Timer("Timer for '" + prefix + "' metrics system", true);
				timer.ScheduleAtFixedRate(new _TimerTask_367(this), millis, millis);
				Log.Info("Scheduled snapshot period at " + period + " second(s).");
			}
		}

		private sealed class _TimerTask_367 : TimerTask
		{
			public _TimerTask_367(MetricsSystemImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				try
				{
					this._enclosing.OnTimerEvent();
				}
				catch (Exception e)
				{
					Org.Apache.Hadoop.Metrics2.Impl.MetricsSystemImpl.Log.Warn(e);
				}
			}

			private readonly MetricsSystemImpl _enclosing;
		}

		internal virtual void OnTimerEvent()
		{
			lock (this)
			{
				logicalTime += period;
				if (sinks.Count > 0)
				{
					PublishMetrics(SampleMetrics(), false);
				}
			}
		}

		/// <summary>Requests an immediate publish of all metrics from sources to sinks.</summary>
		public override void PublishMetricsNow()
		{
			lock (this)
			{
				if (sinks.Count > 0)
				{
					PublishMetrics(SampleMetrics(), true);
				}
			}
		}

		/// <summary>Sample all the sources for a snapshot of metrics/tags</summary>
		/// <returns>the metrics buffer containing the snapshot</returns>
		[VisibleForTesting]
		public virtual MetricsBuffer SampleMetrics()
		{
			lock (this)
			{
				collector.Clear();
				MetricsBufferBuilder bufferBuilder = new MetricsBufferBuilder();
				foreach (KeyValuePair<string, MetricsSourceAdapter> entry in sources)
				{
					if (sourceFilter == null || sourceFilter.Accepts(entry.Key))
					{
						SnapshotMetrics(entry.Value, bufferBuilder);
					}
				}
				if (publishSelfMetrics)
				{
					SnapshotMetrics(sysSource, bufferBuilder);
				}
				MetricsBuffer buffer = bufferBuilder.Get();
				return buffer;
			}
		}

		private void SnapshotMetrics(MetricsSourceAdapter sa, MetricsBufferBuilder bufferBuilder
			)
		{
			long startTime = Time.Now();
			bufferBuilder.Add(sa.Name(), sa.GetMetrics(collector, true));
			collector.Clear();
			snapshotStat.Add(Time.Now() - startTime);
			Log.Debug("Snapshotted source " + sa.Name());
		}

		/// <summary>Publish a metrics snapshot to all the sinks</summary>
		/// <param name="buffer">the metrics snapshot to publish</param>
		/// <param name="immediate">
		/// indicates that we should publish metrics immediately
		/// instead of using a separate thread.
		/// </param>
		internal virtual void PublishMetrics(MetricsBuffer buffer, bool immediate)
		{
			lock (this)
			{
				int dropped = 0;
				foreach (MetricsSinkAdapter sa in sinks.Values)
				{
					long startTime = Time.Now();
					bool result;
					if (immediate)
					{
						result = sa.PutMetricsImmediate(buffer);
					}
					else
					{
						result = sa.PutMetrics(buffer, logicalTime);
					}
					dropped += result ? 0 : 1;
					publishStat.Add(Time.Now() - startTime);
				}
				droppedPubAll.Incr(dropped);
			}
		}

		private void StopTimer()
		{
			lock (this)
			{
				if (timer == null)
				{
					Log.Warn(prefix + " metrics system timer already stopped!");
					return;
				}
				timer.Cancel();
				timer = null;
			}
		}

		private void StopSources()
		{
			lock (this)
			{
				foreach (KeyValuePair<string, MetricsSourceAdapter> entry in sources)
				{
					MetricsSourceAdapter sa = entry.Value;
					Log.Debug("Stopping metrics source " + entry.Key + ": class=" + sa.Source().GetType
						());
					sa.Stop();
				}
				sysSource.Stop();
				sources.Clear();
			}
		}

		private void StopSinks()
		{
			lock (this)
			{
				foreach (KeyValuePair<string, MetricsSinkAdapter> entry in sinks)
				{
					MetricsSinkAdapter sa = entry.Value;
					Log.Debug("Stopping metrics sink " + entry.Key + ": class=" + sa.Sink().GetType()
						);
					sa.Stop();
				}
				sinks.Clear();
			}
		}

		private void Configure(string prefix)
		{
			lock (this)
			{
				config = MetricsConfig.Create(prefix);
				ConfigureSinks();
				ConfigureSources();
				ConfigureSystem();
			}
		}

		private void ConfigureSystem()
		{
			lock (this)
			{
				injectedTags.AddItem(Interns.Tag(MsInfo.Hostname, GetHostname()));
			}
		}

		private void ConfigureSinks()
		{
			lock (this)
			{
				sinkConfigs = config.GetInstanceConfigs(SinkKey);
				int confPeriod = 0;
				foreach (KeyValuePair<string, MetricsConfig> entry in sinkConfigs)
				{
					MetricsConfig conf = entry.Value;
					int sinkPeriod = conf.GetInt(PeriodKey, PeriodDefault);
					confPeriod = confPeriod == 0 ? sinkPeriod : ArithmeticUtils.Gcd(confPeriod, sinkPeriod
						);
					string clsName = conf.GetClassName(string.Empty);
					if (clsName == null)
					{
						continue;
					}
					// sink can be registered later on
					string sinkName = entry.Key;
					try
					{
						MetricsSinkAdapter sa = NewSink(sinkName, conf.GetString(DescKey, sinkName), conf
							);
						sa.Start();
						sinks[sinkName] = sa;
					}
					catch (Exception e)
					{
						Log.Warn("Error creating sink '" + sinkName + "'", e);
					}
				}
				period = confPeriod > 0 ? confPeriod : config.GetInt(PeriodKey, PeriodDefault);
			}
		}

		internal static MetricsSinkAdapter NewSink(string name, string desc, MetricsSink 
			sink, MetricsConfig conf)
		{
			return new MetricsSinkAdapter(name, desc, sink, conf.GetString(ContextKey), conf.
				GetFilter(SourceFilterKey), conf.GetFilter(RecordFilterKey), conf.GetFilter(MetricFilterKey
				), conf.GetInt(PeriodKey, PeriodDefault), conf.GetInt(QueueCapacityKey, QueueCapacityDefault
				), conf.GetInt(RetryDelayKey, RetryDelayDefault), conf.GetFloat(RetryBackoffKey, 
				RetryBackoffDefault), conf.GetInt(RetryCountKey, RetryCountDefault));
		}

		internal static MetricsSinkAdapter NewSink(string name, string desc, MetricsConfig
			 conf)
		{
			return NewSink(name, desc, (MetricsSink)conf.GetPlugin(string.Empty), conf);
		}

		private void ConfigureSources()
		{
			sourceFilter = config.GetFilter(PrefixDefault + SourceFilterKey);
			sourceConfigs = config.GetInstanceConfigs(SourceKey);
			RegisterSystemSource();
		}

		private void ClearConfigs()
		{
			sinkConfigs.Clear();
			sourceConfigs.Clear();
			injectedTags.Clear();
			config = null;
		}

		internal static string GetHostname()
		{
			try
			{
				return Sharpen.Runtime.GetLocalHost().GetHostName();
			}
			catch (Exception e)
			{
				Log.Error("Error getting localhost name. Using 'localhost'...", e);
			}
			return "localhost";
		}

		private void RegisterSystemSource()
		{
			MetricsConfig sysConf = sourceConfigs[MsName];
			sysSource = new MetricsSourceAdapter(prefix, MsStatsName, MsStatsDesc, MetricsAnnotations
				.MakeSource(this), injectedTags, period, sysConf == null ? ((MetricsConfig)config
				.Subset(SourceKey)) : sysConf);
			sysSource.Start();
		}

		public virtual void GetMetrics(MetricsCollector builder, bool all)
		{
			lock (this)
			{
				MetricsRecordBuilder rb = builder.AddRecord(MsName).AddGauge(MsInfo.NumActiveSources
					, sources.Count).AddGauge(MsInfo.NumAllSources, allSources.Count).AddGauge(MsInfo
					.NumActiveSinks, sinks.Count).AddGauge(MsInfo.NumAllSinks, allSinks.Count);
				foreach (MetricsSinkAdapter sa in sinks.Values)
				{
					sa.Snapshot(rb, all);
				}
				registry.Snapshot(rb, all);
			}
		}

		private void InitSystemMBean()
		{
			Preconditions.CheckNotNull(prefix, "prefix should not be null here!");
			if (mbeanName == null)
			{
				mbeanName = MBeans.Register(prefix, MsControlName, this);
			}
		}

		public override bool Shutdown()
		{
			lock (this)
			{
				Log.Debug("refCount=" + refCount);
				if (refCount <= 0)
				{
					Log.Debug("Redundant shutdown", new Exception());
					return true;
				}
				// already shutdown
				if (--refCount > 0)
				{
					return false;
				}
				if (monitoring)
				{
					try
					{
						Stop();
					}
					catch (Exception e)
					{
						Log.Warn("Error stopping the metrics system", e);
					}
				}
				allSources.Clear();
				allSinks.Clear();
				callbacks.Clear();
				namedCallbacks.Clear();
				if (mbeanName != null)
				{
					MBeans.Unregister(mbeanName);
					mbeanName = null;
				}
				Log.Info(prefix + " metrics system shutdown complete.");
				return true;
			}
		}

		public override MetricsSource GetSource(string name)
		{
			return allSources[name];
		}

		[VisibleForTesting]
		internal virtual MetricsSourceAdapter GetSourceAdapter(string name)
		{
			return sources[name];
		}

		private MetricsSystemImpl.InitMode InitMode()
		{
			Log.Debug("from system property: " + Runtime.GetProperty(MsInitModeKey));
			Log.Debug("from environment variable: " + Runtime.Getenv(MsInitModeKey));
			string m = Runtime.GetProperty(MsInitModeKey);
			string m2 = m == null ? Runtime.Getenv(MsInitModeKey) : m;
			return MetricsSystemImpl.InitMode.ValueOf(StringUtils.ToUpperCase((m2 == null ? MetricsSystemImpl.InitMode
				.Normal.ToString() : m2)));
		}
	}
}
