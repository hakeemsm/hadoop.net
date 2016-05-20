using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	/// <summary>A base class for metrics system singletons</summary>
	public class MetricsSystemImpl : org.apache.hadoop.metrics2.MetricsSystem, org.apache.hadoop.metrics2.MetricsSource
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.impl.MetricsSystemImpl
			)));

		internal const string MS_NAME = "MetricsSystem";

		internal const string MS_STATS_NAME = MS_NAME + ",sub=Stats";

		internal const string MS_STATS_DESC = "Metrics system metrics";

		internal const string MS_CONTROL_NAME = MS_NAME + ",sub=Control";

		internal const string MS_INIT_MODE_KEY = "hadoop.metrics.init.mode";

		internal enum InitMode
		{
			NORMAL,
			STANDBY
		}

		private readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.impl.MetricsSourceAdapter
			> sources;

		private readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.MetricsSource
			> allSources;

		private readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.impl.MetricsSinkAdapter
			> sinks;

		private readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.MetricsSink
			> allSinks;

		private readonly System.Collections.Generic.IList<org.apache.hadoop.metrics2.MetricsSystem.Callback
			> callbacks;

		private readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.MetricsSystem.Callback
			> namedCallbacks;

		private readonly org.apache.hadoop.metrics2.impl.MetricsCollectorImpl collector;

		private readonly org.apache.hadoop.metrics2.lib.MetricsRegistry registry = new org.apache.hadoop.metrics2.lib.MetricsRegistry
			(MS_NAME);

		internal org.apache.hadoop.metrics2.lib.MutableStat snapshotStat;

		internal org.apache.hadoop.metrics2.lib.MutableStat publishStat;

		internal org.apache.hadoop.metrics2.lib.MutableCounterLong droppedPubAll;

		private readonly System.Collections.Generic.IList<org.apache.hadoop.metrics2.MetricsTag
			> injectedTags;

		private string prefix;

		private org.apache.hadoop.metrics2.MetricsFilter sourceFilter;

		private org.apache.hadoop.metrics2.impl.MetricsConfig config;

		private System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.impl.MetricsConfig
			> sourceConfigs;

		private System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.impl.MetricsConfig
			> sinkConfigs;

		private bool monitoring = false;

		private java.util.Timer timer;

		private int period;

		private long logicalTime;

		private javax.management.ObjectName mbeanName;

		private bool publishSelfMetrics = true;

		private org.apache.hadoop.metrics2.impl.MetricsSourceAdapter sysSource;

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
			allSources = com.google.common.collect.Maps.newHashMap();
			sources = com.google.common.collect.Maps.newLinkedHashMap();
			allSinks = com.google.common.collect.Maps.newHashMap();
			sinks = com.google.common.collect.Maps.newLinkedHashMap();
			sourceConfigs = com.google.common.collect.Maps.newHashMap();
			sinkConfigs = com.google.common.collect.Maps.newHashMap();
			callbacks = com.google.common.collect.Lists.newArrayList();
			namedCallbacks = com.google.common.collect.Maps.newHashMap();
			injectedTags = com.google.common.collect.Lists.newArrayList();
			collector = new org.apache.hadoop.metrics2.impl.MetricsCollectorImpl();
			if (prefix != null)
			{
				// prefix could be null for default ctor, which requires init later
				initSystemMBean();
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
		public override org.apache.hadoop.metrics2.MetricsSystem init(string prefix)
		{
			lock (this)
			{
				if (monitoring && !org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.inMiniClusterMode
					())
				{
					LOG.warn(this.prefix + " metrics system already initialized!");
					return this;
				}
				this.prefix = com.google.common.@base.Preconditions.checkNotNull(prefix, "prefix"
					);
				++refCount;
				if (monitoring)
				{
					// in mini cluster mode
					LOG.info(this.prefix + " metrics system started (again)");
					return this;
				}
				switch (initMode())
				{
					case org.apache.hadoop.metrics2.impl.MetricsSystemImpl.InitMode.NORMAL:
					{
						try
						{
							start();
						}
						catch (org.apache.hadoop.metrics2.impl.MetricsConfigException e)
						{
							// Configuration errors (e.g., typos) should not be fatal.
							// We can always start the metrics system later via JMX.
							LOG.warn("Metrics system not started: " + e.Message);
							LOG.debug("Stacktrace: ", e);
						}
						break;
					}

					case org.apache.hadoop.metrics2.impl.MetricsSystemImpl.InitMode.STANDBY:
					{
						LOG.info(prefix + " metrics system started in standby mode");
						break;
					}
				}
				initSystemMBean();
				return this;
			}
		}

		public override void start()
		{
			lock (this)
			{
				com.google.common.@base.Preconditions.checkNotNull(prefix, "prefix");
				if (monitoring)
				{
					LOG.warn(prefix + " metrics system already started!", new org.apache.hadoop.metrics2.MetricsException
						("Illegal start"));
					return;
				}
				foreach (org.apache.hadoop.metrics2.MetricsSystem.Callback cb in callbacks)
				{
					cb.preStart();
				}
				foreach (org.apache.hadoop.metrics2.MetricsSystem.Callback cb_1 in namedCallbacks
					.Values)
				{
					cb_1.preStart();
				}
				configure(prefix);
				startTimer();
				monitoring = true;
				LOG.info(prefix + " metrics system started");
				foreach (org.apache.hadoop.metrics2.MetricsSystem.Callback cb_2 in callbacks)
				{
					cb_2.postStart();
				}
				foreach (org.apache.hadoop.metrics2.MetricsSystem.Callback cb_3 in namedCallbacks
					.Values)
				{
					cb_3.postStart();
				}
			}
		}

		public override void stop()
		{
			lock (this)
			{
				if (!monitoring && !org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.inMiniClusterMode
					())
				{
					LOG.warn(prefix + " metrics system not yet started!", new org.apache.hadoop.metrics2.MetricsException
						("Illegal stop"));
					return;
				}
				if (!monitoring)
				{
					// in mini cluster mode
					LOG.info(prefix + " metrics system stopped (again)");
					return;
				}
				foreach (org.apache.hadoop.metrics2.MetricsSystem.Callback cb in callbacks)
				{
					cb.preStop();
				}
				foreach (org.apache.hadoop.metrics2.MetricsSystem.Callback cb_1 in namedCallbacks
					.Values)
				{
					cb_1.preStop();
				}
				LOG.info("Stopping " + prefix + " metrics system...");
				stopTimer();
				stopSources();
				stopSinks();
				clearConfigs();
				monitoring = false;
				LOG.info(prefix + " metrics system stopped.");
				foreach (org.apache.hadoop.metrics2.MetricsSystem.Callback cb_2 in callbacks)
				{
					cb_2.postStop();
				}
				foreach (org.apache.hadoop.metrics2.MetricsSystem.Callback cb_3 in namedCallbacks
					.Values)
				{
					cb_3.postStop();
				}
			}
		}

		public override T register<T>(string name, string desc, T source)
		{
			lock (this)
			{
				org.apache.hadoop.metrics2.lib.MetricsSourceBuilder sb = org.apache.hadoop.metrics2.lib.MetricsAnnotations
					.newSourceBuilder(source);
				org.apache.hadoop.metrics2.MetricsSource s = sb.build();
				org.apache.hadoop.metrics2.MetricsInfo si = sb.info();
				string name2 = name == null ? si.name() : name;
				string finalDesc = desc == null ? si.description() : desc;
				string finalName = org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.sourceName
					(name2, !monitoring);
				// be friendly to non-metrics tests
				allSources[finalName] = s;
				LOG.debug(finalName + ", " + finalDesc);
				if (monitoring)
				{
					registerSource(finalName, finalDesc, s);
				}
				// We want to re-register the source to pick up new config when the
				// metrics system restarts.
				register(finalName, new _AbstractCallback_238(this, finalName, finalDesc, s));
				return source;
			}
		}

		private sealed class _AbstractCallback_238 : org.apache.hadoop.metrics2.MetricsSystem.AbstractCallback
		{
			public _AbstractCallback_238(MetricsSystemImpl _enclosing, string finalName, string
				 finalDesc, org.apache.hadoop.metrics2.MetricsSource s)
			{
				this._enclosing = _enclosing;
				this.finalName = finalName;
				this.finalDesc = finalDesc;
				this.s = s;
			}

			public override void postStart()
			{
				this._enclosing.registerSource(finalName, finalDesc, s);
			}

			private readonly MetricsSystemImpl _enclosing;

			private readonly string finalName;

			private readonly string finalDesc;

			private readonly org.apache.hadoop.metrics2.MetricsSource s;
		}

		public override void unregisterSource(string name)
		{
			lock (this)
			{
				if (sources.Contains(name))
				{
					sources[name].stop();
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

		internal virtual void registerSource(string name, string desc, org.apache.hadoop.metrics2.MetricsSource
			 source)
		{
			lock (this)
			{
				com.google.common.@base.Preconditions.checkNotNull(config, "config");
				org.apache.hadoop.metrics2.impl.MetricsConfig conf = sourceConfigs[name];
				org.apache.hadoop.metrics2.impl.MetricsSourceAdapter sa = conf != null ? new org.apache.hadoop.metrics2.impl.MetricsSourceAdapter
					(prefix, name, desc, source, injectedTags, period, conf) : new org.apache.hadoop.metrics2.impl.MetricsSourceAdapter
					(prefix, name, desc, source, injectedTags, period, ((org.apache.hadoop.metrics2.impl.MetricsConfig
					)config.subset(SOURCE_KEY)));
				sources[name] = sa;
				sa.start();
				LOG.debug("Registered source " + name);
			}
		}

		public override T register<T>(string name, string description, T sink)
		{
			lock (this)
			{
				LOG.debug(name + ", " + description);
				if (allSinks.Contains(name))
				{
					LOG.warn("Sink " + name + " already exists!");
					return sink;
				}
				allSinks[name] = sink;
				if (config != null)
				{
					registerSink(name, description, sink);
				}
				// We want to re-register the sink to pick up new config
				// when the metrics system restarts.
				register(name, new _AbstractCallback_287(this, name, description, sink));
				return sink;
			}
		}

		private sealed class _AbstractCallback_287 : org.apache.hadoop.metrics2.MetricsSystem.AbstractCallback
		{
			public _AbstractCallback_287(MetricsSystemImpl _enclosing, string name, string description
				, T sink)
			{
				this._enclosing = _enclosing;
				this.name = name;
				this.description = description;
				this.sink = sink;
			}

			public override void postStart()
			{
				this._enclosing.register(name, description, sink);
			}

			private readonly MetricsSystemImpl _enclosing;

			private readonly string name;

			private readonly string description;

			private readonly T sink;
		}

		internal virtual void registerSink(string name, string desc, org.apache.hadoop.metrics2.MetricsSink
			 sink)
		{
			lock (this)
			{
				com.google.common.@base.Preconditions.checkNotNull(config, "config");
				org.apache.hadoop.metrics2.impl.MetricsConfig conf = sinkConfigs[name];
				org.apache.hadoop.metrics2.impl.MetricsSinkAdapter sa = conf != null ? newSink(name
					, desc, sink, conf) : newSink(name, desc, sink, ((org.apache.hadoop.metrics2.impl.MetricsConfig
					)config.subset(SINK_KEY)));
				sinks[name] = sa;
				sa.start();
				LOG.info("Registered sink " + name);
			}
		}

		public override void register(org.apache.hadoop.metrics2.MetricsSystem.Callback callback
			)
		{
			lock (this)
			{
				callbacks.add((org.apache.hadoop.metrics2.MetricsSystem.Callback)getProxyForCallback
					(callback));
			}
		}

		private void register(string name, org.apache.hadoop.metrics2.MetricsSystem.Callback
			 callback)
		{
			lock (this)
			{
				namedCallbacks[name] = (org.apache.hadoop.metrics2.MetricsSystem.Callback)getProxyForCallback
					(callback);
			}
		}

		private object getProxyForCallback(org.apache.hadoop.metrics2.MetricsSystem.Callback
			 callback)
		{
			return java.lang.reflect.Proxy.newProxyInstance(Sharpen.Runtime.getClassForObject
				(callback).getClassLoader(), new java.lang.Class[] { Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.metrics2.MetricsSystem.Callback)) }, new _InvocationHandler_317
				(callback));
		}

		private sealed class _InvocationHandler_317 : java.lang.reflect.InvocationHandler
		{
			public _InvocationHandler_317(org.apache.hadoop.metrics2.MetricsSystem.Callback callback
				)
			{
				this.callback = callback;
			}

			/// <exception cref="System.Exception"/>
			public object invoke(object proxy, java.lang.reflect.Method method, object[] args
				)
			{
				try
				{
					return method.invoke(callback, args);
				}
				catch (System.Exception e)
				{
					// These are not considered fatal.
					org.apache.hadoop.metrics2.impl.MetricsSystemImpl.LOG.warn("Caught exception in callback "
						 + method.getName(), e);
				}
				return null;
			}

			private readonly org.apache.hadoop.metrics2.MetricsSystem.Callback callback;
		}

		public override void startMetricsMBeans()
		{
			lock (this)
			{
				foreach (org.apache.hadoop.metrics2.impl.MetricsSourceAdapter sa in sources.Values)
				{
					sa.startMBeans();
				}
			}
		}

		public override void stopMetricsMBeans()
		{
			lock (this)
			{
				foreach (org.apache.hadoop.metrics2.impl.MetricsSourceAdapter sa in sources.Values)
				{
					sa.stopMBeans();
				}
			}
		}

		public override string currentConfig()
		{
			lock (this)
			{
				org.apache.commons.configuration.PropertiesConfiguration saver = new org.apache.commons.configuration.PropertiesConfiguration
					();
				System.IO.StringWriter writer = new System.IO.StringWriter();
				saver.copy(config);
				try
				{
					saver.save(writer);
				}
				catch (System.Exception e)
				{
					throw new org.apache.hadoop.metrics2.impl.MetricsConfigException("Error stringify config"
						, e);
				}
				return writer.ToString();
			}
		}

		private void startTimer()
		{
			lock (this)
			{
				if (timer != null)
				{
					LOG.warn(prefix + " metrics system timer already started!");
					return;
				}
				logicalTime = 0;
				long millis = period * 1000;
				timer = new java.util.Timer("Timer for '" + prefix + "' metrics system", true);
				timer.scheduleAtFixedRate(new _TimerTask_367(this), millis, millis);
				LOG.info("Scheduled snapshot period at " + period + " second(s).");
			}
		}

		private sealed class _TimerTask_367 : java.util.TimerTask
		{
			public _TimerTask_367(MetricsSystemImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void run()
			{
				try
				{
					this._enclosing.onTimerEvent();
				}
				catch (System.Exception e)
				{
					org.apache.hadoop.metrics2.impl.MetricsSystemImpl.LOG.warn(e);
				}
			}

			private readonly MetricsSystemImpl _enclosing;
		}

		internal virtual void onTimerEvent()
		{
			lock (this)
			{
				logicalTime += period;
				if (sinks.Count > 0)
				{
					publishMetrics(sampleMetrics(), false);
				}
			}
		}

		/// <summary>Requests an immediate publish of all metrics from sources to sinks.</summary>
		public override void publishMetricsNow()
		{
			lock (this)
			{
				if (sinks.Count > 0)
				{
					publishMetrics(sampleMetrics(), true);
				}
			}
		}

		/// <summary>Sample all the sources for a snapshot of metrics/tags</summary>
		/// <returns>the metrics buffer containing the snapshot</returns>
		[com.google.common.annotations.VisibleForTesting]
		public virtual org.apache.hadoop.metrics2.impl.MetricsBuffer sampleMetrics()
		{
			lock (this)
			{
				collector.clear();
				org.apache.hadoop.metrics2.impl.MetricsBufferBuilder bufferBuilder = new org.apache.hadoop.metrics2.impl.MetricsBufferBuilder
					();
				foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.metrics2.impl.MetricsSourceAdapter
					> entry in sources)
				{
					if (sourceFilter == null || sourceFilter.accepts(entry.Key))
					{
						snapshotMetrics(entry.Value, bufferBuilder);
					}
				}
				if (publishSelfMetrics)
				{
					snapshotMetrics(sysSource, bufferBuilder);
				}
				org.apache.hadoop.metrics2.impl.MetricsBuffer buffer = bufferBuilder.get();
				return buffer;
			}
		}

		private void snapshotMetrics(org.apache.hadoop.metrics2.impl.MetricsSourceAdapter
			 sa, org.apache.hadoop.metrics2.impl.MetricsBufferBuilder bufferBuilder)
		{
			long startTime = org.apache.hadoop.util.Time.now();
			bufferBuilder.add(sa.name(), sa.getMetrics(collector, true));
			collector.clear();
			snapshotStat.add(org.apache.hadoop.util.Time.now() - startTime);
			LOG.debug("Snapshotted source " + sa.name());
		}

		/// <summary>Publish a metrics snapshot to all the sinks</summary>
		/// <param name="buffer">the metrics snapshot to publish</param>
		/// <param name="immediate">
		/// indicates that we should publish metrics immediately
		/// instead of using a separate thread.
		/// </param>
		internal virtual void publishMetrics(org.apache.hadoop.metrics2.impl.MetricsBuffer
			 buffer, bool immediate)
		{
			lock (this)
			{
				int dropped = 0;
				foreach (org.apache.hadoop.metrics2.impl.MetricsSinkAdapter sa in sinks.Values)
				{
					long startTime = org.apache.hadoop.util.Time.now();
					bool result;
					if (immediate)
					{
						result = sa.putMetricsImmediate(buffer);
					}
					else
					{
						result = sa.putMetrics(buffer, logicalTime);
					}
					dropped += result ? 0 : 1;
					publishStat.add(org.apache.hadoop.util.Time.now() - startTime);
				}
				droppedPubAll.incr(dropped);
			}
		}

		private void stopTimer()
		{
			lock (this)
			{
				if (timer == null)
				{
					LOG.warn(prefix + " metrics system timer already stopped!");
					return;
				}
				timer.cancel();
				timer = null;
			}
		}

		private void stopSources()
		{
			lock (this)
			{
				foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.metrics2.impl.MetricsSourceAdapter
					> entry in sources)
				{
					org.apache.hadoop.metrics2.impl.MetricsSourceAdapter sa = entry.Value;
					LOG.debug("Stopping metrics source " + entry.Key + ": class=" + Sharpen.Runtime.getClassForObject
						(sa.source()));
					sa.stop();
				}
				sysSource.stop();
				sources.clear();
			}
		}

		private void stopSinks()
		{
			lock (this)
			{
				foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.metrics2.impl.MetricsSinkAdapter
					> entry in sinks)
				{
					org.apache.hadoop.metrics2.impl.MetricsSinkAdapter sa = entry.Value;
					LOG.debug("Stopping metrics sink " + entry.Key + ": class=" + Sharpen.Runtime.getClassForObject
						(sa.sink()));
					sa.stop();
				}
				sinks.clear();
			}
		}

		private void configure(string prefix)
		{
			lock (this)
			{
				config = org.apache.hadoop.metrics2.impl.MetricsConfig.create(prefix);
				configureSinks();
				configureSources();
				configureSystem();
			}
		}

		private void configureSystem()
		{
			lock (this)
			{
				injectedTags.add(org.apache.hadoop.metrics2.lib.Interns.tag(org.apache.hadoop.metrics2.impl.MsInfo
					.Hostname, getHostname()));
			}
		}

		private void configureSinks()
		{
			lock (this)
			{
				sinkConfigs = config.getInstanceConfigs(SINK_KEY);
				int confPeriod = 0;
				foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.metrics2.impl.MetricsConfig
					> entry in sinkConfigs)
				{
					org.apache.hadoop.metrics2.impl.MetricsConfig conf = entry.Value;
					int sinkPeriod = conf.getInt(PERIOD_KEY, PERIOD_DEFAULT);
					confPeriod = confPeriod == 0 ? sinkPeriod : org.apache.commons.math3.util.ArithmeticUtils
						.gcd(confPeriod, sinkPeriod);
					string clsName = conf.getClassName(string.Empty);
					if (clsName == null)
					{
						continue;
					}
					// sink can be registered later on
					string sinkName = entry.Key;
					try
					{
						org.apache.hadoop.metrics2.impl.MetricsSinkAdapter sa = newSink(sinkName, conf.getString
							(DESC_KEY, sinkName), conf);
						sa.start();
						sinks[sinkName] = sa;
					}
					catch (System.Exception e)
					{
						LOG.warn("Error creating sink '" + sinkName + "'", e);
					}
				}
				period = confPeriod > 0 ? confPeriod : config.getInt(PERIOD_KEY, PERIOD_DEFAULT);
			}
		}

		internal static org.apache.hadoop.metrics2.impl.MetricsSinkAdapter newSink(string
			 name, string desc, org.apache.hadoop.metrics2.MetricsSink sink, org.apache.hadoop.metrics2.impl.MetricsConfig
			 conf)
		{
			return new org.apache.hadoop.metrics2.impl.MetricsSinkAdapter(name, desc, sink, conf
				.getString(CONTEXT_KEY), conf.getFilter(SOURCE_FILTER_KEY), conf.getFilter(RECORD_FILTER_KEY
				), conf.getFilter(METRIC_FILTER_KEY), conf.getInt(PERIOD_KEY, PERIOD_DEFAULT), conf
				.getInt(QUEUE_CAPACITY_KEY, QUEUE_CAPACITY_DEFAULT), conf.getInt(RETRY_DELAY_KEY
				, RETRY_DELAY_DEFAULT), conf.getFloat(RETRY_BACKOFF_KEY, RETRY_BACKOFF_DEFAULT), 
				conf.getInt(RETRY_COUNT_KEY, RETRY_COUNT_DEFAULT));
		}

		internal static org.apache.hadoop.metrics2.impl.MetricsSinkAdapter newSink(string
			 name, string desc, org.apache.hadoop.metrics2.impl.MetricsConfig conf)
		{
			return newSink(name, desc, (org.apache.hadoop.metrics2.MetricsSink)conf.getPlugin
				(string.Empty), conf);
		}

		private void configureSources()
		{
			sourceFilter = config.getFilter(PREFIX_DEFAULT + SOURCE_FILTER_KEY);
			sourceConfigs = config.getInstanceConfigs(SOURCE_KEY);
			registerSystemSource();
		}

		private void clearConfigs()
		{
			sinkConfigs.clear();
			sourceConfigs.clear();
			injectedTags.clear();
			config = null;
		}

		internal static string getHostname()
		{
			try
			{
				return java.net.InetAddress.getLocalHost().getHostName();
			}
			catch (System.Exception e)
			{
				LOG.error("Error getting localhost name. Using 'localhost'...", e);
			}
			return "localhost";
		}

		private void registerSystemSource()
		{
			org.apache.hadoop.metrics2.impl.MetricsConfig sysConf = sourceConfigs[MS_NAME];
			sysSource = new org.apache.hadoop.metrics2.impl.MetricsSourceAdapter(prefix, MS_STATS_NAME
				, MS_STATS_DESC, org.apache.hadoop.metrics2.lib.MetricsAnnotations.makeSource(this
				), injectedTags, period, sysConf == null ? ((org.apache.hadoop.metrics2.impl.MetricsConfig
				)config.subset(SOURCE_KEY)) : sysConf);
			sysSource.start();
		}

		public virtual void getMetrics(org.apache.hadoop.metrics2.MetricsCollector builder
			, bool all)
		{
			lock (this)
			{
				org.apache.hadoop.metrics2.MetricsRecordBuilder rb = builder.addRecord(MS_NAME).addGauge
					(org.apache.hadoop.metrics2.impl.MsInfo.NumActiveSources, sources.Count).addGauge
					(org.apache.hadoop.metrics2.impl.MsInfo.NumAllSources, allSources.Count).addGauge
					(org.apache.hadoop.metrics2.impl.MsInfo.NumActiveSinks, sinks.Count).addGauge(org.apache.hadoop.metrics2.impl.MsInfo
					.NumAllSinks, allSinks.Count);
				foreach (org.apache.hadoop.metrics2.impl.MetricsSinkAdapter sa in sinks.Values)
				{
					sa.snapshot(rb, all);
				}
				registry.snapshot(rb, all);
			}
		}

		private void initSystemMBean()
		{
			com.google.common.@base.Preconditions.checkNotNull(prefix, "prefix should not be null here!"
				);
			if (mbeanName == null)
			{
				mbeanName = org.apache.hadoop.metrics2.util.MBeans.register(prefix, MS_CONTROL_NAME
					, this);
			}
		}

		public override bool shutdown()
		{
			lock (this)
			{
				LOG.debug("refCount=" + refCount);
				if (refCount <= 0)
				{
					LOG.debug("Redundant shutdown", new System.Exception());
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
						stop();
					}
					catch (System.Exception e)
					{
						LOG.warn("Error stopping the metrics system", e);
					}
				}
				allSources.clear();
				allSinks.clear();
				callbacks.clear();
				namedCallbacks.clear();
				if (mbeanName != null)
				{
					org.apache.hadoop.metrics2.util.MBeans.unregister(mbeanName);
					mbeanName = null;
				}
				LOG.info(prefix + " metrics system shutdown complete.");
				return true;
			}
		}

		public override org.apache.hadoop.metrics2.MetricsSource getSource(string name)
		{
			return allSources[name];
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual org.apache.hadoop.metrics2.impl.MetricsSourceAdapter getSourceAdapter
			(string name)
		{
			return sources[name];
		}

		private org.apache.hadoop.metrics2.impl.MetricsSystemImpl.InitMode initMode()
		{
			LOG.debug("from system property: " + Sharpen.Runtime.getProperty(MS_INIT_MODE_KEY
				));
			LOG.debug("from environment variable: " + Sharpen.Runtime.getenv(MS_INIT_MODE_KEY
				));
			string m = Sharpen.Runtime.getProperty(MS_INIT_MODE_KEY);
			string m2 = m == null ? Sharpen.Runtime.getenv(MS_INIT_MODE_KEY) : m;
			return org.apache.hadoop.metrics2.impl.MetricsSystemImpl.InitMode.valueOf(org.apache.hadoop.util.StringUtils
				.toUpperCase((m2 == null ? org.apache.hadoop.metrics2.impl.MetricsSystemImpl.InitMode
				.NORMAL.ToString() : m2)));
		}
	}
}
