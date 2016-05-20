using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	/// <summary>Metrics configuration for MetricsSystemImpl</summary>
	internal class MetricsConfig : org.apache.commons.configuration.SubsetConfiguration
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.impl.MetricsConfig
			)));

		internal const string DEFAULT_FILE_NAME = "hadoop-metrics2.properties";

		internal const string PREFIX_DEFAULT = "*.";

		internal const string PERIOD_KEY = "period";

		internal const int PERIOD_DEFAULT = 10;

		internal const string QUEUE_CAPACITY_KEY = "queue.capacity";

		internal const int QUEUE_CAPACITY_DEFAULT = 1;

		internal const string RETRY_DELAY_KEY = "retry.delay";

		internal const int RETRY_DELAY_DEFAULT = 10;

		internal const string RETRY_BACKOFF_KEY = "retry.backoff";

		internal const int RETRY_BACKOFF_DEFAULT = 2;

		internal const string RETRY_COUNT_KEY = "retry.count";

		internal const int RETRY_COUNT_DEFAULT = 1;

		internal const string JMX_CACHE_TTL_KEY = "jmx.cache.ttl";

		internal const string START_MBEANS_KEY = "source.start_mbeans";

		internal const string PLUGIN_URLS_KEY = "plugin.urls";

		internal const string CONTEXT_KEY = "context";

		internal const string NAME_KEY = "name";

		internal const string DESC_KEY = "description";

		internal const string SOURCE_KEY = "source";

		internal const string SINK_KEY = "sink";

		internal const string METRIC_FILTER_KEY = "metric.filter";

		internal const string RECORD_FILTER_KEY = "record.filter";

		internal const string SOURCE_FILTER_KEY = "source.filter";

		internal static readonly java.util.regex.Pattern INSTANCE_REGEX = java.util.regex.Pattern
			.compile("([^.*]+)\\..+");

		internal static readonly com.google.common.@base.Splitter SPLITTER = com.google.common.@base.Splitter
			.on(',').trimResults();

		private java.lang.ClassLoader pluginLoader;

		internal MetricsConfig(org.apache.commons.configuration.Configuration c, string prefix
			)
			: base(c, org.apache.hadoop.util.StringUtils.toLowerCase(prefix), ".")
		{
		}

		// seconds
		// seconds
		// back off factor
		internal static org.apache.hadoop.metrics2.impl.MetricsConfig create(string prefix
			)
		{
			return loadFirst(prefix, "hadoop-metrics2-" + org.apache.hadoop.util.StringUtils.
				toLowerCase(prefix) + ".properties", DEFAULT_FILE_NAME);
		}

		internal static org.apache.hadoop.metrics2.impl.MetricsConfig create(string prefix
			, params string[] fileNames)
		{
			return loadFirst(prefix, fileNames);
		}

		/// <summary>Load configuration from a list of files until the first successful load</summary>
		/// <param name="conf">the configuration object</param>
		/// <param name="files">the list of filenames to try</param>
		/// <returns>the configuration object</returns>
		internal static org.apache.hadoop.metrics2.impl.MetricsConfig loadFirst(string prefix
			, params string[] fileNames)
		{
			foreach (string fname in fileNames)
			{
				try
				{
					org.apache.commons.configuration.Configuration cf = new org.apache.commons.configuration.PropertiesConfiguration
						(fname).interpolatedConfiguration();
					LOG.info("loaded properties from " + fname);
					LOG.debug(toString(cf));
					org.apache.hadoop.metrics2.impl.MetricsConfig mc = new org.apache.hadoop.metrics2.impl.MetricsConfig
						(cf, prefix);
					LOG.debug(mc);
					return mc;
				}
				catch (org.apache.commons.configuration.ConfigurationException e)
				{
					if (e.Message.StartsWith("Cannot locate configuration"))
					{
						continue;
					}
					throw new org.apache.hadoop.metrics2.impl.MetricsConfigException(e);
				}
			}
			LOG.warn("Cannot locate configuration: tried " + com.google.common.@base.Joiner.on
				(",").join(fileNames));
			// default to an empty configuration
			return new org.apache.hadoop.metrics2.impl.MetricsConfig(new org.apache.commons.configuration.PropertiesConfiguration
				(), prefix);
		}

		public override org.apache.commons.configuration.Configuration subset(string prefix
			)
		{
			return new org.apache.hadoop.metrics2.impl.MetricsConfig(this, prefix);
		}

		/// <summary>Return sub configs for instance specified in the config.</summary>
		/// <remarks>
		/// Return sub configs for instance specified in the config.
		/// Assuming format specified as follows:<pre>
		/// [type].[instance].[option] = [value]</pre>
		/// Note, '*' is a special default instance, which is excluded in the result.
		/// </remarks>
		/// <param name="type">of the instance</param>
		/// <returns>a map with [instance] as key and config object as value</returns>
		internal virtual System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.impl.MetricsConfig
			> getInstanceConfigs(string type)
		{
			System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.impl.MetricsConfig
				> map = com.google.common.collect.Maps.newHashMap();
			org.apache.hadoop.metrics2.impl.MetricsConfig sub = ((org.apache.hadoop.metrics2.impl.MetricsConfig
				)subset(type));
			foreach (string key in sub.keys())
			{
				java.util.regex.Matcher matcher = INSTANCE_REGEX.matcher(key);
				if (matcher.matches())
				{
					string instance = matcher.group(1);
					if (!map.Contains(instance))
					{
						map[instance] = ((org.apache.hadoop.metrics2.impl.MetricsConfig)sub.subset(instance
							));
					}
				}
			}
			return map;
		}

		internal virtual System.Collections.Generic.IEnumerable<string> keys()
		{
			return new _IEnumerable_161(this);
		}

		private sealed class _IEnumerable_161 : System.Collections.Generic.IEnumerable<string
			>
		{
			public _IEnumerable_161(MetricsConfig _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override System.Collections.Generic.IEnumerator<string> GetEnumerator()
			{
				return (System.Collections.Generic.IEnumerator<string>)this._enclosing.getKeys();
			}

			private readonly MetricsConfig _enclosing;
		}

		/// <summary>Will poke parents for defaults</summary>
		/// <param name="key">to lookup</param>
		/// <returns>the value or null</returns>
		public override object getProperty(string key)
		{
			object value = base.getProperty(key);
			if (value == null)
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("poking parent '" + Sharpen.Runtime.getClassForObject(getParent()).getSimpleName
						() + "' for key: " + key);
				}
				return getParent().getProperty(key.StartsWith(PREFIX_DEFAULT) ? key : PREFIX_DEFAULT
					 + key);
			}
			if (LOG.isDebugEnabled())
			{
				LOG.debug("returning '" + value + "' for key: " + key);
			}
			return value;
		}

		internal virtual T getPlugin<T>(string name)
			where T : org.apache.hadoop.metrics2.MetricsPlugin
		{
			string clsName = getClassName(name);
			if (clsName == null)
			{
				return null;
			}
			try
			{
				java.lang.Class cls = java.lang.Class.forName(clsName, true, getPluginLoader());
				T plugin = (T)cls.newInstance();
				plugin.init(name.isEmpty() ? this : ((org.apache.hadoop.metrics2.impl.MetricsConfig
					)subset(name)));
				return plugin;
			}
			catch (System.Exception e)
			{
				throw new org.apache.hadoop.metrics2.impl.MetricsConfigException("Error creating plugin: "
					 + clsName, e);
			}
		}

		internal virtual string getClassName(string prefix)
		{
			string classKey = prefix.isEmpty() ? "class" : prefix + ".class";
			string clsName = getString(classKey);
			LOG.debug(clsName);
			if (clsName == null || clsName.isEmpty())
			{
				return null;
			}
			return clsName;
		}

		internal virtual java.lang.ClassLoader getPluginLoader()
		{
			if (pluginLoader != null)
			{
				return pluginLoader;
			}
			java.lang.ClassLoader defaultLoader = Sharpen.Runtime.getClassForObject(this).getClassLoader
				();
			object purls = base.getProperty(PLUGIN_URLS_KEY);
			if (purls == null)
			{
				return defaultLoader;
			}
			System.Collections.Generic.IEnumerable<string> jars = SPLITTER.split((string)purls
				);
			int len = com.google.common.collect.Iterables.size(jars);
			if (len > 0)
			{
				java.net.URL[] urls = new java.net.URL[len];
				try
				{
					int i = 0;
					foreach (string jar in jars)
					{
						LOG.debug(jar);
						urls[i++] = new java.net.URL(jar);
					}
				}
				catch (System.Exception e)
				{
					throw new org.apache.hadoop.metrics2.impl.MetricsConfigException(e);
				}
				if (LOG.isDebugEnabled())
				{
					LOG.debug("using plugin jars: " + com.google.common.collect.Iterables.toString(jars
						));
				}
				pluginLoader = java.security.AccessController.doPrivileged(new _PrivilegedAction_239
					(urls, defaultLoader));
				return pluginLoader;
			}
			if (parent is org.apache.hadoop.metrics2.impl.MetricsConfig)
			{
				return ((org.apache.hadoop.metrics2.impl.MetricsConfig)parent).getPluginLoader();
			}
			return defaultLoader;
		}

		private sealed class _PrivilegedAction_239 : java.security.PrivilegedAction<java.lang.ClassLoader
			>
		{
			public _PrivilegedAction_239(java.net.URL[] urls, java.lang.ClassLoader defaultLoader
				)
			{
				this.urls = urls;
				this.defaultLoader = defaultLoader;
			}

			public java.lang.ClassLoader run()
			{
				return new java.net.URLClassLoader(urls, defaultLoader);
			}

			private readonly java.net.URL[] urls;

			private readonly java.lang.ClassLoader defaultLoader;
		}

		public override void clear()
		{
			base.clear();
		}

		// pluginLoader.close(); // jdk7 is saner
		internal virtual org.apache.hadoop.metrics2.MetricsFilter getFilter(string prefix
			)
		{
			// don't create filter instances without out options
			org.apache.hadoop.metrics2.impl.MetricsConfig conf = ((org.apache.hadoop.metrics2.impl.MetricsConfig
				)subset(prefix));
			if (conf.isEmpty())
			{
				return null;
			}
			org.apache.hadoop.metrics2.MetricsFilter filter = getPlugin(prefix);
			if (filter != null)
			{
				return filter;
			}
			// glob filter is assumed if pattern is specified but class is not.
			filter = new org.apache.hadoop.metrics2.filter.GlobFilter();
			filter.init(conf);
			return filter;
		}

		public override string ToString()
		{
			return toString(this);
		}

		internal static string toString(org.apache.commons.configuration.Configuration c)
		{
			java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
			try
			{
				System.IO.TextWriter ps = new System.IO.TextWriter(buffer, false, "UTF-8");
				org.apache.commons.configuration.PropertiesConfiguration tmp = new org.apache.commons.configuration.PropertiesConfiguration
					();
				tmp.copy(c);
				tmp.save(ps);
				return buffer.toString("UTF-8");
			}
			catch (System.Exception e)
			{
				throw new org.apache.hadoop.metrics2.impl.MetricsConfigException(e);
			}
		}
	}
}
