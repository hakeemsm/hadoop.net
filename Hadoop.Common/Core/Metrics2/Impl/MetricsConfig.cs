using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Configuration;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Filter;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>Metrics configuration for MetricsSystemImpl</summary>
	internal class MetricsConfig : SubsetConfiguration
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig
			));

		internal const string DefaultFileName = "hadoop-metrics2.properties";

		internal const string PrefixDefault = "*.";

		internal const string PeriodKey = "period";

		internal const int PeriodDefault = 10;

		internal const string QueueCapacityKey = "queue.capacity";

		internal const int QueueCapacityDefault = 1;

		internal const string RetryDelayKey = "retry.delay";

		internal const int RetryDelayDefault = 10;

		internal const string RetryBackoffKey = "retry.backoff";

		internal const int RetryBackoffDefault = 2;

		internal const string RetryCountKey = "retry.count";

		internal const int RetryCountDefault = 1;

		internal const string JmxCacheTtlKey = "jmx.cache.ttl";

		internal const string StartMbeansKey = "source.start_mbeans";

		internal const string PluginUrlsKey = "plugin.urls";

		internal const string ContextKey = "context";

		internal const string NameKey = "name";

		internal const string DescKey = "description";

		internal const string SourceKey = "source";

		internal const string SinkKey = "sink";

		internal const string MetricFilterKey = "metric.filter";

		internal const string RecordFilterKey = "record.filter";

		internal const string SourceFilterKey = "source.filter";

		internal static readonly Sharpen.Pattern InstanceRegex = Sharpen.Pattern.Compile(
			"([^.*]+)\\..+");

		internal static readonly Splitter Splitter = Splitter.On(',').TrimResults();

		private ClassLoader pluginLoader;

		internal MetricsConfig(Org.Apache.Commons.Configuration.Configuration c, string prefix
			)
			: base(c, StringUtils.ToLowerCase(prefix), ".")
		{
		}

		// seconds
		// seconds
		// back off factor
		internal static Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig Create(string prefix
			)
		{
			return LoadFirst(prefix, "hadoop-metrics2-" + StringUtils.ToLowerCase(prefix) + ".properties"
				, DefaultFileName);
		}

		internal static Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig Create(string prefix
			, params string[] fileNames)
		{
			return LoadFirst(prefix, fileNames);
		}

		/// <summary>Load configuration from a list of files until the first successful load</summary>
		/// <param name="conf">the configuration object</param>
		/// <param name="files">the list of filenames to try</param>
		/// <returns>the configuration object</returns>
		internal static Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig LoadFirst(string prefix
			, params string[] fileNames)
		{
			foreach (string fname in fileNames)
			{
				try
				{
					Org.Apache.Commons.Configuration.Configuration cf = new PropertiesConfiguration(fname
						).InterpolatedConfiguration();
					Log.Info("loaded properties from " + fname);
					Log.Debug(ToString(cf));
					Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig mc = new Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig
						(cf, prefix);
					Log.Debug(mc);
					return mc;
				}
				catch (ConfigurationException e)
				{
					if (e.Message.StartsWith("Cannot locate configuration"))
					{
						continue;
					}
					throw new MetricsConfigException(e);
				}
			}
			Log.Warn("Cannot locate configuration: tried " + Joiner.On(",").Join(fileNames));
			// default to an empty configuration
			return new Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig(new PropertiesConfiguration
				(), prefix);
		}

		public override Org.Apache.Commons.Configuration.Configuration Subset(string prefix
			)
		{
			return new Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig(this, prefix);
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
		internal virtual IDictionary<string, Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig
			> GetInstanceConfigs(string type)
		{
			IDictionary<string, Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig> map = Maps.NewHashMap
				();
			Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig sub = ((Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig
				)Subset(type));
			foreach (string key in sub.Keys())
			{
				Matcher matcher = InstanceRegex.Matcher(key);
				if (matcher.Matches())
				{
					string instance = matcher.Group(1);
					if (!map.Contains(instance))
					{
						map[instance] = ((Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig)sub.Subset(instance
							));
					}
				}
			}
			return map;
		}

		internal virtual IEnumerable<string> Keys()
		{
			return new _IEnumerable_161(this);
		}

		private sealed class _IEnumerable_161 : IEnumerable<string>
		{
			public _IEnumerable_161(MetricsConfig _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<string> GetEnumerator()
			{
				return (IEnumerator<string>)this._enclosing.GetKeys();
			}

			private readonly MetricsConfig _enclosing;
		}

		/// <summary>Will poke parents for defaults</summary>
		/// <param name="key">to lookup</param>
		/// <returns>the value or null</returns>
		public override object GetProperty(string key)
		{
			object value = base.GetProperty(key);
			if (value == null)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("poking parent '" + GetParent().GetType().Name + "' for key: " + key);
				}
				return GetParent().GetProperty(key.StartsWith(PrefixDefault) ? key : PrefixDefault
					 + key);
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("returning '" + value + "' for key: " + key);
			}
			return value;
		}

		internal virtual T GetPlugin<T>(string name)
			where T : MetricsPlugin
		{
			string clsName = GetClassName(name);
			if (clsName == null)
			{
				return null;
			}
			try
			{
				Type cls = Sharpen.Runtime.GetType(clsName, true, GetPluginLoader());
				T plugin = (T)System.Activator.CreateInstance(cls);
				plugin.Init(name.IsEmpty() ? this : ((Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig
					)Subset(name)));
				return plugin;
			}
			catch (Exception e)
			{
				throw new MetricsConfigException("Error creating plugin: " + clsName, e);
			}
		}

		internal virtual string GetClassName(string prefix)
		{
			string classKey = prefix.IsEmpty() ? "class" : prefix + ".class";
			string clsName = GetString(classKey);
			Log.Debug(clsName);
			if (clsName == null || clsName.IsEmpty())
			{
				return null;
			}
			return clsName;
		}

		internal virtual ClassLoader GetPluginLoader()
		{
			if (pluginLoader != null)
			{
				return pluginLoader;
			}
			ClassLoader defaultLoader = GetType().GetClassLoader();
			object purls = base.GetProperty(PluginUrlsKey);
			if (purls == null)
			{
				return defaultLoader;
			}
			IEnumerable<string> jars = Splitter.Split((string)purls);
			int len = Iterables.Size(jars);
			if (len > 0)
			{
				Uri[] urls = new Uri[len];
				try
				{
					int i = 0;
					foreach (string jar in jars)
					{
						Log.Debug(jar);
						urls[i++] = new Uri(jar);
					}
				}
				catch (Exception e)
				{
					throw new MetricsConfigException(e);
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("using plugin jars: " + Iterables.ToString(jars));
				}
				pluginLoader = AccessController.DoPrivileged(new _PrivilegedAction_239(urls, defaultLoader
					));
				return pluginLoader;
			}
			if (parent is Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig)
			{
				return ((Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig)parent).GetPluginLoader();
			}
			return defaultLoader;
		}

		private sealed class _PrivilegedAction_239 : PrivilegedAction<ClassLoader>
		{
			public _PrivilegedAction_239(Uri[] urls, ClassLoader defaultLoader)
			{
				this.urls = urls;
				this.defaultLoader = defaultLoader;
			}

			public ClassLoader Run()
			{
				return new URLClassLoader(urls, defaultLoader);
			}

			private readonly Uri[] urls;

			private readonly ClassLoader defaultLoader;
		}

		public override void Clear()
		{
			base.Clear();
		}

		// pluginLoader.close(); // jdk7 is saner
		internal virtual MetricsFilter GetFilter(string prefix)
		{
			// don't create filter instances without out options
			Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig conf = ((Org.Apache.Hadoop.Metrics2.Impl.MetricsConfig
				)Subset(prefix));
			if (conf.IsEmpty())
			{
				return null;
			}
			MetricsFilter filter = GetPlugin(prefix);
			if (filter != null)
			{
				return filter;
			}
			// glob filter is assumed if pattern is specified but class is not.
			filter = new GlobFilter();
			filter.Init(conf);
			return filter;
		}

		public override string ToString()
		{
			return ToString(this);
		}

		internal static string ToString(Org.Apache.Commons.Configuration.Configuration c)
		{
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			try
			{
				TextWriter ps = new TextWriter(buffer, false, "UTF-8");
				PropertiesConfiguration tmp = new PropertiesConfiguration();
				tmp.Copy(c);
				tmp.Save(ps);
				return buffer.ToString("UTF-8");
			}
			catch (Exception e)
			{
				throw new MetricsConfigException(e);
			}
		}
	}
}
