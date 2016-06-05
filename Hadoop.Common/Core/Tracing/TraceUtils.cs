using System.Collections.Generic;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Htrace;


namespace Org.Apache.Hadoop.Tracing
{
	/// <summary>This class provides utility functions for tracing.</summary>
	public class TraceUtils
	{
		private static IList<SpanReceiverInfo.ConfigurationPair> Empty = Collections
			.EmptyList();

		public static HTraceConfiguration WrapHadoopConf(string prefix, Configuration conf
			)
		{
			return WrapHadoopConf(prefix, conf, Empty);
		}

		public static HTraceConfiguration WrapHadoopConf(string prefix, Configuration conf
			, IList<SpanReceiverInfo.ConfigurationPair> extraConfig)
		{
			Dictionary<string, string> extraMap = new Dictionary<string, string>();
			foreach (SpanReceiverInfo.ConfigurationPair pair in extraConfig)
			{
				extraMap[pair.GetKey()] = pair.GetValue();
			}
			return new _HTraceConfiguration_47(extraMap, conf, prefix);
		}

		private sealed class _HTraceConfiguration_47 : HTraceConfiguration
		{
			public _HTraceConfiguration_47(Dictionary<string, string> extraMap, Configuration
				 conf, string prefix)
			{
				this.extraMap = extraMap;
				this.conf = conf;
				this.prefix = prefix;
			}

			public override string Get(string key)
			{
				if (extraMap.Contains(key))
				{
					return extraMap[key];
				}
				return conf.Get(prefix + key, string.Empty);
			}

			public override string Get(string key, string defaultValue)
			{
				if (extraMap.Contains(key))
				{
					return extraMap[key];
				}
				return conf.Get(prefix + key, defaultValue);
			}

			private readonly Dictionary<string, string> extraMap;

			private readonly Configuration conf;

			private readonly string prefix;
		}
	}
}
