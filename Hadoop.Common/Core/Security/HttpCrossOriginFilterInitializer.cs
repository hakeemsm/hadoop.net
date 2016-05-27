using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security.Http;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public class HttpCrossOriginFilterInitializer : FilterInitializer
	{
		public const string Prefix = "hadoop.http.cross-origin.";

		public const string EnabledSuffix = "enabled";

		private static readonly Log Log = LogFactory.GetLog(typeof(HttpCrossOriginFilterInitializer
			));

		public override void InitFilter(FilterContainer container, Configuration conf)
		{
			string key = GetEnabledConfigKey();
			bool enabled = conf.GetBoolean(key, false);
			if (enabled)
			{
				container.AddGlobalFilter("Cross Origin Filter", typeof(CrossOriginFilter).FullName
					, GetFilterParameters(conf, GetPrefix()));
			}
			else
			{
				Log.Info("CORS filter not enabled. Please set " + key + " to 'true' to enable it"
					);
			}
		}

		protected internal static IDictionary<string, string> GetFilterParameters(Configuration
			 conf, string prefix)
		{
			IDictionary<string, string> filterParams = new Dictionary<string, string>();
			foreach (KeyValuePair<string, string> entry in conf.GetValByRegex(prefix))
			{
				string name = entry.Key;
				string value = entry.Value;
				name = Sharpen.Runtime.Substring(name, prefix.Length);
				filterParams[name] = value;
			}
			return filterParams;
		}

		protected internal virtual string GetPrefix()
		{
			return HttpCrossOriginFilterInitializer.Prefix;
		}

		protected internal virtual string GetEnabledConfigKey()
		{
			return GetPrefix() + HttpCrossOriginFilterInitializer.EnabledSuffix;
		}
	}
}
