using Sharpen;

namespace org.apache.hadoop.security
{
	public class HttpCrossOriginFilterInitializer : org.apache.hadoop.http.FilterInitializer
	{
		public const string PREFIX = "hadoop.http.cross-origin.";

		public const string ENABLED_SUFFIX = "enabled";

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.HttpCrossOriginFilterInitializer
			)));

		public override void initFilter(org.apache.hadoop.http.FilterContainer container, 
			org.apache.hadoop.conf.Configuration conf)
		{
			string key = getEnabledConfigKey();
			bool enabled = conf.getBoolean(key, false);
			if (enabled)
			{
				container.addGlobalFilter("Cross Origin Filter", Sharpen.Runtime.getClassForType(
					typeof(org.apache.hadoop.security.http.CrossOriginFilter)).getName(), getFilterParameters
					(conf, getPrefix()));
			}
			else
			{
				LOG.info("CORS filter not enabled. Please set " + key + " to 'true' to enable it"
					);
			}
		}

		protected internal static System.Collections.Generic.IDictionary<string, string> 
			getFilterParameters(org.apache.hadoop.conf.Configuration conf, string prefix)
		{
			System.Collections.Generic.IDictionary<string, string> filterParams = new System.Collections.Generic.Dictionary
				<string, string>();
			foreach (System.Collections.Generic.KeyValuePair<string, string> entry in conf.getValByRegex
				(prefix))
			{
				string name = entry.Key;
				string value = entry.Value;
				name = Sharpen.Runtime.substring(name, prefix.Length);
				filterParams[name] = value;
			}
			return filterParams;
		}

		protected internal virtual string getPrefix()
		{
			return org.apache.hadoop.security.HttpCrossOriginFilterInitializer.PREFIX;
		}

		protected internal virtual string getEnabledConfigKey()
		{
			return getPrefix() + org.apache.hadoop.security.HttpCrossOriginFilterInitializer.
				ENABLED_SUFFIX;
		}
	}
}
