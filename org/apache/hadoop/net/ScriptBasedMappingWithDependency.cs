using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>
	/// This class extends ScriptBasedMapping class and implements
	/// the
	/// <see cref="DNSToSwitchMappingWithDependency"/>
	/// interface using
	/// a script configured via the
	/// <see cref="org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_DEPENDENCY_SCRIPT_FILE_NAME_KEY
	/// 	"/>
	/// option.
	/// <p/>
	/// It contains a static class <code>RawScriptBasedMappingWithDependency</code>
	/// that performs the getDependency work.
	/// <p/>
	/// </summary>
	public class ScriptBasedMappingWithDependency : org.apache.hadoop.net.ScriptBasedMapping
		, org.apache.hadoop.net.DNSToSwitchMappingWithDependency
	{
		/// <summary>
		/// key to the dependency script filename
		/// <value/>
		/// </summary>
		internal const string DEPENDENCY_SCRIPT_FILENAME_KEY = org.apache.hadoop.fs.CommonConfigurationKeys
			.NET_DEPENDENCY_SCRIPT_FILE_NAME_KEY;

		private System.Collections.Generic.IDictionary<string, System.Collections.Generic.IList
			<string>> dependencyCache = new java.util.concurrent.ConcurrentHashMap<string, System.Collections.Generic.IList
			<string>>();

		/// <summary>Create an instance with the default configuration.</summary>
		/// <remarks>
		/// Create an instance with the default configuration.
		/// </p>
		/// Calling
		/// <see cref="setConf(org.apache.hadoop.conf.Configuration)"/>
		/// will trigger a
		/// re-evaluation of the configuration settings and so be used to
		/// set up the mapping script.
		/// </remarks>
		public ScriptBasedMappingWithDependency()
			: base(new org.apache.hadoop.net.ScriptBasedMappingWithDependency.RawScriptBasedMappingWithDependency
				())
		{
		}

		/// <summary>Get the cached mapping and convert it to its real type</summary>
		/// <returns>the inner raw script mapping.</returns>
		private org.apache.hadoop.net.ScriptBasedMapping.RawScriptBasedMapping getRawMapping
			()
		{
			return (org.apache.hadoop.net.ScriptBasedMappingWithDependency.RawScriptBasedMappingWithDependency
				)rawMapping;
		}

		public override string ToString()
		{
			return "script-based mapping with " + ((org.apache.hadoop.net.ScriptBasedMappingWithDependency.RawScriptBasedMappingWithDependency
				)getRawMapping()).ToString();
		}

		/// <summary>
		/// <inheritDoc/>
		/// <p/>
		/// This will get called in the superclass constructor, so a check is needed
		/// to ensure that the raw mapping is defined before trying to relaying a null
		/// configuration.
		/// </summary>
		/// <param name="conf"/>
		public override void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			base.setConf(conf);
			((org.apache.hadoop.net.ScriptBasedMappingWithDependency.RawScriptBasedMappingWithDependency
				)getRawMapping()).setConf(conf);
		}

		/// <summary>Get dependencies in the topology for a given host</summary>
		/// <param name="name">- host name for which we are getting dependency</param>
		/// <returns>a list of hosts dependent on the provided host name</returns>
		public virtual System.Collections.Generic.IList<string> getDependency(string name
			)
		{
			//normalize all input names to be in the form of IP addresses
			name = org.apache.hadoop.net.NetUtils.normalizeHostName(name);
			if (name == null)
			{
				return java.util.Collections.emptyList();
			}
			System.Collections.Generic.IList<string> dependencies = dependencyCache[name];
			if (dependencies == null)
			{
				//not cached
				dependencies = ((org.apache.hadoop.net.ScriptBasedMappingWithDependency.RawScriptBasedMappingWithDependency
					)getRawMapping()).getDependency(name);
				if (dependencies != null)
				{
					dependencyCache[name] = dependencies;
				}
			}
			return dependencies;
		}

		/// <summary>
		/// This is the uncached script mapping that is fed into the cache managed
		/// by the superclass
		/// <see cref="CachedDNSToSwitchMapping"/>
		/// </summary>
		private sealed class RawScriptBasedMappingWithDependency : org.apache.hadoop.net.ScriptBasedMapping.RawScriptBasedMapping
			, org.apache.hadoop.net.DNSToSwitchMappingWithDependency
		{
			private string dependencyScriptName;

			/// <summary>Set the configuration and extract the configuration parameters of interest
			/// 	</summary>
			/// <param name="conf">the new configuration</param>
			public override void setConf(org.apache.hadoop.conf.Configuration conf)
			{
				base.setConf(conf);
				if (conf != null)
				{
					dependencyScriptName = conf.get(DEPENDENCY_SCRIPT_FILENAME_KEY);
				}
				else
				{
					dependencyScriptName = null;
				}
			}

			/// <summary>Constructor.</summary>
			/// <remarks>
			/// Constructor. The mapping is not ready to use until
			/// <see cref="setConf(org.apache.hadoop.conf.Configuration)"/>
			/// has been called
			/// </remarks>
			public RawScriptBasedMappingWithDependency()
			{
			}

			public System.Collections.Generic.IList<string> getDependency(string name)
			{
				if (name == null || dependencyScriptName == null)
				{
					return java.util.Collections.emptyList();
				}
				System.Collections.Generic.IList<string> m = new System.Collections.Generic.LinkedList
					<string>();
				System.Collections.Generic.IList<string> args = new System.Collections.Generic.List
					<string>(1);
				args.add(name);
				string output = runResolveCommand(args, dependencyScriptName);
				if (output != null)
				{
					java.util.StringTokenizer allSwitchInfo = new java.util.StringTokenizer(output);
					while (allSwitchInfo.hasMoreTokens())
					{
						string switchInfo = allSwitchInfo.nextToken();
						m.add(switchInfo);
					}
				}
				else
				{
					// an error occurred. return null to signify this.
					// (exn was already logged in runResolveCommand)
					return null;
				}
				return m;
			}

			public override string ToString()
			{
				return "dependency script " + dependencyScriptName;
			}
		}
	}
}
