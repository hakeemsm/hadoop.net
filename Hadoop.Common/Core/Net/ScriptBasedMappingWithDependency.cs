using System.Collections.Generic;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	/// <summary>
	/// This class extends ScriptBasedMapping class and implements
	/// the
	/// <see cref="DNSToSwitchMappingWithDependency"/>
	/// interface using
	/// a script configured via the
	/// <see cref="Org.Apache.Hadoop.FS.CommonConfigurationKeysPublic.NetDependencyScriptFileNameKey
	/// 	"/>
	/// option.
	/// <p/>
	/// It contains a static class <code>RawScriptBasedMappingWithDependency</code>
	/// that performs the getDependency work.
	/// <p/>
	/// </summary>
	public class ScriptBasedMappingWithDependency : ScriptBasedMapping, DNSToSwitchMappingWithDependency
	{
		/// <summary>
		/// key to the dependency script filename
		/// <value/>
		/// </summary>
		internal const string DependencyScriptFilenameKey = CommonConfigurationKeys.NetDependencyScriptFileNameKey;

		private IDictionary<string, IList<string>> dependencyCache = new ConcurrentHashMap
			<string, IList<string>>();

		/// <summary>Create an instance with the default configuration.</summary>
		/// <remarks>
		/// Create an instance with the default configuration.
		/// </p>
		/// Calling
		/// <see cref="SetConf(Configuration)"/>
		/// will trigger a
		/// re-evaluation of the configuration settings and so be used to
		/// set up the mapping script.
		/// </remarks>
		public ScriptBasedMappingWithDependency()
			: base(new ScriptBasedMappingWithDependency.RawScriptBasedMappingWithDependency()
				)
		{
		}

		/// <summary>Get the cached mapping and convert it to its real type</summary>
		/// <returns>the inner raw script mapping.</returns>
		private ScriptBasedMapping.RawScriptBasedMapping GetRawMapping()
		{
			return (ScriptBasedMappingWithDependency.RawScriptBasedMappingWithDependency)rawMapping;
		}

		public override string ToString()
		{
			return "script-based mapping with " + ((ScriptBasedMappingWithDependency.RawScriptBasedMappingWithDependency
				)GetRawMapping()).ToString();
		}

		/// <summary>
		/// <inheritDoc/>
		/// <p/>
		/// This will get called in the superclass constructor, so a check is needed
		/// to ensure that the raw mapping is defined before trying to relaying a null
		/// configuration.
		/// </summary>
		/// <param name="conf"/>
		public override void SetConf(Configuration conf)
		{
			base.SetConf(conf);
			((ScriptBasedMappingWithDependency.RawScriptBasedMappingWithDependency)GetRawMapping
				()).SetConf(conf);
		}

		/// <summary>Get dependencies in the topology for a given host</summary>
		/// <param name="name">- host name for which we are getting dependency</param>
		/// <returns>a list of hosts dependent on the provided host name</returns>
		public virtual IList<string> GetDependency(string name)
		{
			//normalize all input names to be in the form of IP addresses
			name = NetUtils.NormalizeHostName(name);
			if (name == null)
			{
				return Sharpen.Collections.EmptyList();
			}
			IList<string> dependencies = dependencyCache[name];
			if (dependencies == null)
			{
				//not cached
				dependencies = ((ScriptBasedMappingWithDependency.RawScriptBasedMappingWithDependency
					)GetRawMapping()).GetDependency(name);
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
		private sealed class RawScriptBasedMappingWithDependency : ScriptBasedMapping.RawScriptBasedMapping
			, DNSToSwitchMappingWithDependency
		{
			private string dependencyScriptName;

			/// <summary>Set the configuration and extract the configuration parameters of interest
			/// 	</summary>
			/// <param name="conf">the new configuration</param>
			public override void SetConf(Configuration conf)
			{
				base.SetConf(conf);
				if (conf != null)
				{
					dependencyScriptName = conf.Get(DependencyScriptFilenameKey);
				}
				else
				{
					dependencyScriptName = null;
				}
			}

			/// <summary>Constructor.</summary>
			/// <remarks>
			/// Constructor. The mapping is not ready to use until
			/// <see cref="SetConf(Configuration)"/>
			/// has been called
			/// </remarks>
			public RawScriptBasedMappingWithDependency()
			{
			}

			public IList<string> GetDependency(string name)
			{
				if (name == null || dependencyScriptName == null)
				{
					return Sharpen.Collections.EmptyList();
				}
				IList<string> m = new List<string>();
				IList<string> args = new AList<string>(1);
				args.AddItem(name);
				string output = RunResolveCommand(args, dependencyScriptName);
				if (output != null)
				{
					StringTokenizer allSwitchInfo = new StringTokenizer(output);
					while (allSwitchInfo.HasMoreTokens())
					{
						string switchInfo = allSwitchInfo.NextToken();
						m.AddItem(switchInfo);
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
