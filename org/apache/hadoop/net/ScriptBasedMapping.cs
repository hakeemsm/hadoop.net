using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>
	/// This class implements the
	/// <see cref="DNSToSwitchMapping"/>
	/// interface using a
	/// script configured via the
	/// <see cref="org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY
	/// 	"/>
	/// option.
	/// <p/>
	/// It contains a static class <code>RawScriptBasedMapping</code> that performs
	/// the work: reading the configuration parameters, executing any defined
	/// script, handling errors and such like. The outer
	/// class extends
	/// <see cref="CachedDNSToSwitchMapping"/>
	/// to cache the delegated
	/// queries.
	/// <p/>
	/// This DNS mapper's
	/// <see cref="CachedDNSToSwitchMapping.isSingleSwitch()"/>
	/// predicate returns
	/// true if and only if a script is defined.
	/// </summary>
	public class ScriptBasedMapping : org.apache.hadoop.net.CachedDNSToSwitchMapping
	{
		/// <summary>
		/// Minimum number of arguments:
		/// <value/>
		/// </summary>
		internal const int MIN_ALLOWABLE_ARGS = 1;

		/// <summary>
		/// Default number of arguments:
		/// <value/>
		/// </summary>
		internal const int DEFAULT_ARG_COUNT = org.apache.hadoop.fs.CommonConfigurationKeys
			.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_DEFAULT;

		/// <summary>
		/// key to the script filename
		/// <value/>
		/// </summary>
		internal const string SCRIPT_FILENAME_KEY = org.apache.hadoop.fs.CommonConfigurationKeys
			.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY;

		/// <summary>
		/// key to the argument count that the script supports
		/// <value/>
		/// </summary>
		internal const string SCRIPT_ARG_COUNT_KEY = org.apache.hadoop.fs.CommonConfigurationKeys
			.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY;

		/// <summary>
		/// Text used in the
		/// <see cref="ToString()"/>
		/// method if there is no string
		/// <value/>
		/// </summary>
		public const string NO_SCRIPT = "no script";

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
		public ScriptBasedMapping()
			: this(new org.apache.hadoop.net.ScriptBasedMapping.RawScriptBasedMapping())
		{
		}

		/// <summary>Create an instance from the given raw mapping</summary>
		/// <param name="rawMap">raw DNSTOSwithMapping</param>
		public ScriptBasedMapping(org.apache.hadoop.net.DNSToSwitchMapping rawMap)
			: base(rawMap)
		{
		}

		/// <summary>Create an instance from the given configuration</summary>
		/// <param name="conf">configuration</param>
		public ScriptBasedMapping(org.apache.hadoop.conf.Configuration conf)
			: this()
		{
			setConf(conf);
		}

		/// <summary>Get the cached mapping and convert it to its real type</summary>
		/// <returns>the inner raw script mapping.</returns>
		private org.apache.hadoop.net.ScriptBasedMapping.RawScriptBasedMapping getRawMapping
			()
		{
			return (org.apache.hadoop.net.ScriptBasedMapping.RawScriptBasedMapping)rawMapping;
		}

		public override org.apache.hadoop.conf.Configuration getConf()
		{
			return getRawMapping().getConf();
		}

		public override string ToString()
		{
			return "script-based mapping with " + getRawMapping().ToString();
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
			getRawMapping().setConf(conf);
		}

		/// <summary>
		/// This is the uncached script mapping that is fed into the cache managed
		/// by the superclass
		/// <see cref="CachedDNSToSwitchMapping"/>
		/// </summary>
		protected internal class RawScriptBasedMapping : org.apache.hadoop.net.AbstractDNSToSwitchMapping
		{
			private string scriptName;

			private int maxArgs;

			private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
				.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.net.ScriptBasedMapping
				)));

			//max hostnames per call of the script
			/// <summary>Set the configuration and extract the configuration parameters of interest
			/// 	</summary>
			/// <param name="conf">the new configuration</param>
			public override void setConf(org.apache.hadoop.conf.Configuration conf)
			{
				base.setConf(conf);
				if (conf != null)
				{
					scriptName = conf.get(SCRIPT_FILENAME_KEY);
					maxArgs = conf.getInt(SCRIPT_ARG_COUNT_KEY, DEFAULT_ARG_COUNT);
				}
				else
				{
					scriptName = null;
					maxArgs = 0;
				}
			}

			/// <summary>Constructor.</summary>
			/// <remarks>
			/// Constructor. The mapping is not ready to use until
			/// <see cref="setConf(org.apache.hadoop.conf.Configuration)"/>
			/// has been called
			/// </remarks>
			public RawScriptBasedMapping()
			{
			}

			public override System.Collections.Generic.IList<string> resolve(System.Collections.Generic.IList
				<string> names)
			{
				System.Collections.Generic.IList<string> m = new System.Collections.Generic.List<
					string>(names.Count);
				if (names.isEmpty())
				{
					return m;
				}
				if (scriptName == null)
				{
					foreach (string name in names)
					{
						m.add(org.apache.hadoop.net.NetworkTopology.DEFAULT_RACK);
					}
					return m;
				}
				string output = runResolveCommand(names, scriptName);
				if (output != null)
				{
					java.util.StringTokenizer allSwitchInfo = new java.util.StringTokenizer(output);
					while (allSwitchInfo.hasMoreTokens())
					{
						string switchInfo = allSwitchInfo.nextToken();
						m.add(switchInfo);
					}
					if (m.Count != names.Count)
					{
						// invalid number of entries returned by the script
						LOG.error("Script " + scriptName + " returned " + int.toString(m.Count) + " values when "
							 + int.toString(names.Count) + " were expected.");
						return null;
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

			/// <summary>Build and execute the resolution command.</summary>
			/// <remarks>
			/// Build and execute the resolution command. The command is
			/// executed in the directory specified by the system property
			/// "user.dir" if set; otherwise the current working directory is used
			/// </remarks>
			/// <param name="args">a list of arguments</param>
			/// <returns>
			/// null if the number of arguments is out of range,
			/// or the output of the command.
			/// </returns>
			protected internal virtual string runResolveCommand(System.Collections.Generic.IList
				<string> args, string commandScriptName)
			{
				int loopCount = 0;
				if (args.Count == 0)
				{
					return null;
				}
				java.lang.StringBuilder allOutput = new java.lang.StringBuilder();
				int numProcessed = 0;
				if (maxArgs < MIN_ALLOWABLE_ARGS)
				{
					LOG.warn("Invalid value " + int.toString(maxArgs) + " for " + SCRIPT_ARG_COUNT_KEY
						 + "; must be >= " + int.toString(MIN_ALLOWABLE_ARGS));
					return null;
				}
				while (numProcessed != args.Count)
				{
					int start = maxArgs * loopCount;
					System.Collections.Generic.IList<string> cmdList = new System.Collections.Generic.List
						<string>();
					cmdList.add(commandScriptName);
					for (numProcessed = start; numProcessed < (start + maxArgs) && numProcessed < args
						.Count; numProcessed++)
					{
						cmdList.add(args[numProcessed]);
					}
					java.io.File dir = null;
					string userDir;
					if ((userDir = Sharpen.Runtime.getProperty("user.dir")) != null)
					{
						dir = new java.io.File(userDir);
					}
					org.apache.hadoop.util.Shell.ShellCommandExecutor s = new org.apache.hadoop.util.Shell.ShellCommandExecutor
						(Sharpen.Collections.ToArray(cmdList, new string[cmdList.Count]), dir);
					try
					{
						s.execute();
						allOutput.Append(s.getOutput()).Append(" ");
					}
					catch (System.Exception e)
					{
						LOG.warn("Exception running " + s, e);
						return null;
					}
					loopCount++;
				}
				return allOutput.ToString();
			}

			/// <summary>
			/// Declare that the mapper is single-switched if a script was not named
			/// in the configuration.
			/// </summary>
			/// <returns>true iff there is no script</returns>
			public override bool isSingleSwitch()
			{
				return scriptName == null;
			}

			public override string ToString()
			{
				return scriptName != null ? ("script " + scriptName) : NO_SCRIPT;
			}

			public override void reloadCachedMappings()
			{
			}

			// Nothing to do here, since RawScriptBasedMapping has no cache, and
			// does not inherit from CachedDNSToSwitchMapping
			public override void reloadCachedMappings(System.Collections.Generic.IList<string
				> names)
			{
			}
			// Nothing to do here, since RawScriptBasedMapping has no cache, and
			// does not inherit from CachedDNSToSwitchMapping
		}
	}
}
