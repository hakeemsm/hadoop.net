using System;
using System.Collections.Generic;
using System.Text;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Net
{
	/// <summary>
	/// This class implements the
	/// <see cref="DNSToSwitchMapping"/>
	/// interface using a
	/// script configured via the
	/// <see cref="Org.Apache.Hadoop.FS.CommonConfigurationKeysPublic.NetTopologyScriptFileNameKey
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
	/// <see cref="CachedDNSToSwitchMapping.IsSingleSwitch()"/>
	/// predicate returns
	/// true if and only if a script is defined.
	/// </summary>
	public class ScriptBasedMapping : CachedDNSToSwitchMapping
	{
		/// <summary>
		/// Minimum number of arguments:
		/// <value/>
		/// </summary>
		internal const int MinAllowableArgs = 1;

		/// <summary>
		/// Default number of arguments:
		/// <value/>
		/// </summary>
		internal const int DefaultArgCount = CommonConfigurationKeys.NetTopologyScriptNumberArgsDefault;

		/// <summary>
		/// key to the script filename
		/// <value/>
		/// </summary>
		internal const string ScriptFilenameKey = CommonConfigurationKeys.NetTopologyScriptFileNameKey;

		/// <summary>
		/// key to the argument count that the script supports
		/// <value/>
		/// </summary>
		internal const string ScriptArgCountKey = CommonConfigurationKeys.NetTopologyScriptNumberArgsKey;

		/// <summary>
		/// Text used in the
		/// <see cref="ToString()"/>
		/// method if there is no string
		/// <value/>
		/// </summary>
		public const string NoScript = "no script";

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
		public ScriptBasedMapping()
			: this(new ScriptBasedMapping.RawScriptBasedMapping())
		{
		}

		/// <summary>Create an instance from the given raw mapping</summary>
		/// <param name="rawMap">raw DNSTOSwithMapping</param>
		public ScriptBasedMapping(DNSToSwitchMapping rawMap)
			: base(rawMap)
		{
		}

		/// <summary>Create an instance from the given configuration</summary>
		/// <param name="conf">configuration</param>
		public ScriptBasedMapping(Configuration conf)
			: this()
		{
			SetConf(conf);
		}

		/// <summary>Get the cached mapping and convert it to its real type</summary>
		/// <returns>the inner raw script mapping.</returns>
		private ScriptBasedMapping.RawScriptBasedMapping GetRawMapping()
		{
			return (ScriptBasedMapping.RawScriptBasedMapping)rawMapping;
		}

		public override Configuration GetConf()
		{
			return GetRawMapping().GetConf();
		}

		public override string ToString()
		{
			return "script-based mapping with " + GetRawMapping().ToString();
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
			GetRawMapping().SetConf(conf);
		}

		/// <summary>
		/// This is the uncached script mapping that is fed into the cache managed
		/// by the superclass
		/// <see cref="CachedDNSToSwitchMapping"/>
		/// </summary>
		protected internal class RawScriptBasedMapping : AbstractDNSToSwitchMapping
		{
			private string scriptName;

			private int maxArgs;

			private static readonly Log Log = LogFactory.GetLog(typeof(ScriptBasedMapping));

			//max hostnames per call of the script
			/// <summary>Set the configuration and extract the configuration parameters of interest
			/// 	</summary>
			/// <param name="conf">the new configuration</param>
			public override void SetConf(Configuration conf)
			{
				base.SetConf(conf);
				if (conf != null)
				{
					scriptName = conf.Get(ScriptFilenameKey);
					maxArgs = conf.GetInt(ScriptArgCountKey, DefaultArgCount);
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
			/// <see cref="SetConf(Configuration)"/>
			/// has been called
			/// </remarks>
			public RawScriptBasedMapping()
			{
			}

			public override IList<string> Resolve(IList<string> names)
			{
				IList<string> m = new AList<string>(names.Count);
				if (names.IsEmpty())
				{
					return m;
				}
				if (scriptName == null)
				{
					foreach (string name in names)
					{
						m.AddItem(NetworkTopology.DefaultRack);
					}
					return m;
				}
				string output = RunResolveCommand(names, scriptName);
				if (output != null)
				{
					StringTokenizer allSwitchInfo = new StringTokenizer(output);
					while (allSwitchInfo.HasMoreTokens())
					{
						string switchInfo = allSwitchInfo.NextToken();
						m.AddItem(switchInfo);
					}
					if (m.Count != names.Count)
					{
						// invalid number of entries returned by the script
						Log.Error("Script " + scriptName + " returned " + Extensions.ToString(m.Count
							) + " values when " + Extensions.ToString(names.Count) + " were expected."
							);
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
			protected internal virtual string RunResolveCommand(IList<string> args, string commandScriptName
				)
			{
				int loopCount = 0;
				if (args.Count == 0)
				{
					return null;
				}
				StringBuilder allOutput = new StringBuilder();
				int numProcessed = 0;
				if (maxArgs < MinAllowableArgs)
				{
					Log.Warn("Invalid value " + Extensions.ToString(maxArgs) + " for " + ScriptArgCountKey
						 + "; must be >= " + Extensions.ToString(MinAllowableArgs));
					return null;
				}
				while (numProcessed != args.Count)
				{
					int start = maxArgs * loopCount;
					IList<string> cmdList = new AList<string>();
					cmdList.AddItem(commandScriptName);
					for (numProcessed = start; numProcessed < (start + maxArgs) && numProcessed < args
						.Count; numProcessed++)
					{
						cmdList.AddItem(args[numProcessed]);
					}
					FilePath dir = null;
					string userDir;
					if ((userDir = Runtime.GetProperty("user.dir")) != null)
					{
						dir = new FilePath(userDir);
					}
					Shell.ShellCommandExecutor s = new Shell.ShellCommandExecutor(Collections.ToArray
						(cmdList, new string[cmdList.Count]), dir);
					try
					{
						s.Execute();
						allOutput.Append(s.GetOutput()).Append(" ");
					}
					catch (Exception e)
					{
						Log.Warn("Exception running " + s, e);
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
			public override bool IsSingleSwitch()
			{
				return scriptName == null;
			}

			public override string ToString()
			{
				return scriptName != null ? ("script " + scriptName) : NoScript;
			}

			public override void ReloadCachedMappings()
			{
			}

			// Nothing to do here, since RawScriptBasedMapping has no cache, and
			// does not inherit from CachedDNSToSwitchMapping
			public override void ReloadCachedMappings(IList<string> names)
			{
			}
			// Nothing to do here, since RawScriptBasedMapping has no cache, and
			// does not inherit from CachedDNSToSwitchMapping
		}
	}
}
