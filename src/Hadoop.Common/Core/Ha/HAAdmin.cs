using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.HA
{
	/// <summary>A command-line tool for making calls in the HAServiceProtocol.</summary>
	/// <remarks>
	/// A command-line tool for making calls in the HAServiceProtocol.
	/// For example,. this can be used to force a service to standby or active
	/// mode, or to trigger a health-check.
	/// </remarks>
	public abstract class HAAdmin : Configured, Tool
	{
		private const string Forcefence = "forcefence";

		private const string Forceactive = "forceactive";

		/// <summary>
		/// Undocumented flag which allows an administrator to use manual failover
		/// state transitions even when auto-failover is enabled.
		/// </summary>
		/// <remarks>
		/// Undocumented flag which allows an administrator to use manual failover
		/// state transitions even when auto-failover is enabled. This is an unsafe
		/// operation, which is why it is not documented in the usage below.
		/// </remarks>
		private const string Forcemanual = "forcemanual";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.HA.HAAdmin
			));

		private int rpcTimeoutForChecks = -1;

		protected internal static readonly IDictionary<string, HAAdmin.UsageInfo> Usage = 
			ImmutableMap.Builder<string, HAAdmin.UsageInfo>().Put("-transitionToActive", new 
			HAAdmin.UsageInfo("[--" + Forceactive + "] <serviceId>", "Transitions the service into Active state"
			)).Put("-transitionToStandby", new HAAdmin.UsageInfo("<serviceId>", "Transitions the service into Standby state"
			)).Put("-failover", new HAAdmin.UsageInfo("[--" + Forcefence + "] [--" + Forceactive
			 + "] <serviceId> <serviceId>", "Failover from the first service to the second.\n"
			 + "Unconditionally fence services if the --" + Forcefence + " option is used.\n"
			 + "Try to failover to the target service even if it is not ready if the " + "--"
			 + Forceactive + " option is used.")).Put("-getServiceState", new HAAdmin.UsageInfo
			("<serviceId>", "Returns the state of the service")).Put("-checkHealth", new HAAdmin.UsageInfo
			("<serviceId>", "Requests that the service perform a health check.\n" + "The HAAdmin tool will exit with a non-zero exit code\n"
			 + "if the check fails.")).Put("-help", new HAAdmin.UsageInfo("<command>", "Displays help on the specified command"
			)).Build();

		/// <summary>Output stream for errors, for use in tests</summary>
		protected internal TextWriter errOut = System.Console.Error;

		protected internal TextWriter @out = System.Console.Out;

		private HAServiceProtocol.RequestSource requestSource = HAServiceProtocol.RequestSource
			.RequestByUser;

		protected internal HAAdmin()
			: base()
		{
		}

		protected internal HAAdmin(Configuration conf)
			: base(conf)
		{
		}

		protected internal abstract HAServiceTarget ResolveTarget(string @string);

		protected internal virtual ICollection<string> GetTargetIds(string targetNodeToActivate
			)
		{
			return new AList<string>(Arrays.AsList(new string[] { targetNodeToActivate }));
		}

		protected internal virtual string GetUsageString()
		{
			return "Usage: HAAdmin";
		}

		protected internal virtual void PrintUsage(TextWriter errOut)
		{
			errOut.WriteLine(GetUsageString());
			foreach (KeyValuePair<string, HAAdmin.UsageInfo> e in Usage)
			{
				string cmd = e.Key;
				HAAdmin.UsageInfo usage = e.Value;
				errOut.WriteLine("    [" + cmd + " " + usage.args + "]");
			}
			errOut.WriteLine();
			ToolRunner.PrintGenericCommandUsage(errOut);
		}

		private void PrintUsage(TextWriter errOut, string cmd)
		{
			HAAdmin.UsageInfo usage = Usage[cmd];
			if (usage == null)
			{
				throw new RuntimeException("No usage for cmd " + cmd);
			}
			errOut.WriteLine(GetUsageString() + " [" + cmd + " " + usage.args + "]");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		private int TransitionToActive(CommandLine cmd)
		{
			string[] argv = cmd.GetArgs();
			if (argv.Length != 1)
			{
				errOut.WriteLine("transitionToActive: incorrect number of arguments");
				PrintUsage(errOut, "-transitionToActive");
				return -1;
			}
			/*  returns true if other target node is active or some exception occurred
			and forceActive was not set  */
			if (!cmd.HasOption(Forceactive))
			{
				if (IsOtherTargetNodeActive(argv[0], cmd.HasOption(Forceactive)))
				{
					return -1;
				}
			}
			HAServiceTarget target = ResolveTarget(argv[0]);
			if (!CheckManualStateManagementOK(target))
			{
				return -1;
			}
			HAServiceProtocol proto = target.GetProxy(GetConf(), 0);
			HAServiceProtocolHelper.TransitionToActive(proto, CreateReqInfo());
			return 0;
		}

		/// <summary>Checks whether other target node is active or not</summary>
		/// <param name="targetNodeToActivate"/>
		/// <returns>
		/// true if other target node is active or some other exception
		/// occurred and forceActive was set otherwise false
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		private bool IsOtherTargetNodeActive(string targetNodeToActivate, bool forceActive
			)
		{
			ICollection<string> targetIds = GetTargetIds(targetNodeToActivate);
			targetIds.Remove(targetNodeToActivate);
			foreach (string targetId in targetIds)
			{
				HAServiceTarget target = ResolveTarget(targetId);
				if (!CheckManualStateManagementOK(target))
				{
					return true;
				}
				try
				{
					HAServiceProtocol proto = target.GetProxy(GetConf(), 5000);
					if (proto.GetServiceStatus().GetState() == HAServiceProtocol.HAServiceState.Active)
					{
						errOut.WriteLine("transitionToActive: Node " + targetId + " is already active");
						PrintUsage(errOut, "-transitionToActive");
						return true;
					}
				}
				catch (Exception e)
				{
					//If forceActive switch is false then return true
					if (!forceActive)
					{
						errOut.WriteLine("Unexpected error occurred  " + e.Message);
						PrintUsage(errOut, "-transitionToActive");
						return true;
					}
				}
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		private int TransitionToStandby(CommandLine cmd)
		{
			string[] argv = cmd.GetArgs();
			if (argv.Length != 1)
			{
				errOut.WriteLine("transitionToStandby: incorrect number of arguments");
				PrintUsage(errOut, "-transitionToStandby");
				return -1;
			}
			HAServiceTarget target = ResolveTarget(argv[0]);
			if (!CheckManualStateManagementOK(target))
			{
				return -1;
			}
			HAServiceProtocol proto = target.GetProxy(GetConf(), 0);
			HAServiceProtocolHelper.TransitionToStandby(proto, CreateReqInfo());
			return 0;
		}

		/// <summary>
		/// Ensure that we are allowed to manually manage the HA state of the target
		/// service.
		/// </summary>
		/// <remarks>
		/// Ensure that we are allowed to manually manage the HA state of the target
		/// service. If automatic failover is configured, then the automatic
		/// failover controllers should be doing state management, and it is generally
		/// an error to use the HAAdmin command line to do so.
		/// </remarks>
		/// <param name="target">the target to check</param>
		/// <returns>true if manual state management is allowed</returns>
		private bool CheckManualStateManagementOK(HAServiceTarget target)
		{
			if (target.IsAutoFailoverEnabled())
			{
				if (requestSource != HAServiceProtocol.RequestSource.RequestByUserForced)
				{
					errOut.WriteLine("Automatic failover is enabled for " + target + "\n" + "Refusing to manually manage HA state, since it may cause\n"
						 + "a split-brain scenario or other incorrect state.\n" + "If you are very sure you know what you are doing, please \n"
						 + "specify the --" + Forcemanual + " flag.");
					return false;
				}
				else
				{
					Log.Warn("Proceeding with manual HA state management even though\n" + "automatic failover is enabled for "
						 + target);
					return true;
				}
			}
			return true;
		}

		private HAServiceProtocol.StateChangeRequestInfo CreateReqInfo()
		{
			return new HAServiceProtocol.StateChangeRequestInfo(requestSource);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		private int Failover(CommandLine cmd)
		{
			bool forceFence = cmd.HasOption(Forcefence);
			bool forceActive = cmd.HasOption(Forceactive);
			int numOpts = cmd.GetOptions() == null ? 0 : cmd.GetOptions().Length;
			string[] args = cmd.GetArgs();
			if (numOpts > 3 || args.Length != 2)
			{
				errOut.WriteLine("failover: incorrect arguments");
				PrintUsage(errOut, "-failover");
				return -1;
			}
			HAServiceTarget fromNode = ResolveTarget(args[0]);
			HAServiceTarget toNode = ResolveTarget(args[1]);
			// Check that auto-failover is consistently configured for both nodes.
			Preconditions.CheckState(fromNode.IsAutoFailoverEnabled() == toNode.IsAutoFailoverEnabled
				(), "Inconsistent auto-failover configs between %s and %s!", fromNode, toNode);
			if (fromNode.IsAutoFailoverEnabled())
			{
				if (forceFence || forceActive)
				{
					// -forceActive doesn't make sense with auto-HA, since, if the node
					// is not healthy, then its ZKFC will immediately quit the election
					// again the next time a health check runs.
					//
					// -forceFence doesn't seem to have any real use cases with auto-HA
					// so it isn't implemented.
					errOut.WriteLine(Forcefence + " and " + Forceactive + " flags not " + "supported with auto-failover enabled."
						);
					return -1;
				}
				try
				{
					return GracefulFailoverThroughZKFCs(toNode);
				}
				catch (NotSupportedException e)
				{
					errOut.WriteLine("Failover command is not supported with " + "auto-failover enabled: "
						 + e.GetLocalizedMessage());
					return -1;
				}
			}
			FailoverController fc = new FailoverController(GetConf(), requestSource);
			try
			{
				fc.Failover(fromNode, toNode, forceFence, forceActive);
				@out.WriteLine("Failover from " + args[0] + " to " + args[1] + " successful");
			}
			catch (FailoverFailedException ffe)
			{
				errOut.WriteLine("Failover failed: " + ffe.GetLocalizedMessage());
				return -1;
			}
			return 0;
		}

		/// <summary>Initiate a graceful failover by talking to the target node's ZKFC.</summary>
		/// <remarks>
		/// Initiate a graceful failover by talking to the target node's ZKFC.
		/// This sends an RPC to the ZKFC, which coordinates the failover.
		/// </remarks>
		/// <param name="toNode">the node to fail to</param>
		/// <returns>status code (0 for success)</returns>
		/// <exception cref="System.IO.IOException">if failover does not succeed</exception>
		private int GracefulFailoverThroughZKFCs(HAServiceTarget toNode)
		{
			int timeout = FailoverController.GetRpcTimeoutToNewActive(GetConf());
			ZKFCProtocol proxy = toNode.GetZKFCProxy(GetConf(), timeout);
			try
			{
				proxy.GracefulFailover();
				@out.WriteLine("Failover to " + toNode + " successful");
			}
			catch (ServiceFailedException sfe)
			{
				errOut.WriteLine("Failover failed: " + sfe.GetLocalizedMessage());
				return -1;
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		private int CheckHealth(CommandLine cmd)
		{
			string[] argv = cmd.GetArgs();
			if (argv.Length != 1)
			{
				errOut.WriteLine("checkHealth: incorrect number of arguments");
				PrintUsage(errOut, "-checkHealth");
				return -1;
			}
			HAServiceProtocol proto = ResolveTarget(argv[0]).GetProxy(GetConf(), rpcTimeoutForChecks
				);
			try
			{
				HAServiceProtocolHelper.MonitorHealth(proto, CreateReqInfo());
			}
			catch (HealthCheckFailedException e)
			{
				errOut.WriteLine("Health check failed: " + e.GetLocalizedMessage());
				return -1;
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		private int GetServiceState(CommandLine cmd)
		{
			string[] argv = cmd.GetArgs();
			if (argv.Length != 1)
			{
				errOut.WriteLine("getServiceState: incorrect number of arguments");
				PrintUsage(errOut, "-getServiceState");
				return -1;
			}
			HAServiceProtocol proto = ResolveTarget(argv[0]).GetProxy(GetConf(), rpcTimeoutForChecks
				);
			@out.WriteLine(proto.GetServiceStatus().GetState());
			return 0;
		}

		/// <summary>
		/// Return the serviceId as is, we are assuming it was
		/// given as a service address of form <host:ipcport>.
		/// </summary>
		protected internal virtual string GetServiceAddr(string serviceId)
		{
			return serviceId;
		}

		public override void SetConf(Configuration conf)
		{
			base.SetConf(conf);
			if (conf != null)
			{
				rpcTimeoutForChecks = conf.GetInt(CommonConfigurationKeys.HaFcCliCheckTimeoutKey, 
					CommonConfigurationKeys.HaFcCliCheckTimeoutDefault);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] argv)
		{
			try
			{
				return RunCmd(argv);
			}
			catch (ArgumentException iae)
			{
				errOut.WriteLine("Illegal argument: " + iae.GetLocalizedMessage());
				return -1;
			}
			catch (IOException ioe)
			{
				errOut.WriteLine("Operation failed: " + ioe.GetLocalizedMessage());
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Operation failed", ioe);
				}
				return -1;
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual int RunCmd(string[] argv)
		{
			if (argv.Length < 1)
			{
				PrintUsage(errOut);
				return -1;
			}
			string cmd = argv[0];
			if (!cmd.StartsWith("-"))
			{
				errOut.WriteLine("Bad command '" + cmd + "': expected command starting with '-'");
				PrintUsage(errOut);
				return -1;
			}
			if (!Usage.Contains(cmd))
			{
				errOut.WriteLine(Runtime.Substring(cmd, 1) + ": Unknown command");
				PrintUsage(errOut);
				return -1;
			}
			Options opts = new Options();
			// Add command-specific options
			if ("-failover".Equals(cmd))
			{
				AddFailoverCliOpts(opts);
			}
			if ("-transitionToActive".Equals(cmd))
			{
				AddTransitionToActiveCliOpts(opts);
			}
			// Mutative commands take FORCEMANUAL option
			if ("-transitionToActive".Equals(cmd) || "-transitionToStandby".Equals(cmd) || "-failover"
				.Equals(cmd))
			{
				opts.AddOption(Forcemanual, false, "force manual control even if auto-failover is enabled"
					);
			}
			CommandLine cmdLine = ParseOpts(cmd, opts, argv);
			if (cmdLine == null)
			{
				// error already printed
				return -1;
			}
			if (cmdLine.HasOption(Forcemanual))
			{
				if (!ConfirmForceManual())
				{
					Log.Fatal("Aborted");
					return -1;
				}
				// Instruct the NNs to honor this request even if they're
				// configured for manual failover.
				requestSource = HAServiceProtocol.RequestSource.RequestByUserForced;
			}
			if ("-transitionToActive".Equals(cmd))
			{
				return TransitionToActive(cmdLine);
			}
			else
			{
				if ("-transitionToStandby".Equals(cmd))
				{
					return TransitionToStandby(cmdLine);
				}
				else
				{
					if ("-failover".Equals(cmd))
					{
						return Failover(cmdLine);
					}
					else
					{
						if ("-getServiceState".Equals(cmd))
						{
							return GetServiceState(cmdLine);
						}
						else
						{
							if ("-checkHealth".Equals(cmd))
							{
								return CheckHealth(cmdLine);
							}
							else
							{
								if ("-help".Equals(cmd))
								{
									return Help(argv);
								}
								else
								{
									// we already checked command validity above, so getting here
									// would be a coding error
									throw new Exception("Should not get here, command: " + cmd);
								}
							}
						}
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private bool ConfirmForceManual()
		{
			return ToolRunner.ConfirmPrompt("You have specified the --" + Forcemanual + " flag. This flag is "
				 + "dangerous, as it can induce a split-brain scenario that WILL " + "CORRUPT your HDFS namespace, possibly irrecoverably.\n"
				 + "\n" + "It is recommended not to use this flag, but instead to shut down the "
				 + "cluster and disable automatic failover if you prefer to manually " + "manage your HA state.\n"
				 + "\n" + "You may abort safely by answering 'n' or hitting ^C now.\n" + "\n" + 
				"Are you sure you want to continue?");
		}

		/// <summary>
		/// Add CLI options which are specific to the failover command and no
		/// others.
		/// </summary>
		private void AddFailoverCliOpts(Options failoverOpts)
		{
			failoverOpts.AddOption(Forcefence, false, "force fencing");
			failoverOpts.AddOption(Forceactive, false, "force failover");
		}

		// Don't add FORCEMANUAL, since that's added separately for all commands
		// that change state.
		/// <summary>
		/// Add CLI options which are specific to the transitionToActive command and
		/// no others.
		/// </summary>
		private void AddTransitionToActiveCliOpts(Options transitionToActiveCliOpts)
		{
			transitionToActiveCliOpts.AddOption(Forceactive, false, "force active");
		}

		private CommandLine ParseOpts(string cmdName, Options opts, string[] argv)
		{
			try
			{
				// Strip off the first arg, since that's just the command name
				argv = Arrays.CopyOfRange(argv, 1, argv.Length);
				return new GnuParser().Parse(opts, argv);
			}
			catch (ParseException)
			{
				errOut.WriteLine(Runtime.Substring(cmdName, 1) + ": incorrect arguments");
				PrintUsage(errOut, cmdName);
				return null;
			}
		}

		private int Help(string[] argv)
		{
			if (argv.Length == 1)
			{
				// only -help
				PrintUsage(@out);
				return 0;
			}
			else
			{
				if (argv.Length != 2)
				{
					PrintUsage(errOut, "-help");
					return -1;
				}
			}
			string cmd = argv[1];
			if (!cmd.StartsWith("-"))
			{
				cmd = "-" + cmd;
			}
			HAAdmin.UsageInfo usageInfo = Usage[cmd];
			if (usageInfo == null)
			{
				errOut.WriteLine(cmd + ": Unknown command");
				PrintUsage(errOut);
				return -1;
			}
			@out.WriteLine(cmd + " [" + usageInfo.args + "]: " + usageInfo.help);
			return 0;
		}

		protected internal class UsageInfo
		{
			public readonly string args;

			public readonly string help;

			public UsageInfo(string args, string help)
			{
				this.args = args;
				this.help = help;
			}
		}
	}
}
