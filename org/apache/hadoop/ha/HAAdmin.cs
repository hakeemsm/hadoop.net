using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>A command-line tool for making calls in the HAServiceProtocol.</summary>
	/// <remarks>
	/// A command-line tool for making calls in the HAServiceProtocol.
	/// For example,. this can be used to force a service to standby or active
	/// mode, or to trigger a health-check.
	/// </remarks>
	public abstract class HAAdmin : org.apache.hadoop.conf.Configured, org.apache.hadoop.util.Tool
	{
		private const string FORCEFENCE = "forcefence";

		private const string FORCEACTIVE = "forceactive";

		/// <summary>
		/// Undocumented flag which allows an administrator to use manual failover
		/// state transitions even when auto-failover is enabled.
		/// </summary>
		/// <remarks>
		/// Undocumented flag which allows an administrator to use manual failover
		/// state transitions even when auto-failover is enabled. This is an unsafe
		/// operation, which is why it is not documented in the usage below.
		/// </remarks>
		private const string FORCEMANUAL = "forcemanual";

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.HAAdmin)));

		private int rpcTimeoutForChecks = -1;

		protected internal static readonly System.Collections.Generic.IDictionary<string, 
			org.apache.hadoop.ha.HAAdmin.UsageInfo> USAGE = com.google.common.collect.ImmutableMap
			.builder<string, org.apache.hadoop.ha.HAAdmin.UsageInfo>().put("-transitionToActive"
			, new org.apache.hadoop.ha.HAAdmin.UsageInfo("[--" + FORCEACTIVE + "] <serviceId>"
			, "Transitions the service into Active state")).put("-transitionToStandby", new 
			org.apache.hadoop.ha.HAAdmin.UsageInfo("<serviceId>", "Transitions the service into Standby state"
			)).put("-failover", new org.apache.hadoop.ha.HAAdmin.UsageInfo("[--" + FORCEFENCE
			 + "] [--" + FORCEACTIVE + "] <serviceId> <serviceId>", "Failover from the first service to the second.\n"
			 + "Unconditionally fence services if the --" + FORCEFENCE + " option is used.\n"
			 + "Try to failover to the target service even if it is not ready if the " + "--"
			 + FORCEACTIVE + " option is used.")).put("-getServiceState", new org.apache.hadoop.ha.HAAdmin.UsageInfo
			("<serviceId>", "Returns the state of the service")).put("-checkHealth", new org.apache.hadoop.ha.HAAdmin.UsageInfo
			("<serviceId>", "Requests that the service perform a health check.\n" + "The HAAdmin tool will exit with a non-zero exit code\n"
			 + "if the check fails.")).put("-help", new org.apache.hadoop.ha.HAAdmin.UsageInfo
			("<command>", "Displays help on the specified command")).build();

		/// <summary>Output stream for errors, for use in tests</summary>
		protected internal System.IO.TextWriter errOut = System.Console.Error;

		protected internal System.IO.TextWriter @out = System.Console.Out;

		private org.apache.hadoop.ha.HAServiceProtocol.RequestSource requestSource = org.apache.hadoop.ha.HAServiceProtocol.RequestSource
			.REQUEST_BY_USER;

		protected internal HAAdmin()
			: base()
		{
		}

		protected internal HAAdmin(org.apache.hadoop.conf.Configuration conf)
			: base(conf)
		{
		}

		protected internal abstract org.apache.hadoop.ha.HAServiceTarget resolveTarget(string
			 @string);

		protected internal virtual System.Collections.Generic.ICollection<string> getTargetIds
			(string targetNodeToActivate)
		{
			return new System.Collections.Generic.List<string>(java.util.Arrays.asList(new string
				[] { targetNodeToActivate }));
		}

		protected internal virtual string getUsageString()
		{
			return "Usage: HAAdmin";
		}

		protected internal virtual void printUsage(System.IO.TextWriter errOut)
		{
			errOut.WriteLine(getUsageString());
			foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.ha.HAAdmin.UsageInfo
				> e in USAGE)
			{
				string cmd = e.Key;
				org.apache.hadoop.ha.HAAdmin.UsageInfo usage = e.Value;
				errOut.WriteLine("    [" + cmd + " " + usage.args + "]");
			}
			errOut.WriteLine();
			org.apache.hadoop.util.ToolRunner.printGenericCommandUsage(errOut);
		}

		private void printUsage(System.IO.TextWriter errOut, string cmd)
		{
			org.apache.hadoop.ha.HAAdmin.UsageInfo usage = USAGE[cmd];
			if (usage == null)
			{
				throw new System.Exception("No usage for cmd " + cmd);
			}
			errOut.WriteLine(getUsageString() + " [" + cmd + " " + usage.args + "]");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
		private int transitionToActive(org.apache.commons.cli.CommandLine cmd)
		{
			string[] argv = cmd.getArgs();
			if (argv.Length != 1)
			{
				errOut.WriteLine("transitionToActive: incorrect number of arguments");
				printUsage(errOut, "-transitionToActive");
				return -1;
			}
			/*  returns true if other target node is active or some exception occurred
			and forceActive was not set  */
			if (!cmd.hasOption(FORCEACTIVE))
			{
				if (isOtherTargetNodeActive(argv[0], cmd.hasOption(FORCEACTIVE)))
				{
					return -1;
				}
			}
			org.apache.hadoop.ha.HAServiceTarget target = resolveTarget(argv[0]);
			if (!checkManualStateManagementOK(target))
			{
				return -1;
			}
			org.apache.hadoop.ha.HAServiceProtocol proto = target.getProxy(getConf(), 0);
			org.apache.hadoop.ha.HAServiceProtocolHelper.transitionToActive(proto, createReqInfo
				());
			return 0;
		}

		/// <summary>Checks whether other target node is active or not</summary>
		/// <param name="targetNodeToActivate"/>
		/// <returns>
		/// true if other target node is active or some other exception
		/// occurred and forceActive was set otherwise false
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		private bool isOtherTargetNodeActive(string targetNodeToActivate, bool forceActive
			)
		{
			System.Collections.Generic.ICollection<string> targetIds = getTargetIds(targetNodeToActivate
				);
			targetIds.remove(targetNodeToActivate);
			foreach (string targetId in targetIds)
			{
				org.apache.hadoop.ha.HAServiceTarget target = resolveTarget(targetId);
				if (!checkManualStateManagementOK(target))
				{
					return true;
				}
				try
				{
					org.apache.hadoop.ha.HAServiceProtocol proto = target.getProxy(getConf(), 5000);
					if (proto.getServiceStatus().getState() == org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
						.ACTIVE)
					{
						errOut.WriteLine("transitionToActive: Node " + targetId + " is already active");
						printUsage(errOut, "-transitionToActive");
						return true;
					}
				}
				catch (System.Exception e)
				{
					//If forceActive switch is false then return true
					if (!forceActive)
					{
						errOut.WriteLine("Unexpected error occurred  " + e.Message);
						printUsage(errOut, "-transitionToActive");
						return true;
					}
				}
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
		private int transitionToStandby(org.apache.commons.cli.CommandLine cmd)
		{
			string[] argv = cmd.getArgs();
			if (argv.Length != 1)
			{
				errOut.WriteLine("transitionToStandby: incorrect number of arguments");
				printUsage(errOut, "-transitionToStandby");
				return -1;
			}
			org.apache.hadoop.ha.HAServiceTarget target = resolveTarget(argv[0]);
			if (!checkManualStateManagementOK(target))
			{
				return -1;
			}
			org.apache.hadoop.ha.HAServiceProtocol proto = target.getProxy(getConf(), 0);
			org.apache.hadoop.ha.HAServiceProtocolHelper.transitionToStandby(proto, createReqInfo
				());
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
		private bool checkManualStateManagementOK(org.apache.hadoop.ha.HAServiceTarget target
			)
		{
			if (target.isAutoFailoverEnabled())
			{
				if (requestSource != org.apache.hadoop.ha.HAServiceProtocol.RequestSource.REQUEST_BY_USER_FORCED)
				{
					errOut.WriteLine("Automatic failover is enabled for " + target + "\n" + "Refusing to manually manage HA state, since it may cause\n"
						 + "a split-brain scenario or other incorrect state.\n" + "If you are very sure you know what you are doing, please \n"
						 + "specify the --" + FORCEMANUAL + " flag.");
					return false;
				}
				else
				{
					LOG.warn("Proceeding with manual HA state management even though\n" + "automatic failover is enabled for "
						 + target);
					return true;
				}
			}
			return true;
		}

		private org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo createReqInfo
			()
		{
			return new org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo(requestSource
				);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
		private int failover(org.apache.commons.cli.CommandLine cmd)
		{
			bool forceFence = cmd.hasOption(FORCEFENCE);
			bool forceActive = cmd.hasOption(FORCEACTIVE);
			int numOpts = cmd.getOptions() == null ? 0 : cmd.getOptions().Length;
			string[] args = cmd.getArgs();
			if (numOpts > 3 || args.Length != 2)
			{
				errOut.WriteLine("failover: incorrect arguments");
				printUsage(errOut, "-failover");
				return -1;
			}
			org.apache.hadoop.ha.HAServiceTarget fromNode = resolveTarget(args[0]);
			org.apache.hadoop.ha.HAServiceTarget toNode = resolveTarget(args[1]);
			// Check that auto-failover is consistently configured for both nodes.
			com.google.common.@base.Preconditions.checkState(fromNode.isAutoFailoverEnabled()
				 == toNode.isAutoFailoverEnabled(), "Inconsistent auto-failover configs between %s and %s!"
				, fromNode, toNode);
			if (fromNode.isAutoFailoverEnabled())
			{
				if (forceFence || forceActive)
				{
					// -forceActive doesn't make sense with auto-HA, since, if the node
					// is not healthy, then its ZKFC will immediately quit the election
					// again the next time a health check runs.
					//
					// -forceFence doesn't seem to have any real use cases with auto-HA
					// so it isn't implemented.
					errOut.WriteLine(FORCEFENCE + " and " + FORCEACTIVE + " flags not " + "supported with auto-failover enabled."
						);
					return -1;
				}
				try
				{
					return gracefulFailoverThroughZKFCs(toNode);
				}
				catch (System.NotSupportedException e)
				{
					errOut.WriteLine("Failover command is not supported with " + "auto-failover enabled: "
						 + e.getLocalizedMessage());
					return -1;
				}
			}
			org.apache.hadoop.ha.FailoverController fc = new org.apache.hadoop.ha.FailoverController
				(getConf(), requestSource);
			try
			{
				fc.failover(fromNode, toNode, forceFence, forceActive);
				@out.WriteLine("Failover from " + args[0] + " to " + args[1] + " successful");
			}
			catch (org.apache.hadoop.ha.FailoverFailedException ffe)
			{
				errOut.WriteLine("Failover failed: " + ffe.getLocalizedMessage());
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
		private int gracefulFailoverThroughZKFCs(org.apache.hadoop.ha.HAServiceTarget toNode
			)
		{
			int timeout = org.apache.hadoop.ha.FailoverController.getRpcTimeoutToNewActive(getConf
				());
			org.apache.hadoop.ha.ZKFCProtocol proxy = toNode.getZKFCProxy(getConf(), timeout);
			try
			{
				proxy.gracefulFailover();
				@out.WriteLine("Failover to " + toNode + " successful");
			}
			catch (org.apache.hadoop.ha.ServiceFailedException sfe)
			{
				errOut.WriteLine("Failover failed: " + sfe.getLocalizedMessage());
				return -1;
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
		private int checkHealth(org.apache.commons.cli.CommandLine cmd)
		{
			string[] argv = cmd.getArgs();
			if (argv.Length != 1)
			{
				errOut.WriteLine("checkHealth: incorrect number of arguments");
				printUsage(errOut, "-checkHealth");
				return -1;
			}
			org.apache.hadoop.ha.HAServiceProtocol proto = resolveTarget(argv[0]).getProxy(getConf
				(), rpcTimeoutForChecks);
			try
			{
				org.apache.hadoop.ha.HAServiceProtocolHelper.monitorHealth(proto, createReqInfo()
					);
			}
			catch (org.apache.hadoop.ha.HealthCheckFailedException e)
			{
				errOut.WriteLine("Health check failed: " + e.getLocalizedMessage());
				return -1;
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
		private int getServiceState(org.apache.commons.cli.CommandLine cmd)
		{
			string[] argv = cmd.getArgs();
			if (argv.Length != 1)
			{
				errOut.WriteLine("getServiceState: incorrect number of arguments");
				printUsage(errOut, "-getServiceState");
				return -1;
			}
			org.apache.hadoop.ha.HAServiceProtocol proto = resolveTarget(argv[0]).getProxy(getConf
				(), rpcTimeoutForChecks);
			@out.WriteLine(proto.getServiceStatus().getState());
			return 0;
		}

		/// <summary>
		/// Return the serviceId as is, we are assuming it was
		/// given as a service address of form <host:ipcport>.
		/// </summary>
		protected internal virtual string getServiceAddr(string serviceId)
		{
			return serviceId;
		}

		public override void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			base.setConf(conf);
			if (conf != null)
			{
				rpcTimeoutForChecks = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY
					, org.apache.hadoop.fs.CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual int run(string[] argv)
		{
			try
			{
				return runCmd(argv);
			}
			catch (System.ArgumentException iae)
			{
				errOut.WriteLine("Illegal argument: " + iae.getLocalizedMessage());
				return -1;
			}
			catch (System.IO.IOException ioe)
			{
				errOut.WriteLine("Operation failed: " + ioe.getLocalizedMessage());
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Operation failed", ioe);
				}
				return -1;
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual int runCmd(string[] argv)
		{
			if (argv.Length < 1)
			{
				printUsage(errOut);
				return -1;
			}
			string cmd = argv[0];
			if (!cmd.StartsWith("-"))
			{
				errOut.WriteLine("Bad command '" + cmd + "': expected command starting with '-'");
				printUsage(errOut);
				return -1;
			}
			if (!USAGE.Contains(cmd))
			{
				errOut.WriteLine(Sharpen.Runtime.substring(cmd, 1) + ": Unknown command");
				printUsage(errOut);
				return -1;
			}
			org.apache.commons.cli.Options opts = new org.apache.commons.cli.Options();
			// Add command-specific options
			if ("-failover".Equals(cmd))
			{
				addFailoverCliOpts(opts);
			}
			if ("-transitionToActive".Equals(cmd))
			{
				addTransitionToActiveCliOpts(opts);
			}
			// Mutative commands take FORCEMANUAL option
			if ("-transitionToActive".Equals(cmd) || "-transitionToStandby".Equals(cmd) || "-failover"
				.Equals(cmd))
			{
				opts.addOption(FORCEMANUAL, false, "force manual control even if auto-failover is enabled"
					);
			}
			org.apache.commons.cli.CommandLine cmdLine = parseOpts(cmd, opts, argv);
			if (cmdLine == null)
			{
				// error already printed
				return -1;
			}
			if (cmdLine.hasOption(FORCEMANUAL))
			{
				if (!confirmForceManual())
				{
					LOG.fatal("Aborted");
					return -1;
				}
				// Instruct the NNs to honor this request even if they're
				// configured for manual failover.
				requestSource = org.apache.hadoop.ha.HAServiceProtocol.RequestSource.REQUEST_BY_USER_FORCED;
			}
			if ("-transitionToActive".Equals(cmd))
			{
				return transitionToActive(cmdLine);
			}
			else
			{
				if ("-transitionToStandby".Equals(cmd))
				{
					return transitionToStandby(cmdLine);
				}
				else
				{
					if ("-failover".Equals(cmd))
					{
						return failover(cmdLine);
					}
					else
					{
						if ("-getServiceState".Equals(cmd))
						{
							return getServiceState(cmdLine);
						}
						else
						{
							if ("-checkHealth".Equals(cmd))
							{
								return checkHealth(cmdLine);
							}
							else
							{
								if ("-help".Equals(cmd))
								{
									return help(argv);
								}
								else
								{
									// we already checked command validity above, so getting here
									// would be a coding error
									throw new java.lang.AssertionError("Should not get here, command: " + cmd);
								}
							}
						}
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private bool confirmForceManual()
		{
			return org.apache.hadoop.util.ToolRunner.confirmPrompt("You have specified the --"
				 + FORCEMANUAL + " flag. This flag is " + "dangerous, as it can induce a split-brain scenario that WILL "
				 + "CORRUPT your HDFS namespace, possibly irrecoverably.\n" + "\n" + "It is recommended not to use this flag, but instead to shut down the "
				 + "cluster and disable automatic failover if you prefer to manually " + "manage your HA state.\n"
				 + "\n" + "You may abort safely by answering 'n' or hitting ^C now.\n" + "\n" + 
				"Are you sure you want to continue?");
		}

		/// <summary>
		/// Add CLI options which are specific to the failover command and no
		/// others.
		/// </summary>
		private void addFailoverCliOpts(org.apache.commons.cli.Options failoverOpts)
		{
			failoverOpts.addOption(FORCEFENCE, false, "force fencing");
			failoverOpts.addOption(FORCEACTIVE, false, "force failover");
		}

		// Don't add FORCEMANUAL, since that's added separately for all commands
		// that change state.
		/// <summary>
		/// Add CLI options which are specific to the transitionToActive command and
		/// no others.
		/// </summary>
		private void addTransitionToActiveCliOpts(org.apache.commons.cli.Options transitionToActiveCliOpts
			)
		{
			transitionToActiveCliOpts.addOption(FORCEACTIVE, false, "force active");
		}

		private org.apache.commons.cli.CommandLine parseOpts(string cmdName, org.apache.commons.cli.Options
			 opts, string[] argv)
		{
			try
			{
				// Strip off the first arg, since that's just the command name
				argv = java.util.Arrays.copyOfRange(argv, 1, argv.Length);
				return new org.apache.commons.cli.GnuParser().parse(opts, argv);
			}
			catch (org.apache.commons.cli.ParseException)
			{
				errOut.WriteLine(Sharpen.Runtime.substring(cmdName, 1) + ": incorrect arguments");
				printUsage(errOut, cmdName);
				return null;
			}
		}

		private int help(string[] argv)
		{
			if (argv.Length == 1)
			{
				// only -help
				printUsage(@out);
				return 0;
			}
			else
			{
				if (argv.Length != 2)
				{
					printUsage(errOut, "-help");
					return -1;
				}
			}
			string cmd = argv[1];
			if (!cmd.StartsWith("-"))
			{
				cmd = "-" + cmd;
			}
			org.apache.hadoop.ha.HAAdmin.UsageInfo usageInfo = USAGE[cmd];
			if (usageInfo == null)
			{
				errOut.WriteLine(cmd + ": Unknown command");
				printUsage(errOut);
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
