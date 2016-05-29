using System;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Cli;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Logaggregation;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Cli
{
	public class LogsCLI : Configured, Tool
	{
		private const string ContainerIdOption = "containerId";

		private const string ApplicationIdOption = "applicationId";

		private const string NodeAddressOption = "nodeAddress";

		private const string AppOwnerOption = "appOwner";

		public const string HelpCmd = "help";

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Options opts = new Options();
			opts.AddOption(HelpCmd, false, "Displays help for all commands.");
			Option appIdOpt = new Option(ApplicationIdOption, true, "ApplicationId (required)"
				);
			appIdOpt.SetRequired(true);
			opts.AddOption(appIdOpt);
			opts.AddOption(ContainerIdOption, true, "ContainerId (must be specified if node address is specified)"
				);
			opts.AddOption(NodeAddressOption, true, "NodeAddress in the format " + "nodename:port (must be specified if container id is specified)"
				);
			opts.AddOption(AppOwnerOption, true, "AppOwner (assumed to be current user if not specified)"
				);
			opts.GetOption(ApplicationIdOption).SetArgName("Application ID");
			opts.GetOption(ContainerIdOption).SetArgName("Container ID");
			opts.GetOption(NodeAddressOption).SetArgName("Node Address");
			opts.GetOption(AppOwnerOption).SetArgName("Application Owner");
			Options printOpts = new Options();
			printOpts.AddOption(opts.GetOption(HelpCmd));
			printOpts.AddOption(opts.GetOption(ContainerIdOption));
			printOpts.AddOption(opts.GetOption(NodeAddressOption));
			printOpts.AddOption(opts.GetOption(AppOwnerOption));
			if (args.Length < 1)
			{
				PrintHelpMessage(printOpts);
				return -1;
			}
			if (args[0].Equals("-help"))
			{
				PrintHelpMessage(printOpts);
				return 0;
			}
			CommandLineParser parser = new GnuParser();
			string appIdStr = null;
			string containerIdStr = null;
			string nodeAddress = null;
			string appOwner = null;
			try
			{
				CommandLine commandLine = parser.Parse(opts, args, true);
				appIdStr = commandLine.GetOptionValue(ApplicationIdOption);
				containerIdStr = commandLine.GetOptionValue(ContainerIdOption);
				nodeAddress = commandLine.GetOptionValue(NodeAddressOption);
				appOwner = commandLine.GetOptionValue(AppOwnerOption);
			}
			catch (ParseException e)
			{
				System.Console.Error.WriteLine("options parsing failed: " + e.Message);
				PrintHelpMessage(printOpts);
				return -1;
			}
			if (appIdStr == null)
			{
				System.Console.Error.WriteLine("ApplicationId cannot be null!");
				PrintHelpMessage(printOpts);
				return -1;
			}
			ApplicationId appId = null;
			try
			{
				appId = ConverterUtils.ToApplicationId(appIdStr);
			}
			catch (Exception)
			{
				System.Console.Error.WriteLine("Invalid ApplicationId specified");
				return -1;
			}
			try
			{
				int resultCode = VerifyApplicationState(appId);
				if (resultCode != 0)
				{
					System.Console.Out.WriteLine("Logs are not avaiable right now.");
					return resultCode;
				}
			}
			catch (Exception)
			{
				System.Console.Error.WriteLine("Unable to get ApplicationState." + " Attempting to fetch logs directly from the filesystem."
					);
			}
			LogCLIHelpers logCliHelper = new LogCLIHelpers();
			logCliHelper.SetConf(GetConf());
			if (appOwner == null || appOwner.IsEmpty())
			{
				appOwner = UserGroupInformation.GetCurrentUser().GetShortUserName();
			}
			int resultCode_1 = 0;
			if (containerIdStr == null && nodeAddress == null)
			{
				resultCode_1 = logCliHelper.DumpAllContainersLogs(appId, appOwner, System.Console.Out
					);
			}
			else
			{
				if ((containerIdStr == null && nodeAddress != null) || (containerIdStr != null &&
					 nodeAddress == null))
				{
					System.Console.Out.WriteLine("ContainerId or NodeAddress cannot be null!");
					PrintHelpMessage(printOpts);
					resultCode_1 = -1;
				}
				else
				{
					resultCode_1 = logCliHelper.DumpAContainersLogs(appIdStr, containerIdStr, nodeAddress
						, appOwner);
				}
			}
			return resultCode_1;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private int VerifyApplicationState(ApplicationId appId)
		{
			YarnClient yarnClient = CreateYarnClient();
			try
			{
				ApplicationReport appReport = yarnClient.GetApplicationReport(appId);
				switch (appReport.GetYarnApplicationState())
				{
					case YarnApplicationState.New:
					case YarnApplicationState.NewSaving:
					case YarnApplicationState.Submitted:
					{
						return -1;
					}

					case YarnApplicationState.Accepted:
					case YarnApplicationState.Running:
					case YarnApplicationState.Failed:
					case YarnApplicationState.Finished:
					case YarnApplicationState.Killed:
					default:
					{
						break;
					}
				}
			}
			finally
			{
				yarnClient.Close();
			}
			return 0;
		}

		[VisibleForTesting]
		protected internal virtual YarnClient CreateYarnClient()
		{
			YarnClient yarnClient = YarnClient.CreateYarnClient();
			yarnClient.Init(GetConf());
			yarnClient.Start();
			return yarnClient;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			Configuration conf = new YarnConfiguration();
			LogsCLI logDumper = new LogsCLI();
			logDumper.SetConf(conf);
			int exitCode = logDumper.Run(args);
			System.Environment.Exit(exitCode);
		}

		private void PrintHelpMessage(Options options)
		{
			System.Console.Out.WriteLine("Retrieve logs for completed YARN applications.");
			HelpFormatter formatter = new HelpFormatter();
			formatter.PrintHelp("yarn logs -applicationId <application ID> [OPTIONS]", new Options
				());
			formatter.SetSyntaxPrefix(string.Empty);
			formatter.PrintHelp("general options are:", options);
		}
	}
}
