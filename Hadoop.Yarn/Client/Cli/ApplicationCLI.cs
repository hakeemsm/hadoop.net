using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Cli;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Cli
{
	public class ApplicationCLI : YarnCLI
	{
		private static readonly string ApplicationsPattern = "%30s\t%20s\t%20s\t%10s\t%10s\t%18s\t%18s\t%15s\t%35s"
			 + Runtime.GetProperty("line.separator");

		private static readonly string ApplicationAttemptsPattern = "%30s\t%20s\t%35s\t%35s"
			 + Runtime.GetProperty("line.separator");

		private static readonly string ContainerPattern = "%30s\t%20s\t%20s\t%20s\t%20s\t%20s\t%35s"
			 + Runtime.GetProperty("line.separator");

		private const string AppTypeCmd = "appTypes";

		private const string AppStateCmd = "appStates";

		private const string AllstatesOption = "ALL";

		private const string QueueCmd = "queue";

		public const string Application = "application";

		public const string ApplicationAttempt = "applicationattempt";

		public const string Container = "container";

		private bool allAppStates;

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			ApplicationCLI cli = new ApplicationCLI();
			cli.SetSysOutPrintStream(System.Console.Out);
			cli.SetSysErrPrintStream(System.Console.Error);
			int res = ToolRunner.Run(cli, args);
			cli.Stop();
			System.Environment.Exit(res);
		}

		/// <exception cref="System.Exception"/>
		public override int Run(string[] args)
		{
			Options opts = new Options();
			string title = null;
			if (args.Length > 0 && Sharpen.Runtime.EqualsIgnoreCase(args[0], Application))
			{
				title = Application;
				opts.AddOption(StatusCmd, true, "Prints the status of the application.");
				opts.AddOption(ListCmd, false, "List applications. " + "Supports optional use of -appTypes to filter applications "
					 + "based on application type, " + "and -appStates to filter applications based on application state."
					);
				opts.AddOption(KillCmd, true, "Kills the application.");
				opts.AddOption(MoveToQueueCmd, true, "Moves the application to a " + "different queue."
					);
				opts.AddOption(QueueCmd, true, "Works with the movetoqueue command to" + " specify which queue to move an application to."
					);
				opts.AddOption(HelpCmd, false, "Displays help for all commands.");
				Option appTypeOpt = new Option(AppTypeCmd, true, "Works with -list to " + "filter applications based on "
					 + "input comma-separated list of application types.");
				appTypeOpt.SetValueSeparator(',');
				appTypeOpt.SetArgs(Option.UnlimitedValues);
				appTypeOpt.SetArgName("Types");
				opts.AddOption(appTypeOpt);
				Option appStateOpt = new Option(AppStateCmd, true, "Works with -list " + "to filter applications based on input comma-separated list of "
					 + "application states. " + GetAllValidApplicationStates());
				appStateOpt.SetValueSeparator(',');
				appStateOpt.SetArgs(Option.UnlimitedValues);
				appStateOpt.SetArgName("States");
				opts.AddOption(appStateOpt);
				opts.GetOption(KillCmd).SetArgName("Application ID");
				opts.GetOption(MoveToQueueCmd).SetArgName("Application ID");
				opts.GetOption(QueueCmd).SetArgName("Queue Name");
				opts.GetOption(StatusCmd).SetArgName("Application ID");
			}
			else
			{
				if (args.Length > 0 && Sharpen.Runtime.EqualsIgnoreCase(args[0], ApplicationAttempt
					))
				{
					title = ApplicationAttempt;
					opts.AddOption(StatusCmd, true, "Prints the status of the application attempt.");
					opts.AddOption(ListCmd, true, "List application attempts for aplication.");
					opts.AddOption(HelpCmd, false, "Displays help for all commands.");
					opts.GetOption(StatusCmd).SetArgName("Application Attempt ID");
					opts.GetOption(ListCmd).SetArgName("Application ID");
				}
				else
				{
					if (args.Length > 0 && Sharpen.Runtime.EqualsIgnoreCase(args[0], Container))
					{
						title = Container;
						opts.AddOption(StatusCmd, true, "Prints the status of the container.");
						opts.AddOption(ListCmd, true, "List containers for application attempt.");
						opts.AddOption(HelpCmd, false, "Displays help for all commands.");
						opts.GetOption(StatusCmd).SetArgName("Container ID");
						opts.GetOption(ListCmd).SetArgName("Application Attempt ID");
					}
				}
			}
			int exitCode = -1;
			CommandLine cliParser = null;
			try
			{
				cliParser = new GnuParser().Parse(opts, args);
			}
			catch (MissingArgumentException)
			{
				sysout.WriteLine("Missing argument for options");
				PrintUsage(title, opts);
				return exitCode;
			}
			if (cliParser.HasOption(StatusCmd))
			{
				if (args.Length != 3)
				{
					PrintUsage(title, opts);
					return exitCode;
				}
				if (Sharpen.Runtime.EqualsIgnoreCase(args[0], Application))
				{
					exitCode = PrintApplicationReport(cliParser.GetOptionValue(StatusCmd));
				}
				else
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(args[0], ApplicationAttempt))
					{
						exitCode = PrintApplicationAttemptReport(cliParser.GetOptionValue(StatusCmd));
					}
					else
					{
						if (Sharpen.Runtime.EqualsIgnoreCase(args[0], Container))
						{
							exitCode = PrintContainerReport(cliParser.GetOptionValue(StatusCmd));
						}
					}
				}
				return exitCode;
			}
			else
			{
				if (cliParser.HasOption(ListCmd))
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(args[0], Application))
					{
						allAppStates = false;
						ICollection<string> appTypes = new HashSet<string>();
						if (cliParser.HasOption(AppTypeCmd))
						{
							string[] types = cliParser.GetOptionValues(AppTypeCmd);
							if (types != null)
							{
								foreach (string type in types)
								{
									if (!type.Trim().IsEmpty())
									{
										appTypes.AddItem(StringUtils.ToUpperCase(type).Trim());
									}
								}
							}
						}
						EnumSet<YarnApplicationState> appStates = EnumSet.NoneOf<YarnApplicationState>();
						if (cliParser.HasOption(AppStateCmd))
						{
							string[] states = cliParser.GetOptionValues(AppStateCmd);
							if (states != null)
							{
								foreach (string state in states)
								{
									if (!state.Trim().IsEmpty())
									{
										if (Sharpen.Runtime.EqualsIgnoreCase(state.Trim(), AllstatesOption))
										{
											allAppStates = true;
											break;
										}
										try
										{
											appStates.AddItem(YarnApplicationState.ValueOf(StringUtils.ToUpperCase(state).Trim
												()));
										}
										catch (ArgumentException)
										{
											sysout.WriteLine("The application state " + state + " is invalid.");
											sysout.WriteLine(GetAllValidApplicationStates());
											return exitCode;
										}
									}
								}
							}
						}
						ListApplications(appTypes, appStates);
					}
					else
					{
						if (Sharpen.Runtime.EqualsIgnoreCase(args[0], ApplicationAttempt))
						{
							if (args.Length != 3)
							{
								PrintUsage(title, opts);
								return exitCode;
							}
							ListApplicationAttempts(cliParser.GetOptionValue(ListCmd));
						}
						else
						{
							if (Sharpen.Runtime.EqualsIgnoreCase(args[0], Container))
							{
								if (args.Length != 3)
								{
									PrintUsage(title, opts);
									return exitCode;
								}
								ListContainers(cliParser.GetOptionValue(ListCmd));
							}
						}
					}
				}
				else
				{
					if (cliParser.HasOption(KillCmd))
					{
						if (args.Length != 3)
						{
							PrintUsage(title, opts);
							return exitCode;
						}
						try
						{
							KillApplication(cliParser.GetOptionValue(KillCmd));
						}
						catch (ApplicationNotFoundException)
						{
							return exitCode;
						}
					}
					else
					{
						if (cliParser.HasOption(MoveToQueueCmd))
						{
							if (!cliParser.HasOption(QueueCmd))
							{
								PrintUsage(title, opts);
								return exitCode;
							}
							MoveApplicationAcrossQueues(cliParser.GetOptionValue(MoveToQueueCmd), cliParser.GetOptionValue
								(QueueCmd));
						}
						else
						{
							if (cliParser.HasOption(HelpCmd))
							{
								PrintUsage(title, opts);
								return 0;
							}
							else
							{
								syserr.WriteLine("Invalid Command Usage : ");
								PrintUsage(title, opts);
							}
						}
					}
				}
			}
			return 0;
		}

		/// <summary>It prints the usage of the command</summary>
		/// <param name="opts"/>
		[VisibleForTesting]
		internal virtual void PrintUsage(string title, Options opts)
		{
			new HelpFormatter().PrintHelp(title, opts);
		}

		/// <summary>Prints the application attempt report for an application attempt id.</summary>
		/// <param name="applicationAttemptId"/>
		/// <returns>exitCode</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private int PrintApplicationAttemptReport(string applicationAttemptId)
		{
			ApplicationAttemptReport appAttemptReport = null;
			try
			{
				appAttemptReport = client.GetApplicationAttemptReport(ConverterUtils.ToApplicationAttemptId
					(applicationAttemptId));
			}
			catch (ApplicationNotFoundException)
			{
				sysout.WriteLine("Application for AppAttempt with id '" + applicationAttemptId + 
					"' doesn't exist in RM or Timeline Server.");
				return -1;
			}
			catch (ApplicationAttemptNotFoundException)
			{
				sysout.WriteLine("Application Attempt with id '" + applicationAttemptId + "' doesn't exist in RM or Timeline Server."
					);
				return -1;
			}
			// Use PrintWriter.println, which uses correct platform line ending.
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter appAttemptReportStr = new PrintWriter(new OutputStreamWriter(baos, Sharpen.Extensions.GetEncoding
				("UTF-8")));
			if (appAttemptReport != null)
			{
				appAttemptReportStr.WriteLine("Application Attempt Report : ");
				appAttemptReportStr.Write("\tApplicationAttempt-Id : ");
				appAttemptReportStr.WriteLine(appAttemptReport.GetApplicationAttemptId());
				appAttemptReportStr.Write("\tState : ");
				appAttemptReportStr.WriteLine(appAttemptReport.GetYarnApplicationAttemptState());
				appAttemptReportStr.Write("\tAMContainer : ");
				appAttemptReportStr.WriteLine(appAttemptReport.GetAMContainerId().ToString());
				appAttemptReportStr.Write("\tTracking-URL : ");
				appAttemptReportStr.WriteLine(appAttemptReport.GetTrackingUrl());
				appAttemptReportStr.Write("\tRPC Port : ");
				appAttemptReportStr.WriteLine(appAttemptReport.GetRpcPort());
				appAttemptReportStr.Write("\tAM Host : ");
				appAttemptReportStr.WriteLine(appAttemptReport.GetHost());
				appAttemptReportStr.Write("\tDiagnostics : ");
				appAttemptReportStr.Write(appAttemptReport.GetDiagnostics());
			}
			else
			{
				appAttemptReportStr.Write("Application Attempt with id '" + applicationAttemptId 
					+ "' doesn't exist in Timeline Server.");
				appAttemptReportStr.Close();
				sysout.WriteLine(baos.ToString("UTF-8"));
				return -1;
			}
			appAttemptReportStr.Close();
			sysout.WriteLine(baos.ToString("UTF-8"));
			return 0;
		}

		/// <summary>Prints the container report for an container id.</summary>
		/// <param name="containerId"/>
		/// <returns>exitCode</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private int PrintContainerReport(string containerId)
		{
			ContainerReport containerReport = null;
			try
			{
				containerReport = client.GetContainerReport((ConverterUtils.ToContainerId(containerId
					)));
			}
			catch (ApplicationNotFoundException)
			{
				sysout.WriteLine("Application for Container with id '" + containerId + "' doesn't exist in RM or Timeline Server."
					);
				return -1;
			}
			catch (ApplicationAttemptNotFoundException)
			{
				sysout.WriteLine("Application Attempt for Container with id '" + containerId + "' doesn't exist in RM or Timeline Server."
					);
				return -1;
			}
			catch (ContainerNotFoundException)
			{
				sysout.WriteLine("Container with id '" + containerId + "' doesn't exist in RM or Timeline Server."
					);
				return -1;
			}
			// Use PrintWriter.println, which uses correct platform line ending.
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter containerReportStr = new PrintWriter(new OutputStreamWriter(baos, Sharpen.Extensions.GetEncoding
				("UTF-8")));
			if (containerReport != null)
			{
				containerReportStr.WriteLine("Container Report : ");
				containerReportStr.Write("\tContainer-Id : ");
				containerReportStr.WriteLine(containerReport.GetContainerId());
				containerReportStr.Write("\tStart-Time : ");
				containerReportStr.WriteLine(containerReport.GetCreationTime());
				containerReportStr.Write("\tFinish-Time : ");
				containerReportStr.WriteLine(containerReport.GetFinishTime());
				containerReportStr.Write("\tState : ");
				containerReportStr.WriteLine(containerReport.GetContainerState());
				containerReportStr.Write("\tLOG-URL : ");
				containerReportStr.WriteLine(containerReport.GetLogUrl());
				containerReportStr.Write("\tHost : ");
				containerReportStr.WriteLine(containerReport.GetAssignedNode());
				containerReportStr.Write("\tNodeHttpAddress : ");
				containerReportStr.WriteLine(containerReport.GetNodeHttpAddress() == null ? "N/A"
					 : containerReport.GetNodeHttpAddress());
				containerReportStr.Write("\tDiagnostics : ");
				containerReportStr.Write(containerReport.GetDiagnosticsInfo());
			}
			else
			{
				containerReportStr.Write("Container with id '" + containerId + "' doesn't exist in Timeline Server."
					);
				containerReportStr.Close();
				sysout.WriteLine(baos.ToString("UTF-8"));
				return -1;
			}
			containerReportStr.Close();
			sysout.WriteLine(baos.ToString("UTF-8"));
			return 0;
		}

		/// <summary>
		/// Lists the applications matching the given application Types And application
		/// States present in the Resource Manager
		/// </summary>
		/// <param name="appTypes"/>
		/// <param name="appStates"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void ListApplications(ICollection<string> appTypes, EnumSet<YarnApplicationState
			> appStates)
		{
			PrintWriter writer = new PrintWriter(new OutputStreamWriter(sysout, Sharpen.Extensions.GetEncoding
				("UTF-8")));
			if (allAppStates)
			{
				foreach (YarnApplicationState appState in YarnApplicationState.Values())
				{
					appStates.AddItem(appState);
				}
			}
			else
			{
				if (appStates.IsEmpty())
				{
					appStates.AddItem(YarnApplicationState.Running);
					appStates.AddItem(YarnApplicationState.Accepted);
					appStates.AddItem(YarnApplicationState.Submitted);
				}
			}
			IList<ApplicationReport> appsReport = client.GetApplications(appTypes, appStates);
			writer.WriteLine("Total number of applications (application-types: " + appTypes +
				 " and states: " + appStates + ")" + ":" + appsReport.Count);
			writer.Printf(ApplicationsPattern, "Application-Id", "Application-Name", "Application-Type"
				, "User", "Queue", "State", "Final-State", "Progress", "Tracking-URL");
			foreach (ApplicationReport appReport in appsReport)
			{
				DecimalFormat formatter = new DecimalFormat("###.##%");
				string progress = formatter.Format(appReport.GetProgress());
				writer.Printf(ApplicationsPattern, appReport.GetApplicationId(), appReport.GetName
					(), appReport.GetApplicationType(), appReport.GetUser(), appReport.GetQueue(), appReport
					.GetYarnApplicationState(), appReport.GetFinalApplicationStatus(), progress, appReport
					.GetOriginalTrackingUrl());
			}
			writer.Flush();
		}

		/// <summary>Kills the application with the application id as appId</summary>
		/// <param name="applicationId"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void KillApplication(string applicationId)
		{
			ApplicationId appId = ConverterUtils.ToApplicationId(applicationId);
			ApplicationReport appReport = null;
			try
			{
				appReport = client.GetApplicationReport(appId);
			}
			catch (ApplicationNotFoundException e)
			{
				sysout.WriteLine("Application with id '" + applicationId + "' doesn't exist in RM."
					);
				throw;
			}
			if (appReport.GetYarnApplicationState() == YarnApplicationState.Finished || appReport
				.GetYarnApplicationState() == YarnApplicationState.Killed || appReport.GetYarnApplicationState
				() == YarnApplicationState.Failed)
			{
				sysout.WriteLine("Application " + applicationId + " has already finished ");
			}
			else
			{
				sysout.WriteLine("Killing application " + applicationId);
				client.KillApplication(appId);
			}
		}

		/// <summary>Moves the application with the given ID to the given queue.</summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void MoveApplicationAcrossQueues(string applicationId, string queue)
		{
			ApplicationId appId = ConverterUtils.ToApplicationId(applicationId);
			ApplicationReport appReport = client.GetApplicationReport(appId);
			if (appReport.GetYarnApplicationState() == YarnApplicationState.Finished || appReport
				.GetYarnApplicationState() == YarnApplicationState.Killed || appReport.GetYarnApplicationState
				() == YarnApplicationState.Failed)
			{
				sysout.WriteLine("Application " + applicationId + " has already finished ");
			}
			else
			{
				sysout.WriteLine("Moving application " + applicationId + " to queue " + queue);
				client.MoveApplicationAcrossQueues(appId, queue);
				sysout.WriteLine("Successfully completed move.");
			}
		}

		/// <summary>Prints the application report for an application id.</summary>
		/// <param name="applicationId"/>
		/// <returns>exitCode</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private int PrintApplicationReport(string applicationId)
		{
			ApplicationReport appReport = null;
			try
			{
				appReport = client.GetApplicationReport(ConverterUtils.ToApplicationId(applicationId
					));
			}
			catch (ApplicationNotFoundException)
			{
				sysout.WriteLine("Application with id '" + applicationId + "' doesn't exist in RM or Timeline Server."
					);
				return -1;
			}
			// Use PrintWriter.println, which uses correct platform line ending.
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter appReportStr = new PrintWriter(new OutputStreamWriter(baos, Sharpen.Extensions.GetEncoding
				("UTF-8")));
			if (appReport != null)
			{
				appReportStr.WriteLine("Application Report : ");
				appReportStr.Write("\tApplication-Id : ");
				appReportStr.WriteLine(appReport.GetApplicationId());
				appReportStr.Write("\tApplication-Name : ");
				appReportStr.WriteLine(appReport.GetName());
				appReportStr.Write("\tApplication-Type : ");
				appReportStr.WriteLine(appReport.GetApplicationType());
				appReportStr.Write("\tUser : ");
				appReportStr.WriteLine(appReport.GetUser());
				appReportStr.Write("\tQueue : ");
				appReportStr.WriteLine(appReport.GetQueue());
				appReportStr.Write("\tStart-Time : ");
				appReportStr.WriteLine(appReport.GetStartTime());
				appReportStr.Write("\tFinish-Time : ");
				appReportStr.WriteLine(appReport.GetFinishTime());
				appReportStr.Write("\tProgress : ");
				DecimalFormat formatter = new DecimalFormat("###.##%");
				string progress = formatter.Format(appReport.GetProgress());
				appReportStr.WriteLine(progress);
				appReportStr.Write("\tState : ");
				appReportStr.WriteLine(appReport.GetYarnApplicationState());
				appReportStr.Write("\tFinal-State : ");
				appReportStr.WriteLine(appReport.GetFinalApplicationStatus());
				appReportStr.Write("\tTracking-URL : ");
				appReportStr.WriteLine(appReport.GetOriginalTrackingUrl());
				appReportStr.Write("\tRPC Port : ");
				appReportStr.WriteLine(appReport.GetRpcPort());
				appReportStr.Write("\tAM Host : ");
				appReportStr.WriteLine(appReport.GetHost());
				appReportStr.Write("\tAggregate Resource Allocation : ");
				ApplicationResourceUsageReport usageReport = appReport.GetApplicationResourceUsageReport
					();
				if (usageReport != null)
				{
					//completed app report in the timeline server doesn't have usage report
					appReportStr.Write(usageReport.GetMemorySeconds() + " MB-seconds, ");
					appReportStr.WriteLine(usageReport.GetVcoreSeconds() + " vcore-seconds");
				}
				else
				{
					appReportStr.WriteLine("N/A");
				}
				appReportStr.Write("\tDiagnostics : ");
				appReportStr.Write(appReport.GetDiagnostics());
			}
			else
			{
				appReportStr.Write("Application with id '" + applicationId + "' doesn't exist in RM."
					);
				appReportStr.Close();
				sysout.WriteLine(baos.ToString("UTF-8"));
				return -1;
			}
			appReportStr.Close();
			sysout.WriteLine(baos.ToString("UTF-8"));
			return 0;
		}

		private string GetAllValidApplicationStates()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("The valid application state can be" + " one of the following: ");
			sb.Append(AllstatesOption + ",");
			foreach (YarnApplicationState appState in YarnApplicationState.Values())
			{
				sb.Append(appState + ",");
			}
			string output = sb.ToString();
			return Sharpen.Runtime.Substring(output, 0, output.Length - 1);
		}

		/// <summary>Lists the application attempts matching the given applicationid</summary>
		/// <param name="applicationId"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void ListApplicationAttempts(string applicationId)
		{
			PrintWriter writer = new PrintWriter(new OutputStreamWriter(sysout, Sharpen.Extensions.GetEncoding
				("UTF-8")));
			IList<ApplicationAttemptReport> appAttemptsReport = client.GetApplicationAttempts
				(ConverterUtils.ToApplicationId(applicationId));
			writer.WriteLine("Total number of application attempts " + ":" + appAttemptsReport
				.Count);
			writer.Printf(ApplicationAttemptsPattern, "ApplicationAttempt-Id", "State", "AM-Container-Id"
				, "Tracking-URL");
			foreach (ApplicationAttemptReport appAttemptReport in appAttemptsReport)
			{
				writer.Printf(ApplicationAttemptsPattern, appAttemptReport.GetApplicationAttemptId
					(), appAttemptReport.GetYarnApplicationAttemptState(), appAttemptReport.GetAMContainerId
					().ToString(), appAttemptReport.GetTrackingUrl());
			}
			writer.Flush();
		}

		/// <summary>Lists the containers matching the given application attempts</summary>
		/// <param name="appAttemptId"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void ListContainers(string appAttemptId)
		{
			PrintWriter writer = new PrintWriter(new OutputStreamWriter(sysout, Sharpen.Extensions.GetEncoding
				("UTF-8")));
			IList<ContainerReport> appsReport = client.GetContainers(ConverterUtils.ToApplicationAttemptId
				(appAttemptId));
			writer.WriteLine("Total number of containers " + ":" + appsReport.Count);
			writer.Printf(ContainerPattern, "Container-Id", "Start Time", "Finish Time", "State"
				, "Host", "Node Http Address", "LOG-URL");
			foreach (ContainerReport containerReport in appsReport)
			{
				writer.Printf(ContainerPattern, containerReport.GetContainerId(), Times.Format(containerReport
					.GetCreationTime()), Times.Format(containerReport.GetFinishTime()), containerReport
					.GetContainerState(), containerReport.GetAssignedNode(), containerReport.GetNodeHttpAddress
					() == null ? "N/A" : containerReport.GetNodeHttpAddress(), containerReport.GetLogUrl
					());
			}
			writer.Flush();
		}
	}
}
