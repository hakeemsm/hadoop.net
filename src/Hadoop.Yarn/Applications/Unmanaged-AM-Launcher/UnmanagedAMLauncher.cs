using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Applications.Unmanagedamlauncher
{
	/// <summary>The UnmanagedLauncher is a simple client that launches and unmanaged AM.
	/// 	</summary>
	/// <remarks>
	/// The UnmanagedLauncher is a simple client that launches and unmanaged AM. An
	/// unmanagedAM is an AM that is not launched and managed by the RM. The client
	/// creates a new application on the RM and negotiates a new attempt id. Then it
	/// waits for the RM app state to reach be YarnApplicationState.ACCEPTED after
	/// which it spawns the AM in another process and passes it the container id via
	/// env variable Environment.CONTAINER_ID. The AM can be in any
	/// language. The AM can register with the RM using the attempt id obtained
	/// from the container id and proceed as normal.
	/// The client redirects app stdout and stderr to its own stdout and
	/// stderr and waits for the AM process to exit. Then it waits for the RM to
	/// report app completion.
	/// </remarks>
	public class UnmanagedAMLauncher
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Applications.Unmanagedamlauncher.UnmanagedAMLauncher
			));

		private Configuration conf;

		protected internal YarnClient rmClient;

		private string appName = string.Empty;

		private int amPriority = 0;

		private string amQueue = string.Empty;

		private string amCmd = null;

		private string classpath = null;

		private volatile bool amCompleted = false;

		private const long AmStateWaitTimeoutMs = 10000;

		// Handle to talk to the Resource Manager/Applications Manager
		// Application master specific info to register a new Application with RM/ASM
		// App master priority
		// Queue for App master
		// cmd to start AM
		// set the classpath explicitly
		/// <param name="args">Command line arguments</param>
		public static void Main(string[] args)
		{
			try
			{
				Org.Apache.Hadoop.Yarn.Applications.Unmanagedamlauncher.UnmanagedAMLauncher client
					 = new Org.Apache.Hadoop.Yarn.Applications.Unmanagedamlauncher.UnmanagedAMLauncher
					();
				Log.Info("Initializing Client");
				bool doRun = client.Init(args);
				if (!doRun)
				{
					System.Environment.Exit(0);
				}
				client.Run();
			}
			catch (Exception t)
			{
				Log.Fatal("Error running Client", t);
				System.Environment.Exit(1);
			}
		}

		/// <exception cref="System.Exception"/>
		public UnmanagedAMLauncher(Configuration conf)
		{
			// Set up RPC
			this.conf = conf;
		}

		/// <exception cref="System.Exception"/>
		public UnmanagedAMLauncher()
			: this(new Configuration())
		{
		}

		private void PrintUsage(Options opts)
		{
			new HelpFormatter().PrintHelp("Client", opts);
		}

		/// <exception cref="Org.Apache.Commons.Cli.ParseException"/>
		public virtual bool Init(string[] args)
		{
			Options opts = new Options();
			opts.AddOption("appname", true, "Application Name. Default value - UnmanagedAM");
			opts.AddOption("priority", true, "Application Priority. Default 0");
			opts.AddOption("queue", true, "RM Queue in which this application is to be submitted"
				);
			opts.AddOption("master_memory", true, "Amount of memory in MB to be requested to run the application master"
				);
			opts.AddOption("cmd", true, "command to start unmanaged AM (required)");
			opts.AddOption("classpath", true, "additional classpath");
			opts.AddOption("help", false, "Print usage");
			CommandLine cliParser = new GnuParser().Parse(opts, args);
			if (args.Length == 0)
			{
				PrintUsage(opts);
				throw new ArgumentException("No args specified for client to initialize");
			}
			if (cliParser.HasOption("help"))
			{
				PrintUsage(opts);
				return false;
			}
			appName = cliParser.GetOptionValue("appname", "UnmanagedAM");
			amPriority = System.Convert.ToInt32(cliParser.GetOptionValue("priority", "0"));
			amQueue = cliParser.GetOptionValue("queue", "default");
			classpath = cliParser.GetOptionValue("classpath", null);
			amCmd = cliParser.GetOptionValue("cmd");
			if (amCmd == null)
			{
				PrintUsage(opts);
				throw new ArgumentException("No cmd specified for application master");
			}
			YarnConfiguration yarnConf = new YarnConfiguration(conf);
			rmClient = YarnClient.CreateYarnClient();
			rmClient.Init(yarnConf);
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void LaunchAM(ApplicationAttemptId attemptId)
		{
			Credentials credentials = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = rmClient.GetAMRMToken
				(attemptId.GetApplicationId());
			// Service will be empty but that's okay, we are just passing down only
			// AMRMToken down to the real AM which eventually sets the correct
			// service-address.
			credentials.AddToken(token.GetService(), token);
			FilePath tokenFile = FilePath.CreateTempFile("unmanagedAMRMToken", string.Empty, 
				new FilePath(Runtime.GetProperty("user.dir")));
			try
			{
				FileUtil.Chmod(tokenFile.GetAbsolutePath(), "600");
			}
			catch (Exception ex)
			{
				throw new RuntimeException(ex);
			}
			tokenFile.DeleteOnExit();
			DataOutputStream os = new DataOutputStream(new FileOutputStream(tokenFile, true));
			credentials.WriteTokenStorageToStream(os);
			os.Close();
			IDictionary<string, string> env = Sharpen.Runtime.GetEnv();
			AList<string> envAMList = new AList<string>();
			bool setClasspath = false;
			foreach (KeyValuePair<string, string> entry in env)
			{
				string key = entry.Key;
				string value = entry.Value;
				if (key.Equals("CLASSPATH"))
				{
					setClasspath = true;
					if (classpath != null)
					{
						value = value + FilePath.pathSeparator + classpath;
					}
				}
				envAMList.AddItem(key + "=" + value);
			}
			if (!setClasspath && classpath != null)
			{
				envAMList.AddItem("CLASSPATH=" + classpath);
			}
			ContainerId containerId = ContainerId.NewContainerId(attemptId, 0);
			string hostname = Sharpen.Runtime.GetLocalHost().GetHostName();
			envAMList.AddItem(ApplicationConstants.Environment.ContainerId.ToString() + "=" +
				 containerId);
			envAMList.AddItem(ApplicationConstants.Environment.NmHost.ToString() + "=" + hostname
				);
			envAMList.AddItem(ApplicationConstants.Environment.NmHttpPort.ToString() + "=0");
			envAMList.AddItem(ApplicationConstants.Environment.NmPort.ToString() + "=0");
			envAMList.AddItem(ApplicationConstants.Environment.LocalDirs.ToString() + "= /tmp"
				);
			envAMList.AddItem(ApplicationConstants.AppSubmitTimeEnv + "=" + Runtime.CurrentTimeMillis
				());
			envAMList.AddItem(ApplicationConstants.ContainerTokenFileEnvName + "=" + tokenFile
				.GetAbsolutePath());
			string[] envAM = new string[envAMList.Count];
			SystemProcess amProc = Runtime.GetRuntime().Exec(amCmd, Sharpen.Collections.ToArray
				(envAMList, envAM));
			BufferedReader errReader = new BufferedReader(new InputStreamReader(amProc.GetErrorStream
				(), Sharpen.Extensions.GetEncoding("UTF-8")));
			BufferedReader inReader = new BufferedReader(new InputStreamReader(amProc.GetInputStream
				(), Sharpen.Extensions.GetEncoding("UTF-8")));
			// read error and input streams as this would free up the buffers
			// free the error stream buffer
			Sharpen.Thread errThread = new _Thread_244(errReader);
			Sharpen.Thread outThread = new _Thread_258(inReader);
			try
			{
				errThread.Start();
				outThread.Start();
			}
			catch (InvalidOperationException)
			{
			}
			// wait for the process to finish and check the exit code
			try
			{
				int exitCode = amProc.WaitFor();
				Log.Info("AM process exited with value: " + exitCode);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
			finally
			{
				amCompleted = true;
			}
			try
			{
				// make sure that the error thread exits
				// on Windows these threads sometimes get stuck and hang the execution
				// timeout and join later after destroying the process.
				errThread.Join();
				outThread.Join();
				errReader.Close();
				inReader.Close();
			}
			catch (Exception ie)
			{
				Log.Info("ShellExecutor: Interrupted while reading the error/out stream", ie);
			}
			catch (IOException ioe)
			{
				Log.Warn("Error while closing the error/out stream", ioe);
			}
			amProc.Destroy();
		}

		private sealed class _Thread_244 : Sharpen.Thread
		{
			public _Thread_244(BufferedReader errReader)
			{
				this.errReader = errReader;
			}

			public override void Run()
			{
				try
				{
					string line = errReader.ReadLine();
					while ((line != null) && !this.IsInterrupted())
					{
						System.Console.Error.WriteLine(line);
						line = errReader.ReadLine();
					}
				}
				catch (IOException ioe)
				{
					Org.Apache.Hadoop.Yarn.Applications.Unmanagedamlauncher.UnmanagedAMLauncher.Log.Warn
						("Error reading the error stream", ioe);
				}
			}

			private readonly BufferedReader errReader;
		}

		private sealed class _Thread_258 : Sharpen.Thread
		{
			public _Thread_258(BufferedReader inReader)
			{
				this.inReader = inReader;
			}

			public override void Run()
			{
				try
				{
					string line = inReader.ReadLine();
					while ((line != null) && !this.IsInterrupted())
					{
						System.Console.Out.WriteLine(line);
						line = inReader.ReadLine();
					}
				}
				catch (IOException ioe)
				{
					Org.Apache.Hadoop.Yarn.Applications.Unmanagedamlauncher.UnmanagedAMLauncher.Log.Warn
						("Error reading the out stream", ioe);
				}
			}

			private readonly BufferedReader inReader;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual bool Run()
		{
			Log.Info("Starting Client");
			// Connect to ResourceManager
			rmClient.Start();
			try
			{
				// Create launch context for app master
				Log.Info("Setting up application submission context for ASM");
				ApplicationSubmissionContext appContext = rmClient.CreateApplication().GetApplicationSubmissionContext
					();
				ApplicationId appId = appContext.GetApplicationId();
				// set the application name
				appContext.SetApplicationName(appName);
				// Set the priority for the application master
				Priority pri = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Priority>();
				pri.SetPriority(amPriority);
				appContext.SetPriority(pri);
				// Set the queue to which this application is to be submitted in the RM
				appContext.SetQueue(amQueue);
				// Set up the container launch context for the application master
				ContainerLaunchContext amContainer = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<ContainerLaunchContext>();
				appContext.SetAMContainerSpec(amContainer);
				// unmanaged AM
				appContext.SetUnmanagedAM(true);
				Log.Info("Setting unmanaged AM");
				// Submit the application to the applications manager
				Log.Info("Submitting application to ASM");
				rmClient.SubmitApplication(appContext);
				ApplicationReport appReport = MonitorApplication(appId, EnumSet.Of(YarnApplicationState
					.Accepted, YarnApplicationState.Killed, YarnApplicationState.Failed, YarnApplicationState
					.Finished));
				if (appReport.GetYarnApplicationState() == YarnApplicationState.Accepted)
				{
					// Monitor the application attempt to wait for launch state
					ApplicationAttemptReport attemptReport = MonitorCurrentAppAttempt(appId, YarnApplicationAttemptState
						.Launched);
					ApplicationAttemptId attemptId = attemptReport.GetApplicationAttemptId();
					Log.Info("Launching AM with application attempt id " + attemptId);
					// launch AM
					LaunchAM(attemptId);
					// Monitor the application for end state
					appReport = MonitorApplication(appId, EnumSet.Of(YarnApplicationState.Killed, YarnApplicationState
						.Failed, YarnApplicationState.Finished));
				}
				YarnApplicationState appState = appReport.GetYarnApplicationState();
				FinalApplicationStatus appStatus = appReport.GetFinalApplicationStatus();
				Log.Info("App ended with state: " + appReport.GetYarnApplicationState() + " and status: "
					 + appStatus);
				bool success;
				if (YarnApplicationState.Finished == appState && FinalApplicationStatus.Succeeded
					 == appStatus)
				{
					Log.Info("Application has completed successfully.");
					success = true;
				}
				else
				{
					Log.Info("Application did finished unsuccessfully." + " YarnState=" + appState.ToString
						() + ", FinalStatus=" + appStatus.ToString());
					success = false;
				}
				return success;
			}
			finally
			{
				rmClient.Stop();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private ApplicationAttemptReport MonitorCurrentAppAttempt(ApplicationId appId, YarnApplicationAttemptState
			 attemptState)
		{
			long startTime = Runtime.CurrentTimeMillis();
			ApplicationAttemptId attemptId = null;
			while (true)
			{
				if (attemptId == null)
				{
					attemptId = rmClient.GetApplicationReport(appId).GetCurrentApplicationAttemptId();
				}
				ApplicationAttemptReport attemptReport = null;
				if (attemptId != null)
				{
					attemptReport = rmClient.GetApplicationAttemptReport(attemptId);
					if (attemptState.Equals(attemptReport.GetYarnApplicationAttemptState()))
					{
						return attemptReport;
					}
				}
				Log.Info("Current attempt state of " + appId + " is " + (attemptReport == null ? 
					" N/A " : attemptReport.GetYarnApplicationAttemptState()) + ", waiting for current attempt to reach "
					 + attemptState);
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
					Log.Warn("Interrupted while waiting for current attempt of " + appId + " to reach "
						 + attemptState);
				}
				if (Runtime.CurrentTimeMillis() - startTime > AmStateWaitTimeoutMs)
				{
					string errmsg = "Timeout for waiting current attempt of " + appId + " to reach " 
						+ attemptState;
					Log.Error(errmsg);
					throw new RuntimeException(errmsg);
				}
			}
		}

		/// <summary>Monitor the submitted application for completion.</summary>
		/// <remarks>
		/// Monitor the submitted application for completion. Kill application if time
		/// expires.
		/// </remarks>
		/// <param name="appId">Application Id of application to be monitored</param>
		/// <returns>true if application completed successfully</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private ApplicationReport MonitorApplication(ApplicationId appId, ICollection<YarnApplicationState
			> finalState)
		{
			long foundAMCompletedTime = 0;
			StringBuilder expectedFinalState = new StringBuilder();
			bool first = true;
			foreach (YarnApplicationState state in finalState)
			{
				if (first)
				{
					first = false;
					expectedFinalState.Append(state.ToString());
				}
				else
				{
					expectedFinalState.Append("," + state.ToString());
				}
			}
			while (true)
			{
				// Check app status every 1 second.
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
					Log.Debug("Thread sleep in monitoring loop interrupted");
				}
				// Get application report for the appId we are interested in
				ApplicationReport report = rmClient.GetApplicationReport(appId);
				Log.Info("Got application report from ASM for" + ", appId=" + appId.GetId() + ", appAttemptId="
					 + report.GetCurrentApplicationAttemptId() + ", clientToAMToken=" + report.GetClientToAMToken
					() + ", appDiagnostics=" + report.GetDiagnostics() + ", appMasterHost=" + report
					.GetHost() + ", appQueue=" + report.GetQueue() + ", appMasterRpcPort=" + report.
					GetRpcPort() + ", appStartTime=" + report.GetStartTime() + ", yarnAppState=" + report
					.GetYarnApplicationState().ToString() + ", distributedFinalState=" + report.GetFinalApplicationStatus
					().ToString() + ", appTrackingUrl=" + report.GetTrackingUrl() + ", appUser=" + report
					.GetUser());
				YarnApplicationState state_1 = report.GetYarnApplicationState();
				if (finalState.Contains(state_1))
				{
					return report;
				}
				// wait for 10 seconds after process has completed for app report to
				// come back
				if (amCompleted)
				{
					if (foundAMCompletedTime == 0)
					{
						foundAMCompletedTime = Runtime.CurrentTimeMillis();
					}
					else
					{
						if ((Runtime.CurrentTimeMillis() - foundAMCompletedTime) > AmStateWaitTimeoutMs)
						{
							Log.Warn("Waited " + AmStateWaitTimeoutMs / 1000 + " seconds after process completed for AppReport"
								 + " to reach desired final state. Not waiting anymore." + "CurrentState = " + state_1
								 + ", ExpectedStates = " + expectedFinalState.ToString());
							throw new RuntimeException("Failed to receive final expected state" + " in ApplicationReport"
								 + ", CurrentState=" + state_1 + ", ExpectedStates=" + expectedFinalState.ToString
								());
						}
					}
				}
			}
		}
	}
}
