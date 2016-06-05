using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Client.Api.Async;
using Org.Apache.Hadoop.Yarn.Client.Api.Async.Impl;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Log4j;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Applications.Distributedshell
{
	/// <summary>
	/// An ApplicationMaster for executing shell commands on a set of launched
	/// containers using the YARN framework.
	/// </summary>
	/// <remarks>
	/// An ApplicationMaster for executing shell commands on a set of launched
	/// containers using the YARN framework.
	/// <p>
	/// This class is meant to act as an example on how to write yarn-based
	/// application masters.
	/// </p>
	/// <p>
	/// The ApplicationMaster is started on a container by the
	/// <code>ResourceManager</code>'s launcher. The first thing that the
	/// <code>ApplicationMaster</code> needs to do is to connect and register itself
	/// with the <code>ResourceManager</code>. The registration sets up information
	/// within the <code>ResourceManager</code> regarding what host:port the
	/// ApplicationMaster is listening on to provide any form of functionality to a
	/// client as well as a tracking url that a client can use to keep track of
	/// status/job history if needed. However, in the distributedshell, trackingurl
	/// and appMasterHost:appMasterRpcPort are not supported.
	/// </p>
	/// <p>
	/// The <code>ApplicationMaster</code> needs to send a heartbeat to the
	/// <code>ResourceManager</code> at regular intervals to inform the
	/// <code>ResourceManager</code> that it is up and alive. The
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest)
	/// 	"/>
	/// to the <code>ResourceManager</code> from the
	/// <code>ApplicationMaster</code> acts as a heartbeat.
	/// <p>
	/// For the actual handling of the job, the <code>ApplicationMaster</code> has to
	/// request the <code>ResourceManager</code> via
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest"/>
	/// for the
	/// required no. of containers using
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ResourceRequest"/>
	/// with the necessary
	/// resource specifications such as node location, computational
	/// (memory/disk/cpu) resource requirements. The <code>ResourceManager</code>
	/// responds with an
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateResponse"/>
	/// that informs the
	/// <code>ApplicationMaster</code> of the set of newly allocated containers,
	/// completed containers as well as current state of available resources.
	/// </p>
	/// <p>
	/// For each allocated container, the <code>ApplicationMaster</code> can then set
	/// up the necessary launch context via
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerLaunchContext"/>
	/// to specify
	/// the allocated container id, local resources required by the executable, the
	/// environment to be setup for the executable, commands to execute, etc. and
	/// submit a
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainerRequest"/>
	/// to the
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol"/>
	/// to
	/// launch and execute the defined commands on the given allocated container.
	/// </p>
	/// <p>
	/// The <code>ApplicationMaster</code> can monitor the launched container by
	/// either querying the <code>ResourceManager</code> using
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest)
	/// 	"/>
	/// to get updates on completed containers or via
	/// the
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol"/>
	/// by querying for the status of the allocated
	/// container's
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
	/// .
	/// <p>
	/// After the job has been completed, the <code>ApplicationMaster</code> has to
	/// send a
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.FinishApplicationMasterRequest
	/// 	"/>
	/// to the
	/// <code>ResourceManager</code> to inform it that the
	/// <code>ApplicationMaster</code> has been completed.
	/// </remarks>
	public class ApplicationMaster
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Applications.Distributedshell.ApplicationMaster
			));

		public enum DSEvent
		{
			DsAppAttemptStart,
			DsAppAttemptEnd,
			DsContainerStart,
			DsContainerEnd
		}

		public enum DSEntity
		{
			DsAppAttempt,
			DsContainer
		}

		private Configuration conf;

		private AMRMClientAsync amRMClient;

		[VisibleForTesting]
		internal UserGroupInformation appSubmitterUgi;

		private NMClientAsync nmClientAsync;

		private ApplicationMaster.NMCallbackHandler containerListener;

		[VisibleForTesting]
		protected internal ApplicationAttemptId appAttemptID;

		private string appMasterHostname = string.Empty;

		private int appMasterRpcPort = -1;

		private string appMasterTrackingUrl = string.Empty;

		[VisibleForTesting]
		protected internal int numTotalContainers = 1;

		private int containerMemory = 10;

		private int containerVirtualCores = 1;

		private int requestPriority;

		private AtomicInteger numCompletedContainers = new AtomicInteger();

		[VisibleForTesting]
		protected internal AtomicInteger numAllocatedContainers = new AtomicInteger();

		private AtomicInteger numFailedContainers = new AtomicInteger();

		[VisibleForTesting]
		protected internal AtomicInteger numRequestedContainers = new AtomicInteger();

		private string shellCommand = string.Empty;

		private string shellArgs = string.Empty;

		private IDictionary<string, string> shellEnv = new Dictionary<string, string>();

		private string scriptPath = string.Empty;

		private long shellScriptPathTimestamp = 0;

		private long shellScriptPathLen = 0;

		private string domainId = null;

		private const string ExecShellStringPath = Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
			.ScriptPath + ".sh";

		private const string ExecBatScripStringtPath = Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
			.ScriptPath + ".bat";

		private const string log4jPath = "log4j.properties";

		private const string shellCommandPath = "shellCommands";

		private const string shellArgsPath = "shellArgs";

		private volatile bool done;

		private ByteBuffer allTokens;

		private IList<Sharpen.Thread> launchThreads = new AList<Sharpen.Thread>();

		[VisibleForTesting]
		internal TimelineClient timelineClient;

		private readonly string linux_bash_command = "bash";

		private readonly string windows_command = "cmd /c";

		// Configuration
		// Handle to communicate with the Resource Manager
		// In both secure and non-secure modes, this points to the job-submitter.
		// Handle to communicate with the Node Manager
		// Listen to process the response from the Node Manager
		// Application Attempt Id ( combination of attemptId and fail count )
		// TODO
		// For status update for clients - yet to be implemented
		// Hostname of the container
		// Port on which the app master listens for status updates from clients
		// Tracking url to which app master publishes info for clients to monitor
		// App Master configuration
		// No. of containers to run shell command on
		// Memory to request for the container on which the shell command will run
		// VirtualCores to request for the container on which the shell command will run
		// Priority of the request
		// Counter for completed containers ( complete denotes successful or failed )
		// Allocated container count so that we know how many containers has the RM
		// allocated to us
		// Count of failed containers
		// Count of containers already requested from the RM
		// Needed as once requested, we should not request for containers again.
		// Only request for more if the original requirement changes.
		// Shell command to be executed
		// Args to be passed to the shell command
		// Env variables to be setup for the shell command
		// Location of shell script ( obtained from info set in env )
		// Shell script path in fs
		// Timestamp needed for creating a local resource
		// File length needed for local resource
		// Timeline domain ID
		// Hardcoded path to shell script in launch container's local env
		// Hardcoded path to custom log_properties
		// Launch threads
		// Timeline Client
		/// <param name="args">Command line args</param>
		public static void Main(string[] args)
		{
			bool result = false;
			try
			{
				Org.Apache.Hadoop.Yarn.Applications.Distributedshell.ApplicationMaster appMaster = 
					new Org.Apache.Hadoop.Yarn.Applications.Distributedshell.ApplicationMaster();
				Log.Info("Initializing ApplicationMaster");
				bool doRun = appMaster.Init(args);
				if (!doRun)
				{
					System.Environment.Exit(0);
				}
				appMaster.Run();
				result = appMaster.Finish();
			}
			catch (Exception t)
			{
				Log.Fatal("Error running ApplicationMaster", t);
				LogManager.Shutdown();
				ExitUtil.Terminate(1, t);
			}
			if (result)
			{
				Log.Info("Application Master completed successfully. exiting");
				System.Environment.Exit(0);
			}
			else
			{
				Log.Info("Application Master failed. exiting");
				System.Environment.Exit(2);
			}
		}

		/// <summary>Dump out contents of $CWD and the environment to stdout for debugging</summary>
		private void DumpOutDebugInfo()
		{
			Log.Info("Dump debug output");
			IDictionary<string, string> envs = Sharpen.Runtime.GetEnv();
			foreach (KeyValuePair<string, string> env in envs)
			{
				Log.Info("System env: key=" + env.Key + ", val=" + env.Value);
				System.Console.Out.WriteLine("System env: key=" + env.Key + ", val=" + env.Value);
			}
			BufferedReader buf = null;
			try
			{
				string lines = Shell.Windows ? Shell.ExecCommand("cmd", "/c", "dir") : Shell.ExecCommand
					("ls", "-al");
				buf = new BufferedReader(new StringReader(lines));
				string line = string.Empty;
				while ((line = buf.ReadLine()) != null)
				{
					Log.Info("System CWD content: " + line);
					System.Console.Out.WriteLine("System CWD content: " + line);
				}
			}
			catch (IOException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
			finally
			{
				IOUtils.Cleanup(Log, buf);
			}
		}

		public ApplicationMaster()
		{
			// Set up the configuration
			conf = new YarnConfiguration();
		}

		/// <summary>Parse command line options</summary>
		/// <param name="args">Command line args</param>
		/// <returns>Whether init successful and run should be invoked</returns>
		/// <exception cref="Org.Apache.Commons.Cli.ParseException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Init(string[] args)
		{
			Options opts = new Options();
			opts.AddOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes"
				);
			opts.AddOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs"
				);
			opts.AddOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command"
				);
			opts.AddOption("container_vcores", true, "Amount of virtual cores to be requested to run the shell command"
				);
			opts.AddOption("num_containers", true, "No. of containers on which the shell command needs to be executed"
				);
			opts.AddOption("priority", true, "Application Priority. Default 0");
			opts.AddOption("debug", false, "Dump out debug information");
			opts.AddOption("help", false, "Print usage");
			CommandLine cliParser = new GnuParser().Parse(opts, args);
			if (args.Length == 0)
			{
				PrintUsage(opts);
				throw new ArgumentException("No args specified for application master to initialize"
					);
			}
			//Check whether customer log4j.properties file exists
			if (FileExist(log4jPath))
			{
				try
				{
					Log4jPropertyHelper.UpdateLog4jConfiguration(typeof(Org.Apache.Hadoop.Yarn.Applications.Distributedshell.ApplicationMaster
						), log4jPath);
				}
				catch (Exception e)
				{
					Log.Warn("Can not set up custom log4j properties. " + e);
				}
			}
			if (cliParser.HasOption("help"))
			{
				PrintUsage(opts);
				return false;
			}
			if (cliParser.HasOption("debug"))
			{
				DumpOutDebugInfo();
			}
			IDictionary<string, string> envs = Sharpen.Runtime.GetEnv();
			if (!envs.Contains(ApplicationConstants.Environment.ContainerId.ToString()))
			{
				if (cliParser.HasOption("app_attempt_id"))
				{
					string appIdStr = cliParser.GetOptionValue("app_attempt_id", string.Empty);
					appAttemptID = ConverterUtils.ToApplicationAttemptId(appIdStr);
				}
				else
				{
					throw new ArgumentException("Application Attempt Id not set in the environment");
				}
			}
			else
			{
				ContainerId containerId = ConverterUtils.ToContainerId(envs[ApplicationConstants.Environment
					.ContainerId.ToString()]);
				appAttemptID = containerId.GetApplicationAttemptId();
			}
			if (!envs.Contains(ApplicationConstants.AppSubmitTimeEnv))
			{
				throw new RuntimeException(ApplicationConstants.AppSubmitTimeEnv + " not set in the environment"
					);
			}
			if (!envs.Contains(ApplicationConstants.Environment.NmHost.ToString()))
			{
				throw new RuntimeException(ApplicationConstants.Environment.NmHost.ToString() + " not set in the environment"
					);
			}
			if (!envs.Contains(ApplicationConstants.Environment.NmHttpPort.ToString()))
			{
				throw new RuntimeException(ApplicationConstants.Environment.NmHttpPort + " not set in the environment"
					);
			}
			if (!envs.Contains(ApplicationConstants.Environment.NmPort.ToString()))
			{
				throw new RuntimeException(ApplicationConstants.Environment.NmPort.ToString() + " not set in the environment"
					);
			}
			Log.Info("Application master for app" + ", appId=" + appAttemptID.GetApplicationId
				().GetId() + ", clustertimestamp=" + appAttemptID.GetApplicationId().GetClusterTimestamp
				() + ", attemptId=" + appAttemptID.GetAttemptId());
			if (!FileExist(shellCommandPath) && envs[DSConstants.Distributedshellscriptlocation
				].IsEmpty())
			{
				throw new ArgumentException("No shell command or shell script specified to be executed by application master"
					);
			}
			if (FileExist(shellCommandPath))
			{
				shellCommand = ReadContent(shellCommandPath);
			}
			if (FileExist(shellArgsPath))
			{
				shellArgs = ReadContent(shellArgsPath);
			}
			if (cliParser.HasOption("shell_env"))
			{
				string[] shellEnvs = cliParser.GetOptionValues("shell_env");
				foreach (string env in shellEnvs)
				{
					env = env.Trim();
					int index = env.IndexOf('=');
					if (index == -1)
					{
						shellEnv[env] = string.Empty;
						continue;
					}
					string key = Sharpen.Runtime.Substring(env, 0, index);
					string val = string.Empty;
					if (index < (env.Length - 1))
					{
						val = Sharpen.Runtime.Substring(env, index + 1);
					}
					shellEnv[key] = val;
				}
			}
			if (envs.Contains(DSConstants.Distributedshellscriptlocation))
			{
				scriptPath = envs[DSConstants.Distributedshellscriptlocation];
				if (envs.Contains(DSConstants.Distributedshellscripttimestamp))
				{
					shellScriptPathTimestamp = long.Parse(envs[DSConstants.Distributedshellscripttimestamp
						]);
				}
				if (envs.Contains(DSConstants.Distributedshellscriptlen))
				{
					shellScriptPathLen = long.Parse(envs[DSConstants.Distributedshellscriptlen]);
				}
				if (!scriptPath.IsEmpty() && (shellScriptPathTimestamp <= 0 || shellScriptPathLen
					 <= 0))
				{
					Log.Error("Illegal values in env for shell script path" + ", path=" + scriptPath 
						+ ", len=" + shellScriptPathLen + ", timestamp=" + shellScriptPathTimestamp);
					throw new ArgumentException("Illegal values in env for shell script path");
				}
			}
			if (envs.Contains(DSConstants.Distributedshelltimelinedomain))
			{
				domainId = envs[DSConstants.Distributedshelltimelinedomain];
			}
			containerMemory = System.Convert.ToInt32(cliParser.GetOptionValue("container_memory"
				, "10"));
			containerVirtualCores = System.Convert.ToInt32(cliParser.GetOptionValue("container_vcores"
				, "1"));
			numTotalContainers = System.Convert.ToInt32(cliParser.GetOptionValue("num_containers"
				, "1"));
			if (numTotalContainers == 0)
			{
				throw new ArgumentException("Cannot run distributed shell with no containers");
			}
			requestPriority = System.Convert.ToInt32(cliParser.GetOptionValue("priority", "0"
				));
			return true;
		}

		/// <summary>Helper function to print usage</summary>
		/// <param name="opts">Parsed command line options</param>
		private void PrintUsage(Options opts)
		{
			new HelpFormatter().PrintHelp("ApplicationMaster", opts);
		}

		/// <summary>Main run function for the application master</summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void Run()
		{
			Log.Info("Starting ApplicationMaster");
			// Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
			// are marked as LimitedPrivate
			Credentials credentials = UserGroupInformation.GetCurrentUser().GetCredentials();
			DataOutputBuffer dob = new DataOutputBuffer();
			credentials.WriteTokenStorageToStream(dob);
			// Now remove the AM->RM token so that containers cannot access it.
			IEnumerator<Org.Apache.Hadoop.Security.Token.Token<object>> iter = credentials.GetAllTokens
				().GetEnumerator();
			Log.Info("Executing with tokens:");
			while (iter.HasNext())
			{
				Org.Apache.Hadoop.Security.Token.Token<object> token = iter.Next();
				Log.Info(token);
				if (token.GetKind().Equals(AMRMTokenIdentifier.KindName))
				{
					iter.Remove();
				}
			}
			allTokens = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
			// Create appSubmitterUgi and add original tokens to it
			string appSubmitterUserName = Runtime.Getenv(ApplicationConstants.Environment.User
				.ToString());
			appSubmitterUgi = UserGroupInformation.CreateRemoteUser(appSubmitterUserName);
			appSubmitterUgi.AddCredentials(credentials);
			AMRMClientAsync.CallbackHandler allocListener = new ApplicationMaster.RMCallbackHandler
				(this);
			amRMClient = AMRMClientAsync.CreateAMRMClientAsync(1000, allocListener);
			amRMClient.Init(conf);
			amRMClient.Start();
			containerListener = CreateNMCallbackHandler();
			nmClientAsync = new NMClientAsyncImpl(containerListener);
			nmClientAsync.Init(conf);
			nmClientAsync.Start();
			StartTimelineClient(conf);
			if (timelineClient != null)
			{
				PublishApplicationAttemptEvent(timelineClient, appAttemptID.ToString(), ApplicationMaster.DSEvent
					.DsAppAttemptStart, domainId, appSubmitterUgi);
			}
			// Setup local RPC Server to accept status requests directly from clients
			// TODO need to setup a protocol for client to be able to communicate to
			// the RPC server
			// TODO use the rpc port info to register with the RM for the client to
			// send requests to this app master
			// Register self with ResourceManager
			// This will start heartbeating to the RM
			appMasterHostname = NetUtils.GetHostname();
			RegisterApplicationMasterResponse response = amRMClient.RegisterApplicationMaster
				(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);
			// Dump out information about cluster capability as seen by the
			// resource manager
			int maxMem = response.GetMaximumResourceCapability().GetMemory();
			Log.Info("Max mem capabililty of resources in this cluster " + maxMem);
			int maxVCores = response.GetMaximumResourceCapability().GetVirtualCores();
			Log.Info("Max vcores capabililty of resources in this cluster " + maxVCores);
			// A resource ask cannot exceed the max.
			if (containerMemory > maxMem)
			{
				Log.Info("Container memory specified above max threshold of cluster." + " Using max value."
					 + ", specified=" + containerMemory + ", max=" + maxMem);
				containerMemory = maxMem;
			}
			if (containerVirtualCores > maxVCores)
			{
				Log.Info("Container virtual cores specified above max threshold of cluster." + " Using max value."
					 + ", specified=" + containerVirtualCores + ", max=" + maxVCores);
				containerVirtualCores = maxVCores;
			}
			IList<Container> previousAMRunningContainers = response.GetContainersFromPreviousAttempts
				();
			Log.Info(appAttemptID + " received " + previousAMRunningContainers.Count + " previous attempts' running containers on AM registration."
				);
			numAllocatedContainers.AddAndGet(previousAMRunningContainers.Count);
			int numTotalContainersToRequest = numTotalContainers - previousAMRunningContainers
				.Count;
			// Setup ask for containers from RM
			// Send request for containers to RM
			// Until we get our fully allocated quota, we keep on polling RM for
			// containers
			// Keep looping until all the containers are launched and shell script
			// executed on them ( regardless of success/failure).
			for (int i = 0; i < numTotalContainersToRequest; ++i)
			{
				AMRMClient.ContainerRequest containerAsk = SetupContainerAskForRM();
				amRMClient.AddContainerRequest(containerAsk);
			}
			numRequestedContainers.Set(numTotalContainers);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		internal virtual void StartTimelineClient(Configuration conf)
		{
			try
			{
				appSubmitterUgi.DoAs(new _PrivilegedExceptionAction_625(this, conf));
			}
			catch (UndeclaredThrowableException e)
			{
				// Creating the Timeline Client
				throw new YarnException(e.InnerException);
			}
		}

		private sealed class _PrivilegedExceptionAction_625 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_625(ApplicationMaster _enclosing, Configuration
				 conf)
			{
				this._enclosing = _enclosing;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				if (conf.GetBoolean(YarnConfiguration.TimelineServiceEnabled, YarnConfiguration.DefaultTimelineServiceEnabled
					))
				{
					this._enclosing.timelineClient = TimelineClient.CreateTimelineClient();
					this._enclosing.timelineClient.Init(conf);
					this._enclosing.timelineClient.Start();
				}
				else
				{
					this._enclosing.timelineClient = null;
					Org.Apache.Hadoop.Yarn.Applications.Distributedshell.ApplicationMaster.Log.Warn("Timeline service is not enabled"
						);
				}
				return null;
			}

			private readonly ApplicationMaster _enclosing;

			private readonly Configuration conf;
		}

		[VisibleForTesting]
		internal virtual ApplicationMaster.NMCallbackHandler CreateNMCallbackHandler()
		{
			return new ApplicationMaster.NMCallbackHandler(this);
		}

		[VisibleForTesting]
		protected internal virtual bool Finish()
		{
			// wait for completion.
			while (!done && (numCompletedContainers.Get() != numTotalContainers))
			{
				try
				{
					Sharpen.Thread.Sleep(200);
				}
				catch (Exception)
				{
				}
			}
			if (timelineClient != null)
			{
				PublishApplicationAttemptEvent(timelineClient, appAttemptID.ToString(), ApplicationMaster.DSEvent
					.DsAppAttemptEnd, domainId, appSubmitterUgi);
			}
			// Join all launched threads
			// needed for when we time out
			// and we need to release containers
			foreach (Sharpen.Thread launchThread in launchThreads)
			{
				try
				{
					launchThread.Join(10000);
				}
				catch (Exception e)
				{
					Log.Info("Exception thrown in thread join: " + e.Message);
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}
			// When the application completes, it should stop all running containers
			Log.Info("Application completed. Stopping running containers");
			nmClientAsync.Stop();
			// When the application completes, it should send a finish application
			// signal to the RM
			Log.Info("Application completed. Signalling finish to RM");
			FinalApplicationStatus appStatus;
			string appMessage = null;
			bool success = true;
			if (numFailedContainers.Get() == 0 && numCompletedContainers.Get() == numTotalContainers)
			{
				appStatus = FinalApplicationStatus.Succeeded;
			}
			else
			{
				appStatus = FinalApplicationStatus.Failed;
				appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed=" + 
					numCompletedContainers.Get() + ", allocated=" + numAllocatedContainers.Get() + ", failed="
					 + numFailedContainers.Get();
				Log.Info(appMessage);
				success = false;
			}
			try
			{
				amRMClient.UnregisterApplicationMaster(appStatus, appMessage, null);
			}
			catch (YarnException ex)
			{
				Log.Error("Failed to unregister application", ex);
			}
			catch (IOException e)
			{
				Log.Error("Failed to unregister application", e);
			}
			amRMClient.Stop();
			// Stop Timeline Client
			if (timelineClient != null)
			{
				timelineClient.Stop();
			}
			return success;
		}

		private class RMCallbackHandler : AMRMClientAsync.CallbackHandler
		{
			public virtual void OnContainersCompleted(IList<ContainerStatus> completedContainers
				)
			{
				ApplicationMaster.Log.Info("Got response from RM for container ask, completedCnt="
					 + completedContainers.Count);
				foreach (ContainerStatus containerStatus in completedContainers)
				{
					ApplicationMaster.Log.Info(this._enclosing.appAttemptID + " got container status for containerID="
						 + containerStatus.GetContainerId() + ", state=" + containerStatus.GetState() + 
						", exitStatus=" + containerStatus.GetExitStatus() + ", diagnostics=" + containerStatus
						.GetDiagnostics());
					// non complete containers should not be here
					System.Diagnostics.Debug.Assert((containerStatus.GetState() == ContainerState.Complete
						));
					// increment counters for completed/failed containers
					int exitStatus = containerStatus.GetExitStatus();
					if (0 != exitStatus)
					{
						// container failed
						if (ContainerExitStatus.Aborted != exitStatus)
						{
							// shell script failed
							// counts as completed
							this._enclosing.numCompletedContainers.IncrementAndGet();
							this._enclosing.numFailedContainers.IncrementAndGet();
						}
						else
						{
							// container was killed by framework, possibly preempted
							// we should re-try as the container was lost for some reason
							this._enclosing.numAllocatedContainers.DecrementAndGet();
							this._enclosing.numRequestedContainers.DecrementAndGet();
						}
					}
					else
					{
						// we do not need to release the container as it would be done
						// by the RM
						// nothing to do
						// container completed successfully
						this._enclosing.numCompletedContainers.IncrementAndGet();
						ApplicationMaster.Log.Info("Container completed successfully." + ", containerId="
							 + containerStatus.GetContainerId());
					}
					if (this._enclosing.timelineClient != null)
					{
						ApplicationMaster.PublishContainerEndEvent(this._enclosing.timelineClient, containerStatus
							, this._enclosing.domainId, this._enclosing.appSubmitterUgi);
					}
				}
				// ask for more containers if any failed
				int askCount = this._enclosing.numTotalContainers - this._enclosing.numRequestedContainers
					.Get();
				this._enclosing.numRequestedContainers.AddAndGet(askCount);
				if (askCount > 0)
				{
					for (int i = 0; i < askCount; ++i)
					{
						AMRMClient.ContainerRequest containerAsk = this._enclosing.SetupContainerAskForRM
							();
						this._enclosing.amRMClient.AddContainerRequest(containerAsk);
					}
				}
				if (this._enclosing.numCompletedContainers.Get() == this._enclosing.numTotalContainers)
				{
					this._enclosing.done = true;
				}
			}

			public virtual void OnContainersAllocated(IList<Container> allocatedContainers)
			{
				ApplicationMaster.Log.Info("Got response from RM for container ask, allocatedCnt="
					 + allocatedContainers.Count);
				this._enclosing.numAllocatedContainers.AddAndGet(allocatedContainers.Count);
				foreach (Container allocatedContainer in allocatedContainers)
				{
					ApplicationMaster.Log.Info("Launching shell command on a new container." + ", containerId="
						 + allocatedContainer.GetId() + ", containerNode=" + allocatedContainer.GetNodeId
						().GetHost() + ":" + allocatedContainer.GetNodeId().GetPort() + ", containerNodeURI="
						 + allocatedContainer.GetNodeHttpAddress() + ", containerResourceMemory" + allocatedContainer
						.GetResource().GetMemory() + ", containerResourceVirtualCores" + allocatedContainer
						.GetResource().GetVirtualCores());
					// + ", containerToken"
					// +allocatedContainer.getContainerToken().getIdentifier().toString());
					ApplicationMaster.LaunchContainerRunnable runnableLaunchContainer = new ApplicationMaster.LaunchContainerRunnable
						(this, allocatedContainer, this._enclosing.containerListener);
					Sharpen.Thread launchThread = new Sharpen.Thread(runnableLaunchContainer);
					// launch and start the container on a separate thread to keep
					// the main thread unblocked
					// as all containers may not be allocated at one go.
					this._enclosing.launchThreads.AddItem(launchThread);
					launchThread.Start();
				}
			}

			public virtual void OnShutdownRequest()
			{
				this._enclosing.done = true;
			}

			public virtual void OnNodesUpdated(IList<NodeReport> updatedNodes)
			{
			}

			public virtual float GetProgress()
			{
				// set progress to deliver to RM on next heartbeat
				float progress = (float)this._enclosing.numCompletedContainers.Get() / this._enclosing
					.numTotalContainers;
				return progress;
			}

			public virtual void OnError(Exception e)
			{
				this._enclosing.done = true;
				this._enclosing.amRMClient.Stop();
			}

			internal RMCallbackHandler(ApplicationMaster _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ApplicationMaster _enclosing;
		}

		internal class NMCallbackHandler : NMClientAsync.CallbackHandler
		{
			private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<
				ContainerId, Container>();

			private readonly ApplicationMaster applicationMaster;

			public NMCallbackHandler(ApplicationMaster applicationMaster)
			{
				this.applicationMaster = applicationMaster;
			}

			public virtual void AddContainer(ContainerId containerId, Container container)
			{
				containers.PutIfAbsent(containerId, container);
			}

			public virtual void OnContainerStopped(ContainerId containerId)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Succeeded to stop Container " + containerId);
				}
				Sharpen.Collections.Remove(containers, containerId);
			}

			public virtual void OnContainerStatusReceived(ContainerId containerId, ContainerStatus
				 containerStatus)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Container Status: id=" + containerId + ", status=" + containerStatus);
				}
			}

			public virtual void OnContainerStarted(ContainerId containerId, IDictionary<string
				, ByteBuffer> allServiceResponse)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Succeeded to start Container " + containerId);
				}
				Container container = containers[containerId];
				if (container != null)
				{
					applicationMaster.nmClientAsync.GetContainerStatusAsync(containerId, container.GetNodeId
						());
				}
				if (applicationMaster.timelineClient != null)
				{
					ApplicationMaster.PublishContainerStartEvent(applicationMaster.timelineClient, container
						, applicationMaster.domainId, applicationMaster.appSubmitterUgi);
				}
			}

			public virtual void OnStartContainerError(ContainerId containerId, Exception t)
			{
				Log.Error("Failed to start Container " + containerId);
				Sharpen.Collections.Remove(containers, containerId);
				applicationMaster.numCompletedContainers.IncrementAndGet();
				applicationMaster.numFailedContainers.IncrementAndGet();
			}

			public virtual void OnGetContainerStatusError(ContainerId containerId, Exception 
				t)
			{
				Log.Error("Failed to query the status of Container " + containerId);
			}

			public virtual void OnStopContainerError(ContainerId containerId, Exception t)
			{
				Log.Error("Failed to stop Container " + containerId);
				Sharpen.Collections.Remove(containers, containerId);
			}
		}

		/// <summary>
		/// Thread to connect to the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol"/>
		/// and launch the container
		/// that will execute the shell command.
		/// </summary>
		private class LaunchContainerRunnable : Runnable
		{
			internal Container container;

			internal ApplicationMaster.NMCallbackHandler containerListener;

			/// <param name="lcontainer">Allocated container</param>
			/// <param name="containerListener">Callback handler of the container</param>
			public LaunchContainerRunnable(ApplicationMaster _enclosing, Container lcontainer
				, ApplicationMaster.NMCallbackHandler containerListener)
			{
				this._enclosing = _enclosing;
				// Allocated container
				this.container = lcontainer;
				this.containerListener = containerListener;
			}

			public virtual void Run()
			{
				ApplicationMaster.Log.Info("Setting up container launch container for containerid="
					 + this.container.GetId());
				// Set the local resources
				IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
					>();
				// The container for the eventual shell commands needs its own local
				// resources too.
				// In this scenario, if a shell script is specified, we need to have it
				// copied and made available to the container.
				if (!this._enclosing.scriptPath.IsEmpty())
				{
					Path renamedScriptPath = null;
					if (Shell.Windows)
					{
						renamedScriptPath = new Path(this._enclosing.scriptPath + ".bat");
					}
					else
					{
						renamedScriptPath = new Path(this._enclosing.scriptPath + ".sh");
					}
					try
					{
						// rename the script file based on the underlying OS syntax.
						this._enclosing.RenameScriptFile(renamedScriptPath);
					}
					catch (Exception e)
					{
						ApplicationMaster.Log.Error("Not able to add suffix (.bat/.sh) to the shell script filename"
							, e);
						// We know we cannot continue launching the container
						// so we should release it.
						this._enclosing.numCompletedContainers.IncrementAndGet();
						this._enclosing.numFailedContainers.IncrementAndGet();
						return;
					}
					URL yarnUrl = null;
					try
					{
						yarnUrl = ConverterUtils.GetYarnUrlFromURI(new URI(renamedScriptPath.ToString()));
					}
					catch (URISyntaxException e)
					{
						ApplicationMaster.Log.Error("Error when trying to use shell script path specified"
							 + " in env, path=" + renamedScriptPath, e);
						// A failure scenario on bad input such as invalid shell script path
						// We know we cannot continue launching the container
						// so we should release it.
						// TODO
						this._enclosing.numCompletedContainers.IncrementAndGet();
						this._enclosing.numFailedContainers.IncrementAndGet();
						return;
					}
					LocalResource shellRsrc = LocalResource.NewInstance(yarnUrl, LocalResourceType.File
						, LocalResourceVisibility.Application, this._enclosing.shellScriptPathLen, this.
						_enclosing.shellScriptPathTimestamp);
					localResources[Shell.Windows ? ApplicationMaster.ExecBatScripStringtPath : ApplicationMaster
						.ExecShellStringPath] = shellRsrc;
					this._enclosing.shellCommand = Shell.Windows ? this._enclosing.windows_command : 
						this._enclosing.linux_bash_command;
				}
				// Set the necessary command to execute on the allocated container
				Vector<CharSequence> vargs = new Vector<CharSequence>(5);
				// Set executable command
				vargs.AddItem(this._enclosing.shellCommand);
				// Set shell script path
				if (!this._enclosing.scriptPath.IsEmpty())
				{
					vargs.AddItem(Shell.Windows ? ApplicationMaster.ExecBatScripStringtPath : ApplicationMaster
						.ExecShellStringPath);
				}
				// Set args for the shell command if any
				vargs.AddItem(this._enclosing.shellArgs);
				// Add log redirect params
				vargs.AddItem("1>" + ApplicationConstants.LogDirExpansionVar + "/stdout");
				vargs.AddItem("2>" + ApplicationConstants.LogDirExpansionVar + "/stderr");
				// Get final commmand
				StringBuilder command = new StringBuilder();
				foreach (CharSequence str in vargs)
				{
					command.Append(str).Append(" ");
				}
				IList<string> commands = new AList<string>();
				commands.AddItem(command.ToString());
				// Set up ContainerLaunchContext, setting local resource, environment,
				// command and token for constructor.
				// Note for tokens: Set up tokens for the container too. Today, for normal
				// shell commands, the container in distribute-shell doesn't need any
				// tokens. We are populating them mainly for NodeManagers to be able to
				// download anyfiles in the distributed file-system. The tokens are
				// otherwise also useful in cases, for e.g., when one is running a
				// "hadoop dfs" command inside the distributed shell.
				ContainerLaunchContext ctx = ContainerLaunchContext.NewInstance(localResources, this
					._enclosing.shellEnv, commands, null, this._enclosing.allTokens.Duplicate(), null
					);
				this.containerListener.AddContainer(this.container.GetId(), this.container);
				this._enclosing.nmClientAsync.StartContainerAsync(this.container, ctx);
			}

			private readonly ApplicationMaster _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void RenameScriptFile(Path renamedScriptPath)
		{
			appSubmitterUgi.DoAs(new _PrivilegedExceptionAction_1032(this, renamedScriptPath)
				);
			Log.Info("User " + appSubmitterUgi.GetUserName() + " added suffix(.sh/.bat) to script file as "
				 + renamedScriptPath);
		}

		private sealed class _PrivilegedExceptionAction_1032 : PrivilegedExceptionAction<
			Void>
		{
			public _PrivilegedExceptionAction_1032(ApplicationMaster _enclosing, Path renamedScriptPath
				)
			{
				this._enclosing = _enclosing;
				this.renamedScriptPath = renamedScriptPath;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Run()
			{
				FileSystem fs = renamedScriptPath.GetFileSystem(this._enclosing.conf);
				fs.Rename(new Path(this._enclosing.scriptPath), renamedScriptPath);
				return null;
			}

			private readonly ApplicationMaster _enclosing;

			private readonly Path renamedScriptPath;
		}

		/// <summary>Setup the request that will be sent to the RM for the container ask.</summary>
		/// <returns>the setup ResourceRequest to be sent to RM</returns>
		private AMRMClient.ContainerRequest SetupContainerAskForRM()
		{
			// setup requirements for hosts
			// using * as any host will do for the distributed shell app
			// set the priority for the request
			// TODO - what is the range for priority? how to decide?
			Priority pri = Priority.NewInstance(requestPriority);
			// Set up resource type requirements
			// For now, memory and CPU are supported so we set memory and cpu requirements
			Resource capability = Resource.NewInstance(containerMemory, containerVirtualCores
				);
			AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, 
				null, null, pri);
			Log.Info("Requested container ask: " + request.ToString());
			return request;
		}

		private bool FileExist(string filePath)
		{
			return new FilePath(filePath).Exists();
		}

		/// <exception cref="System.IO.IOException"/>
		private string ReadContent(string filePath)
		{
			DataInputStream ds = null;
			try
			{
				ds = new DataInputStream(new FileInputStream(filePath));
				return ds.ReadUTF();
			}
			finally
			{
				IOUtils.CloseQuietly(ds);
			}
		}

		private static void PublishContainerStartEvent(TimelineClient timelineClient, Container
			 container, string domainId, UserGroupInformation ugi)
		{
			TimelineEntity entity = new TimelineEntity();
			entity.SetEntityId(container.GetId().ToString());
			entity.SetEntityType(ApplicationMaster.DSEntity.DsContainer.ToString());
			entity.SetDomainId(domainId);
			entity.AddPrimaryFilter("user", ugi.GetShortUserName());
			TimelineEvent @event = new TimelineEvent();
			@event.SetTimestamp(Runtime.CurrentTimeMillis());
			@event.SetEventType(ApplicationMaster.DSEvent.DsContainerStart.ToString());
			@event.AddEventInfo("Node", container.GetNodeId().ToString());
			@event.AddEventInfo("Resources", container.GetResource().ToString());
			entity.AddEvent(@event);
			try
			{
				ugi.DoAs(new _PrivilegedExceptionAction_1097(timelineClient, entity));
			}
			catch (Exception e)
			{
				Log.Error("Container start event could not be published for " + container.GetId()
					.ToString(), e is UndeclaredThrowableException ? e.InnerException : e);
			}
		}

		private sealed class _PrivilegedExceptionAction_1097 : PrivilegedExceptionAction<
			TimelinePutResponse>
		{
			public _PrivilegedExceptionAction_1097(TimelineClient timelineClient, TimelineEntity
				 entity)
			{
				this.timelineClient = timelineClient;
				this.entity = entity;
			}

			/// <exception cref="System.Exception"/>
			public TimelinePutResponse Run()
			{
				return timelineClient.PutEntities(entity);
			}

			private readonly TimelineClient timelineClient;

			private readonly TimelineEntity entity;
		}

		private static void PublishContainerEndEvent(TimelineClient timelineClient, ContainerStatus
			 container, string domainId, UserGroupInformation ugi)
		{
			TimelineEntity entity = new TimelineEntity();
			entity.SetEntityId(container.GetContainerId().ToString());
			entity.SetEntityType(ApplicationMaster.DSEntity.DsContainer.ToString());
			entity.SetDomainId(domainId);
			entity.AddPrimaryFilter("user", ugi.GetShortUserName());
			TimelineEvent @event = new TimelineEvent();
			@event.SetTimestamp(Runtime.CurrentTimeMillis());
			@event.SetEventType(ApplicationMaster.DSEvent.DsContainerEnd.ToString());
			@event.AddEventInfo("State", container.GetState().ToString());
			@event.AddEventInfo("Exit Status", container.GetExitStatus());
			entity.AddEvent(@event);
			try
			{
				timelineClient.PutEntities(entity);
			}
			catch (Exception e)
			{
				Log.Error("Container end event could not be published for " + container.GetContainerId
					().ToString(), e);
			}
		}

		private static void PublishApplicationAttemptEvent(TimelineClient timelineClient, 
			string appAttemptId, ApplicationMaster.DSEvent appEvent, string domainId, UserGroupInformation
			 ugi)
		{
			TimelineEntity entity = new TimelineEntity();
			entity.SetEntityId(appAttemptId);
			entity.SetEntityType(ApplicationMaster.DSEntity.DsAppAttempt.ToString());
			entity.SetDomainId(domainId);
			entity.AddPrimaryFilter("user", ugi.GetShortUserName());
			TimelineEvent @event = new TimelineEvent();
			@event.SetEventType(appEvent.ToString());
			@event.SetTimestamp(Runtime.CurrentTimeMillis());
			entity.AddEvent(@event);
			try
			{
				timelineClient.PutEntities(entity);
			}
			catch (Exception e)
			{
				Log.Error("App Attempt " + (appEvent.Equals(ApplicationMaster.DSEvent.DsAppAttemptStart
					) ? "start" : "end") + " event could not be published for " + appAttemptId.ToString
					(), e);
			}
		}
	}
}
