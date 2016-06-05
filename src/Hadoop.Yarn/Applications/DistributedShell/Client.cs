using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Applications.Distributedshell
{
	/// <summary>Client for Distributed Shell application submission to YARN.</summary>
	/// <remarks>
	/// Client for Distributed Shell application submission to YARN.
	/// <p> The distributed shell client allows an application master to be launched that in turn would run
	/// the provided shell command on a set of containers. </p>
	/// <p>This client is meant to act as an example on how to write yarn-based applications. </p>
	/// <p> To submit an application, a client first needs to connect to the <code>ResourceManager</code>
	/// aka ApplicationsManager or ASM via the
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol"/>
	/// . The
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol"/>
	/// 
	/// provides a way for the client to get access to cluster information and to request for a
	/// new
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
	/// . <p>
	/// <p> For the actual job submission, the client first has to create an
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
	/// .
	/// The
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
	/// defines the application details such as
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
	/// 
	/// and application name, the priority assigned to the application and the queue
	/// to which this application needs to be assigned. In addition to this, the
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
	/// also defines the
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerLaunchContext"/>
	/// which describes the <code>Container</code> with which
	/// the
	/// <see cref="ApplicationMaster"/>
	/// is launched. </p>
	/// <p> The
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerLaunchContext"/>
	/// in this scenario defines the resources to be allocated for the
	/// <see cref="ApplicationMaster"/>
	/// 's container, the local resources (jars, configuration files) to be made available
	/// and the environment to be set for the
	/// <see cref="ApplicationMaster"/>
	/// and the commands to be executed to run the
	/// <see cref="ApplicationMaster"/>
	/// . <p>
	/// <p> Using the
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
	/// , the client submits the application to the
	/// <code>ResourceManager</code> and then monitors the application by requesting the <code>ResourceManager</code>
	/// for an
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationReport"/>
	/// at regular time intervals. In case of the application taking too long, the client
	/// kills the application by submitting a
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.KillApplicationRequest"/>
	/// to the <code>ResourceManager</code>. </p>
	/// </remarks>
	public class Client
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
			));

		private Configuration conf;

		private YarnClient yarnClient;

		private string appName = string.Empty;

		private int amPriority = 0;

		private string amQueue = string.Empty;

		private int amMemory = 10;

		private int amVCores = 1;

		private string appMasterJar = string.Empty;

		private readonly string appMasterMainClass;

		private string shellCommand = string.Empty;

		private string shellScriptPath = string.Empty;

		private string[] shellArgs = new string[] {  };

		private IDictionary<string, string> shellEnv = new Dictionary<string, string>();

		private int shellCmdPriority = 0;

		private int containerMemory = 10;

		private int containerVirtualCores = 1;

		private int numContainers = 1;

		private string nodeLabelExpression = null;

		private string log4jPropFile = string.Empty;

		private readonly long clientStartTime = Runtime.CurrentTimeMillis();

		private long clientTimeout = 600000;

		private bool keepContainers = false;

		private long attemptFailuresValidityInterval = -1;

		internal bool debugFlag = false;

		private string domainId = null;

		private bool toCreateDomain = false;

		private string viewACLs = null;

		private string modifyACLs = null;

		private Options opts;

		private const string shellCommandPath = "shellCommands";

		private const string shellArgsPath = "shellArgs";

		private const string appMasterJarPath = "AppMaster.jar";

		private const string log4jPath = "log4j.properties";

		public const string ScriptPath = "ExecScript";

		// Configuration
		// Application master specific info to register a new Application with RM/ASM
		// App master priority
		// Queue for App master
		// Amt. of memory resource to request for to run the App Master
		// Amt. of virtual core resource to request for to run the App Master
		// Application master jar file
		// Main class to invoke application master
		// Shell command to be executed 
		// Location of shell script 
		// Args to be passed to the shell command
		// Env variables to be setup for the shell command 
		// Shell Command Container priority 
		// Amt of memory to request for container in which shell script will be executed
		// Amt. of virtual cores to request for container in which shell script will be executed
		// No. of containers in which the shell script needs to be executed
		// log4j.properties file 
		// if available, add to local resources and set into classpath 
		// Start time for client
		// Timeout threshold for client. Kill app after time interval expires.
		// flag to indicate whether to keep containers across application attempts.
		// Debug flag
		// Timeline domain ID
		// Flag to indicate whether to create the domain of the given ID
		// Timeline domain reader access control
		// Timeline domain writer access control
		// Command line options
		// Hardcoded path to custom log_properties
		/// <param name="args">Command line arguments</param>
		public static void Main(string[] args)
		{
			bool result = false;
			try
			{
				Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client client = new Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
					();
				Log.Info("Initializing Client");
				try
				{
					bool doRun = client.Init(args);
					if (!doRun)
					{
						System.Environment.Exit(0);
					}
				}
				catch (ArgumentException e)
				{
					System.Console.Error.WriteLine(e.GetLocalizedMessage());
					client.PrintUsage();
					System.Environment.Exit(-1);
				}
				result = client.Run();
			}
			catch (Exception t)
			{
				Log.Fatal("Error running Client", t);
				System.Environment.Exit(1);
			}
			if (result)
			{
				Log.Info("Application completed successfully");
				System.Environment.Exit(0);
			}
			Log.Error("Application failed to complete successfully");
			System.Environment.Exit(2);
		}

		/// <exception cref="System.Exception"/>
		public Client(Configuration conf)
			: this("org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster", 
				conf)
		{
		}

		internal Client(string appMasterMainClass, Configuration conf)
		{
			this.conf = conf;
			this.appMasterMainClass = appMasterMainClass;
			yarnClient = YarnClient.CreateYarnClient();
			yarnClient.Init(conf);
			opts = new Options();
			opts.AddOption("appname", true, "Application Name. Default value - DistributedShell"
				);
			opts.AddOption("priority", true, "Application Priority. Default 0");
			opts.AddOption("queue", true, "RM Queue in which this application is to be submitted"
				);
			opts.AddOption("timeout", true, "Application timeout in milliseconds");
			opts.AddOption("master_memory", true, "Amount of memory in MB to be requested to run the application master"
				);
			opts.AddOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master"
				);
			opts.AddOption("jar", true, "Jar file containing the application master");
			opts.AddOption("shell_command", true, "Shell command to be executed by " + "the Application Master. Can only specify either --shell_command "
				 + "or --shell_script");
			opts.AddOption("shell_script", true, "Location of the shell script to be " + "executed. Can only specify either --shell_command or --shell_script"
				);
			opts.AddOption("shell_args", true, "Command line args for the shell script." + "Multiple args can be separated by empty space."
				);
			opts.GetOption("shell_args").SetArgs(Option.UnlimitedValues);
			opts.AddOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs"
				);
			opts.AddOption("shell_cmd_priority", true, "Priority for the shell command containers"
				);
			opts.AddOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command"
				);
			opts.AddOption("container_vcores", true, "Amount of virtual cores to be requested to run the shell command"
				);
			opts.AddOption("num_containers", true, "No. of containers on which the shell command needs to be executed"
				);
			opts.AddOption("log_properties", true, "log4j.properties file");
			opts.AddOption("keep_containers_across_application_attempts", false, "Flag to indicate whether to keep containers across application attempts."
				 + " If the flag is true, running containers will not be killed when" + " application attempt fails and these containers will be retrieved by"
				 + " the new application attempt ");
			opts.AddOption("attempt_failures_validity_interval", true, "when attempt_failures_validity_interval in milliseconds is set to > 0,"
				 + "the failure number will not take failures which happen out of " + "the validityInterval into failure count. "
				 + "If failure count reaches to maxAppAttempts, " + "the application will be failed."
				);
			opts.AddOption("debug", false, "Dump out debug information");
			opts.AddOption("domain", true, "ID of the timeline domain where the " + "timeline entities will be put"
				);
			opts.AddOption("view_acls", true, "Users and groups that allowed to " + "view the timeline entities in the given domain"
				);
			opts.AddOption("modify_acls", true, "Users and groups that allowed to " + "modify the timeline entities in the given domain"
				);
			opts.AddOption("create", false, "Flag to indicate whether to create the " + "domain specified with -domain."
				);
			opts.AddOption("help", false, "Print usage");
			opts.AddOption("node_label_expression", true, "Node label expression to determine the nodes"
				 + " where all the containers of this application" + " will be allocated, \"\" means containers"
				 + " can be allocated anywhere, if you don't specify the option," + " default node_label_expression of queue will be used."
				);
		}

		/// <exception cref="System.Exception"/>
		public Client()
			: this(new YarnConfiguration())
		{
		}

		/// <summary>Helper function to print out usage</summary>
		private void PrintUsage()
		{
			new HelpFormatter().PrintHelp("Client", opts);
		}

		/// <summary>Parse command line options</summary>
		/// <param name="args">Parsed command line options</param>
		/// <returns>Whether the init was successful to run the client</returns>
		/// <exception cref="Org.Apache.Commons.Cli.ParseException"/>
		public virtual bool Init(string[] args)
		{
			CommandLine cliParser = new GnuParser().Parse(opts, args);
			if (args.Length == 0)
			{
				throw new ArgumentException("No args specified for client to initialize");
			}
			if (cliParser.HasOption("log_properties"))
			{
				string log4jPath = cliParser.GetOptionValue("log_properties");
				try
				{
					Log4jPropertyHelper.UpdateLog4jConfiguration(typeof(Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
						), log4jPath);
				}
				catch (Exception e)
				{
					Log.Warn("Can not set up custom log4j properties. " + e);
				}
			}
			if (cliParser.HasOption("help"))
			{
				PrintUsage();
				return false;
			}
			if (cliParser.HasOption("debug"))
			{
				debugFlag = true;
			}
			if (cliParser.HasOption("keep_containers_across_application_attempts"))
			{
				Log.Info("keep_containers_across_application_attempts");
				keepContainers = true;
			}
			appName = cliParser.GetOptionValue("appname", "DistributedShell");
			amPriority = System.Convert.ToInt32(cliParser.GetOptionValue("priority", "0"));
			amQueue = cliParser.GetOptionValue("queue", "default");
			amMemory = System.Convert.ToInt32(cliParser.GetOptionValue("master_memory", "10")
				);
			amVCores = System.Convert.ToInt32(cliParser.GetOptionValue("master_vcores", "1"));
			if (amMemory < 0)
			{
				throw new ArgumentException("Invalid memory specified for application master, exiting."
					 + " Specified memory=" + amMemory);
			}
			if (amVCores < 0)
			{
				throw new ArgumentException("Invalid virtual cores specified for application master, exiting."
					 + " Specified virtual cores=" + amVCores);
			}
			if (!cliParser.HasOption("jar"))
			{
				throw new ArgumentException("No jar file specified for application master");
			}
			appMasterJar = cliParser.GetOptionValue("jar");
			if (!cliParser.HasOption("shell_command") && !cliParser.HasOption("shell_script"))
			{
				throw new ArgumentException("No shell command or shell script specified to be executed by application master"
					);
			}
			else
			{
				if (cliParser.HasOption("shell_command") && cliParser.HasOption("shell_script"))
				{
					throw new ArgumentException("Can not specify shell_command option " + "and shell_script option at the same time"
						);
				}
				else
				{
					if (cliParser.HasOption("shell_command"))
					{
						shellCommand = cliParser.GetOptionValue("shell_command");
					}
					else
					{
						shellScriptPath = cliParser.GetOptionValue("shell_script");
					}
				}
			}
			if (cliParser.HasOption("shell_args"))
			{
				shellArgs = cliParser.GetOptionValues("shell_args");
			}
			if (cliParser.HasOption("shell_env"))
			{
				string[] envs = cliParser.GetOptionValues("shell_env");
				foreach (string env in envs)
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
			shellCmdPriority = System.Convert.ToInt32(cliParser.GetOptionValue("shell_cmd_priority"
				, "0"));
			containerMemory = System.Convert.ToInt32(cliParser.GetOptionValue("container_memory"
				, "10"));
			containerVirtualCores = System.Convert.ToInt32(cliParser.GetOptionValue("container_vcores"
				, "1"));
			numContainers = System.Convert.ToInt32(cliParser.GetOptionValue("num_containers", 
				"1"));
			if (containerMemory < 0 || containerVirtualCores < 0 || numContainers < 1)
			{
				throw new ArgumentException("Invalid no. of containers or container memory/vcores specified,"
					 + " exiting." + " Specified containerMemory=" + containerMemory + ", containerVirtualCores="
					 + containerVirtualCores + ", numContainer=" + numContainers);
			}
			nodeLabelExpression = cliParser.GetOptionValue("node_label_expression", null);
			clientTimeout = System.Convert.ToInt32(cliParser.GetOptionValue("timeout", "600000"
				));
			attemptFailuresValidityInterval = long.Parse(cliParser.GetOptionValue("attempt_failures_validity_interval"
				, "-1"));
			log4jPropFile = cliParser.GetOptionValue("log_properties", string.Empty);
			// Get timeline domain options
			if (cliParser.HasOption("domain"))
			{
				domainId = cliParser.GetOptionValue("domain");
				toCreateDomain = cliParser.HasOption("create");
				if (cliParser.HasOption("view_acls"))
				{
					viewACLs = cliParser.GetOptionValue("view_acls");
				}
				if (cliParser.HasOption("modify_acls"))
				{
					modifyACLs = cliParser.GetOptionValue("modify_acls");
				}
			}
			return true;
		}

		/// <summary>Main run function for the client</summary>
		/// <returns>true if application completed successfully</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual bool Run()
		{
			Log.Info("Running Client");
			yarnClient.Start();
			YarnClusterMetrics clusterMetrics = yarnClient.GetYarnClusterMetrics();
			Log.Info("Got Cluster metric info from ASM" + ", numNodeManagers=" + clusterMetrics
				.GetNumNodeManagers());
			IList<NodeReport> clusterNodeReports = yarnClient.GetNodeReports(NodeState.Running
				);
			Log.Info("Got Cluster node info from ASM");
			foreach (NodeReport node in clusterNodeReports)
			{
				Log.Info("Got node report from ASM for" + ", nodeId=" + node.GetNodeId() + ", nodeAddress"
					 + node.GetHttpAddress() + ", nodeRackName" + node.GetRackName() + ", nodeNumContainers"
					 + node.GetNumContainers());
			}
			QueueInfo queueInfo = yarnClient.GetQueueInfo(this.amQueue);
			Log.Info("Queue info" + ", queueName=" + queueInfo.GetQueueName() + ", queueCurrentCapacity="
				 + queueInfo.GetCurrentCapacity() + ", queueMaxCapacity=" + queueInfo.GetMaximumCapacity
				() + ", queueApplicationCount=" + queueInfo.GetApplications().Count + ", queueChildQueueCount="
				 + queueInfo.GetChildQueues().Count);
			IList<QueueUserACLInfo> listAclInfo = yarnClient.GetQueueAclsInfo();
			foreach (QueueUserACLInfo aclInfo in listAclInfo)
			{
				foreach (QueueACL userAcl in aclInfo.GetUserAcls())
				{
					Log.Info("User ACL Info for Queue" + ", queueName=" + aclInfo.GetQueueName() + ", userAcl="
						 + userAcl.ToString());
				}
			}
			if (domainId != null && domainId.Length > 0 && toCreateDomain)
			{
				PrepareTimelineDomain();
			}
			// Get a new application id
			YarnClientApplication app = yarnClient.CreateApplication();
			GetNewApplicationResponse appResponse = app.GetNewApplicationResponse();
			// TODO get min/max resource capabilities from RM and change memory ask if needed
			// If we do not have min/max, we may not be able to correctly request 
			// the required resources from the RM for the app master
			// Memory ask has to be a multiple of min and less than max. 
			// Dump out information about cluster capability as seen by the resource manager
			int maxMem = appResponse.GetMaximumResourceCapability().GetMemory();
			Log.Info("Max mem capabililty of resources in this cluster " + maxMem);
			// A resource ask cannot exceed the max. 
			if (amMemory > maxMem)
			{
				Log.Info("AM memory specified above max threshold of cluster. Using max value." +
					 ", specified=" + amMemory + ", max=" + maxMem);
				amMemory = maxMem;
			}
			int maxVCores = appResponse.GetMaximumResourceCapability().GetVirtualCores();
			Log.Info("Max virtual cores capabililty of resources in this cluster " + maxVCores
				);
			if (amVCores > maxVCores)
			{
				Log.Info("AM virtual cores specified above max threshold of cluster. " + "Using max value."
					 + ", specified=" + amVCores + ", max=" + maxVCores);
				amVCores = maxVCores;
			}
			// set the application name
			ApplicationSubmissionContext appContext = app.GetApplicationSubmissionContext();
			ApplicationId appId = appContext.GetApplicationId();
			appContext.SetKeepContainersAcrossApplicationAttempts(keepContainers);
			appContext.SetApplicationName(appName);
			if (attemptFailuresValidityInterval >= 0)
			{
				appContext.SetAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
			}
			// set local resources for the application master
			// local files or archives as needed
			// In this scenario, the jar file for the application master is part of the local resources			
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			Log.Info("Copy App Master jar from local filesystem and add to local environment"
				);
			// Copy the application master jar to the filesystem 
			// Create a local resource to point to the destination jar path 
			FileSystem fs = FileSystem.Get(conf);
			AddToLocalResources(fs, appMasterJar, appMasterJarPath, appId.ToString(), localResources
				, null);
			// Set the log4j properties if needed 
			if (!log4jPropFile.IsEmpty())
			{
				AddToLocalResources(fs, log4jPropFile, log4jPath, appId.ToString(), localResources
					, null);
			}
			// The shell script has to be made available on the final container(s)
			// where it will be executed. 
			// To do this, we need to first copy into the filesystem that is visible 
			// to the yarn framework. 
			// We do not need to set this as a local resource for the application 
			// master as the application master does not need it. 		
			string hdfsShellScriptLocation = string.Empty;
			long hdfsShellScriptLen = 0;
			long hdfsShellScriptTimestamp = 0;
			if (!shellScriptPath.IsEmpty())
			{
				Path shellSrc = new Path(shellScriptPath);
				string shellPathSuffix = appName + "/" + appId.ToString() + "/" + ScriptPath;
				Path shellDst = new Path(fs.GetHomeDirectory(), shellPathSuffix);
				fs.CopyFromLocalFile(false, true, shellSrc, shellDst);
				hdfsShellScriptLocation = shellDst.ToUri().ToString();
				FileStatus shellFileStatus = fs.GetFileStatus(shellDst);
				hdfsShellScriptLen = shellFileStatus.GetLen();
				hdfsShellScriptTimestamp = shellFileStatus.GetModificationTime();
			}
			if (!shellCommand.IsEmpty())
			{
				AddToLocalResources(fs, null, shellCommandPath, appId.ToString(), localResources, 
					shellCommand);
			}
			if (shellArgs.Length > 0)
			{
				AddToLocalResources(fs, null, shellArgsPath, appId.ToString(), localResources, StringUtils
					.Join(shellArgs, " "));
			}
			// Set the necessary security tokens as needed
			//amContainer.setContainerTokens(containerToken);
			// Set the env variables to be setup in the env where the application master will be run
			Log.Info("Set the environment for the application master");
			IDictionary<string, string> env = new Dictionary<string, string>();
			// put location of shell script into env
			// using the env info, the application master will create the correct local resource for the 
			// eventual containers that will be launched to execute the shell scripts
			env[DSConstants.Distributedshellscriptlocation] = hdfsShellScriptLocation;
			env[DSConstants.Distributedshellscripttimestamp] = System.Convert.ToString(hdfsShellScriptTimestamp
				);
			env[DSConstants.Distributedshellscriptlen] = System.Convert.ToString(hdfsShellScriptLen
				);
			if (domainId != null && domainId.Length > 0)
			{
				env[DSConstants.Distributedshelltimelinedomain] = domainId;
			}
			// Add AppMaster.jar location to classpath 		
			// At some point we should not be required to add 
			// the hadoop specific classpaths to the env. 
			// It should be provided out of the box. 
			// For now setting all required classpaths including
			// the classpath to "." for the application jar
			StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.Classpath
				.$$()).Append(ApplicationConstants.ClassPathSeparator).Append("./*");
			foreach (string c in conf.GetStrings(YarnConfiguration.YarnApplicationClasspath, 
				YarnConfiguration.DefaultYarnCrossPlatformApplicationClasspath))
			{
				classPathEnv.Append(ApplicationConstants.ClassPathSeparator);
				classPathEnv.Append(c.Trim());
			}
			classPathEnv.Append(ApplicationConstants.ClassPathSeparator).Append("./log4j.properties"
				);
			// add the runtime classpath needed for tests to work
			if (conf.GetBoolean(YarnConfiguration.IsMiniYarnCluster, false))
			{
				classPathEnv.Append(':');
				classPathEnv.Append(Runtime.GetProperty("java.class.path"));
			}
			env["CLASSPATH"] = classPathEnv.ToString();
			// Set the necessary command to execute the application master 
			Vector<CharSequence> vargs = new Vector<CharSequence>(30);
			// Set java executable command 
			Log.Info("Setting up app master command");
			vargs.AddItem(ApplicationConstants.Environment.JavaHome.$$() + "/bin/java");
			// Set Xmx based on am memory size
			vargs.AddItem("-Xmx" + amMemory + "m");
			// Set class name 
			vargs.AddItem(appMasterMainClass);
			// Set params for Application Master
			vargs.AddItem("--container_memory " + containerMemory.ToString());
			vargs.AddItem("--container_vcores " + containerVirtualCores.ToString());
			vargs.AddItem("--num_containers " + numContainers.ToString());
			if (null != nodeLabelExpression)
			{
				appContext.SetNodeLabelExpression(nodeLabelExpression);
			}
			vargs.AddItem("--priority " + shellCmdPriority.ToString());
			foreach (KeyValuePair<string, string> entry in shellEnv)
			{
				vargs.AddItem("--shell_env " + entry.Key + "=" + entry.Value);
			}
			if (debugFlag)
			{
				vargs.AddItem("--debug");
			}
			vargs.AddItem("1>" + ApplicationConstants.LogDirExpansionVar + "/AppMaster.stdout"
				);
			vargs.AddItem("2>" + ApplicationConstants.LogDirExpansionVar + "/AppMaster.stderr"
				);
			// Get final commmand
			StringBuilder command = new StringBuilder();
			foreach (CharSequence str in vargs)
			{
				command.Append(str).Append(" ");
			}
			Log.Info("Completed setting up app master command " + command.ToString());
			IList<string> commands = new AList<string>();
			commands.AddItem(command.ToString());
			// Set up the container launch context for the application master
			ContainerLaunchContext amContainer = ContainerLaunchContext.NewInstance(localResources
				, env, commands, null, null, null);
			// Set up resource type requirements
			// For now, both memory and vcores are supported, so we set memory and 
			// vcores requirements
			Resource capability = Resource.NewInstance(amMemory, amVCores);
			appContext.SetResource(capability);
			// Service data is a binary blob that can be passed to the application
			// Not needed in this scenario
			// amContainer.setServiceData(serviceData);
			// Setup security tokens
			if (UserGroupInformation.IsSecurityEnabled())
			{
				// Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
				Credentials credentials = new Credentials();
				string tokenRenewer = conf.Get(YarnConfiguration.RmPrincipal);
				if (tokenRenewer == null || tokenRenewer.Length == 0)
				{
					throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer"
						);
				}
				// For now, only getting tokens for the default file-system.
				Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = fs.AddDelegationTokens(
					tokenRenewer, credentials);
				if (tokens != null)
				{
					foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in tokens)
					{
						Log.Info("Got dt for " + fs.GetUri() + "; " + token);
					}
				}
				DataOutputBuffer dob = new DataOutputBuffer();
				credentials.WriteTokenStorageToStream(dob);
				ByteBuffer fsTokens = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
				amContainer.SetTokens(fsTokens);
			}
			appContext.SetAMContainerSpec(amContainer);
			// Set the priority for the application master
			// TODO - what is the range for priority? how to decide? 
			Priority pri = Priority.NewInstance(amPriority);
			appContext.SetPriority(pri);
			// Set the queue to which this application is to be submitted in the RM
			appContext.SetQueue(amQueue);
			// Submit the application to the applications manager
			// SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
			// Ignore the response as either a valid response object is returned on success 
			// or an exception thrown to denote some form of a failure
			Log.Info("Submitting application to ASM");
			yarnClient.SubmitApplication(appContext);
			// TODO
			// Try submitting the same request again
			// app submission failure?
			// Monitor the application
			return MonitorApplication(appId);
		}

		/// <summary>Monitor the submitted application for completion.</summary>
		/// <remarks>
		/// Monitor the submitted application for completion.
		/// Kill application if time expires.
		/// </remarks>
		/// <param name="appId">Application Id of application to be monitored</param>
		/// <returns>true if application completed successfully</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private bool MonitorApplication(ApplicationId appId)
		{
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
				ApplicationReport report = yarnClient.GetApplicationReport(appId);
				Log.Info("Got application report from ASM for" + ", appId=" + appId.GetId() + ", clientToAMToken="
					 + report.GetClientToAMToken() + ", appDiagnostics=" + report.GetDiagnostics() +
					 ", appMasterHost=" + report.GetHost() + ", appQueue=" + report.GetQueue() + ", appMasterRpcPort="
					 + report.GetRpcPort() + ", appStartTime=" + report.GetStartTime() + ", yarnAppState="
					 + report.GetYarnApplicationState().ToString() + ", distributedFinalState=" + report
					.GetFinalApplicationStatus().ToString() + ", appTrackingUrl=" + report.GetTrackingUrl
					() + ", appUser=" + report.GetUser());
				YarnApplicationState state = report.GetYarnApplicationState();
				FinalApplicationStatus dsStatus = report.GetFinalApplicationStatus();
				if (YarnApplicationState.Finished == state)
				{
					if (FinalApplicationStatus.Succeeded == dsStatus)
					{
						Log.Info("Application has completed successfully. Breaking monitoring loop");
						return true;
					}
					else
					{
						Log.Info("Application did finished unsuccessfully." + " YarnState=" + state.ToString
							() + ", DSFinalStatus=" + dsStatus.ToString() + ". Breaking monitoring loop");
						return false;
					}
				}
				else
				{
					if (YarnApplicationState.Killed == state || YarnApplicationState.Failed == state)
					{
						Log.Info("Application did not finish." + " YarnState=" + state.ToString() + ", DSFinalStatus="
							 + dsStatus.ToString() + ". Breaking monitoring loop");
						return false;
					}
				}
				if (Runtime.CurrentTimeMillis() > (clientStartTime + clientTimeout))
				{
					Log.Info("Reached client specified timeout for application. Killing application");
					ForceKillApplication(appId);
					return false;
				}
			}
		}

		/// <summary>Kill a submitted application by sending a call to the ASM</summary>
		/// <param name="appId">Application Id to be killed.</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void ForceKillApplication(ApplicationId appId)
		{
			// TODO clarify whether multiple jobs with the same app id can be submitted and be running at 
			// the same time. 
			// If yes, can we kill a particular attempt only?
			// Response can be ignored as it is non-null on success or 
			// throws an exception in case of failures
			yarnClient.KillApplication(appId);
		}

		/// <exception cref="System.IO.IOException"/>
		private void AddToLocalResources(FileSystem fs, string fileSrcPath, string fileDstPath
			, string appId, IDictionary<string, LocalResource> localResources, string resources
			)
		{
			string suffix = appName + "/" + appId + "/" + fileDstPath;
			Path dst = new Path(fs.GetHomeDirectory(), suffix);
			if (fileSrcPath == null)
			{
				FSDataOutputStream ostream = null;
				try
				{
					ostream = FileSystem.Create(fs, dst, new FsPermission((short)0x1c8));
					ostream.WriteUTF(resources);
				}
				finally
				{
					IOUtils.CloseQuietly(ostream);
				}
			}
			else
			{
				fs.CopyFromLocalFile(new Path(fileSrcPath), dst);
			}
			FileStatus scFileStatus = fs.GetFileStatus(dst);
			LocalResource scRsrc = LocalResource.NewInstance(ConverterUtils.GetYarnUrlFromURI
				(dst.ToUri()), LocalResourceType.File, LocalResourceVisibility.Application, scFileStatus
				.GetLen(), scFileStatus.GetModificationTime());
			localResources[fileDstPath] = scRsrc;
		}

		private void PrepareTimelineDomain()
		{
			TimelineClient timelineClient = null;
			if (conf.GetBoolean(YarnConfiguration.TimelineServiceEnabled, YarnConfiguration.DefaultTimelineServiceEnabled
				))
			{
				timelineClient = TimelineClient.CreateTimelineClient();
				timelineClient.Init(conf);
				timelineClient.Start();
			}
			else
			{
				Log.Warn("Cannot put the domain " + domainId + " because the timeline service is not enabled"
					);
				return;
			}
			try
			{
				//TODO: we need to check and combine the existing timeline domain ACLs,
				//but let's do it once we have client java library to query domains.
				TimelineDomain domain = new TimelineDomain();
				domain.SetId(domainId);
				domain.SetReaders(viewACLs != null && viewACLs.Length > 0 ? viewACLs : " ");
				domain.SetWriters(modifyACLs != null && modifyACLs.Length > 0 ? modifyACLs : " ");
				timelineClient.PutDomain(domain);
				Log.Info("Put the timeline domain: " + TimelineUtils.DumpTimelineRecordtoJSON(domain
					));
			}
			catch (Exception e)
			{
				Log.Error("Error when putting the timeline domain", e);
			}
			finally
			{
				timelineClient.Stop();
			}
		}
	}
}
