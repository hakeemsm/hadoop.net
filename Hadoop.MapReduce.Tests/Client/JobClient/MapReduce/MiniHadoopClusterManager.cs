using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server;
using Org.Mortbay.Util.Ajax;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>This class drives the creation of a mini-cluster on the local machine.</summary>
	/// <remarks>
	/// This class drives the creation of a mini-cluster on the local machine. By
	/// default, a MiniDFSCluster and MiniMRCluster are spawned on the first
	/// available ports that are found.
	/// A series of command line flags controls the startup cluster options.
	/// This class can dump a Hadoop configuration and some basic metadata (in JSON)
	/// into a text file.
	/// To shutdown the cluster, kill the process.
	/// </remarks>
	public class MiniHadoopClusterManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(MiniHadoopClusterManager
			));

		private MiniMRClientCluster mr;

		private MiniDFSCluster dfs;

		private string writeDetails;

		private int numNodeManagers;

		private int numDataNodes;

		private int nnPort;

		private int rmPort;

		private int jhsPort;

		private HdfsServerConstants.StartupOption dfsOpts;

		private bool noDFS;

		private bool noMR;

		private string fs;

		private string writeConfig;

		private JobConf conf;

		/// <summary>Creates configuration options object.</summary>
		private Options MakeOptions()
		{
			Options options = new Options();
			options.AddOption("nodfs", false, "Don't start a mini DFS cluster").AddOption("nomr"
				, false, "Don't start a mini MR cluster").AddOption("nodemanagers", true, "How many nodemanagers to start (default 1)"
				).AddOption("datanodes", true, "How many datanodes to start (default 1)").AddOption
				("format", false, "Format the DFS (default false)").AddOption("nnport", true, "NameNode port (default 0--we choose)"
				).AddOption("namenode", true, "URL of the namenode (default " + "is either the DFS cluster or a temporary dir)"
				).AddOption("rmport", true, "ResourceManager port (default 0--we choose)").AddOption
				("jhsport", true, "JobHistoryServer port (default 0--we choose)").AddOption(OptionBuilder
				.Create("D")).AddOption(OptionBuilder.Create("writeConfig")).AddOption(OptionBuilder
				.Create("writeDetails")).AddOption(OptionBuilder.Create("help"));
			return options;
		}

		/// <summary>Main entry-point.</summary>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Run(string[] args)
		{
			if (!ParseArguments(args))
			{
				return;
			}
			Start();
			SleepForever();
		}

		private void SleepForever()
		{
			while (true)
			{
				try
				{
					Sharpen.Thread.Sleep(1000 * 60);
				}
				catch (Exception)
				{
				}
			}
		}

		// nothing
		/// <summary>Starts DFS and MR clusters, as specified in member-variable options.</summary>
		/// <remarks>
		/// Starts DFS and MR clusters, as specified in member-variable options. Also
		/// writes out configuration and details, if requested.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public virtual void Start()
		{
			if (!noDFS)
			{
				dfs = new MiniDFSCluster.Builder(conf).NameNodePort(nnPort).NumDataNodes(numDataNodes
					).StartupOption(dfsOpts).Build();
				Log.Info("Started MiniDFSCluster -- namenode on port " + dfs.GetNameNodePort());
			}
			if (!noMR)
			{
				if (fs == null && dfs != null)
				{
					fs = dfs.GetFileSystem().GetUri().ToString();
				}
				else
				{
					if (fs == null)
					{
						fs = "file:///tmp/minimr-" + Runtime.NanoTime();
					}
				}
				FileSystem.SetDefaultUri(conf, new URI(fs));
				// Instruct the minicluster to use fixed ports, so user will know which
				// ports to use when communicating with the cluster.
				conf.SetBoolean(YarnConfiguration.YarnMiniclusterFixedPorts, true);
				conf.SetBoolean(JHAdminConfig.MrHistoryMiniclusterFixedPorts, true);
				conf.Set(YarnConfiguration.RmAddress, MiniYARNCluster.GetHostname() + ":" + this.
					rmPort);
				conf.Set(JHAdminConfig.MrHistoryAddress, MiniYARNCluster.GetHostname() + ":" + this
					.jhsPort);
				mr = MiniMRClientClusterFactory.Create(this.GetType(), numNodeManagers, conf);
				Log.Info("Started MiniMRCluster");
			}
			if (writeConfig != null)
			{
				FileOutputStream fos = new FileOutputStream(new FilePath(writeConfig));
				conf.WriteXml(fos);
				fos.Close();
			}
			if (writeDetails != null)
			{
				IDictionary<string, object> map = new SortedDictionary<string, object>();
				if (dfs != null)
				{
					map["namenode_port"] = dfs.GetNameNodePort();
				}
				if (mr != null)
				{
					map["resourcemanager_port"] = mr.GetConfig().Get(YarnConfiguration.RmAddress).Split
						(":")[1];
				}
				FileWriter fw = new FileWriter(new FilePath(writeDetails));
				fw.Write(new JSON().ToJSON(map));
				fw.Close();
			}
		}

		/// <summary>Shuts down in-process clusters.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Stop()
		{
			if (mr != null)
			{
				mr.Stop();
			}
			if (dfs != null)
			{
				dfs.Shutdown();
			}
		}

		/// <summary>Parses arguments and fills out the member variables.</summary>
		/// <param name="args">Command-line arguments.</param>
		/// <returns>
		/// true on successful parse; false to indicate that the program should
		/// exit.
		/// </returns>
		private bool ParseArguments(string[] args)
		{
			Options options = MakeOptions();
			CommandLine cli;
			try
			{
				CommandLineParser parser = new GnuParser();
				cli = parser.Parse(options, args);
			}
			catch (ParseException e)
			{
				Log.Warn("options parsing failed:  " + e.Message);
				new HelpFormatter().PrintHelp("...", options);
				return false;
			}
			if (cli.HasOption("help"))
			{
				new HelpFormatter().PrintHelp("...", options);
				return false;
			}
			if (cli.GetArgs().Length > 0)
			{
				foreach (string arg in cli.GetArgs())
				{
					System.Console.Error.WriteLine("Unrecognized option: " + arg);
					new HelpFormatter().PrintHelp("...", options);
					return false;
				}
			}
			// MR
			noMR = cli.HasOption("nomr");
			numNodeManagers = IntArgument(cli, "nodemanagers", 1);
			rmPort = IntArgument(cli, "rmport", 0);
			jhsPort = IntArgument(cli, "jhsport", 0);
			fs = cli.GetOptionValue("namenode");
			// HDFS
			noDFS = cli.HasOption("nodfs");
			numDataNodes = IntArgument(cli, "datanodes", 1);
			nnPort = IntArgument(cli, "nnport", 0);
			dfsOpts = cli.HasOption("format") ? HdfsServerConstants.StartupOption.Format : HdfsServerConstants.StartupOption
				.Regular;
			// Runner
			writeDetails = cli.GetOptionValue("writeDetails");
			writeConfig = cli.GetOptionValue("writeConfig");
			// General
			conf = new JobConf();
			UpdateConfiguration(conf, cli.GetOptionValues("D"));
			return true;
		}

		/// <summary>Updates configuration based on what's given on the command line.</summary>
		/// <param name="conf">The configuration object</param>
		/// <param name="keyvalues">An array of interleaved key value pairs.</param>
		private void UpdateConfiguration(JobConf conf, string[] keyvalues)
		{
			int num_confs_updated = 0;
			if (keyvalues != null)
			{
				foreach (string prop in keyvalues)
				{
					string[] keyval = prop.Split("=", 2);
					if (keyval.Length == 2)
					{
						conf.Set(keyval[0], keyval[1]);
						num_confs_updated++;
					}
					else
					{
						Log.Warn("Ignoring -D option " + prop);
					}
				}
			}
			Log.Info("Updated " + num_confs_updated + " configuration settings from command line."
				);
		}

		/// <summary>Extracts an integer argument with specified default value.</summary>
		private int IntArgument(CommandLine cli, string argName, int default_)
		{
			string o = cli.GetOptionValue(argName);
			if (o == null)
			{
				return default_;
			}
			else
			{
				return System.Convert.ToInt32(o);
			}
		}

		/// <summary>Starts a MiniHadoopCluster.</summary>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			new MiniHadoopClusterManager().Run(args);
		}
	}
}
