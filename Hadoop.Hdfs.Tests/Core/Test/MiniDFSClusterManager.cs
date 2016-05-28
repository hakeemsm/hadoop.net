using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Mortbay.Util.Ajax;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	/// <summary>This class drives the creation of a mini-cluster on the local machine.</summary>
	/// <remarks>
	/// This class drives the creation of a mini-cluster on the local machine. By
	/// default, a MiniDFSCluster is spawned on the first available ports that are
	/// found.
	/// A series of command line flags controls the startup cluster options.
	/// This class can dump a Hadoop configuration and some basic metadata (in JSON)
	/// into a textfile.
	/// To shutdown the cluster, kill the process.
	/// To run this from the command line, do the following (replacing the jar
	/// version as appropriate):
	/// $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/hdfs/hadoop-hdfs-0.24.0-SNAPSHOT-tests.jar org.apache.hadoop.test.MiniDFSClusterManager -options...
	/// </remarks>
	public class MiniDFSClusterManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(MiniDFSClusterManager)
			);

		private MiniDFSCluster dfs;

		private string writeDetails;

		private int numDataNodes;

		private int nameNodePort;

		private HdfsServerConstants.StartupOption dfsOpts;

		private string writeConfig;

		private Configuration conf;

		private bool format;

		private const long SleepIntervalMs = 1000 * 60;

		/// <summary>Creates configuration options object.</summary>
		private Options MakeOptions()
		{
			Options options = new Options();
			options.AddOption("datanodes", true, "How many datanodes to start (default 1)").AddOption
				("format", false, "Format the DFS (default false)").AddOption("cmdport", true, "Which port to listen on for commands (default 0--we choose)"
				).AddOption("nnport", true, "NameNode port (default 0--we choose)").AddOption("namenode"
				, true, "URL of the namenode (default " + "is either the DFS cluster or a temporary dir)"
				).AddOption(OptionBuilder.Create("D")).AddOption(OptionBuilder.Create("writeConfig"
				)).AddOption(OptionBuilder.Create("writeDetails")).AddOption(OptionBuilder.Create
				("help"));
			return options;
		}

		/// <summary>Main entry-point.</summary>
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
					Sharpen.Thread.Sleep(SleepIntervalMs);
					if (!dfs.IsClusterUp())
					{
						Log.Info("Cluster is no longer up, exiting");
						return;
					}
				}
				catch (Exception)
				{
				}
			}
		}

		// nothing
		/// <summary>Starts DFS as specified in member-variable options.</summary>
		/// <remarks>
		/// Starts DFS as specified in member-variable options. Also writes out
		/// configuration and details, if requested.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		public virtual void Start()
		{
			dfs = new MiniDFSCluster.Builder(conf).NameNodePort(nameNodePort).NumDataNodes(numDataNodes
				).StartupOption(dfsOpts).Format(format).Build();
			dfs.WaitActive();
			Log.Info("Started MiniDFSCluster -- namenode on port " + dfs.GetNameNodePort());
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
				FileWriter fw = new FileWriter(new FilePath(writeDetails));
				fw.Write(new JSON().ToJSON(map));
				fw.Close();
			}
		}

		/// <summary>Parses arguments and fills out the member variables.</summary>
		/// <param name="args">Command-line arguments.</param>
		/// <returns>
		/// true on successful parse; false to indicate that the
		/// program should exit.
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
					Log.Error("Unrecognized option: " + arg);
					new HelpFormatter().PrintHelp("...", options);
					return false;
				}
			}
			// HDFS
			numDataNodes = IntArgument(cli, "datanodes", 1);
			nameNodePort = IntArgument(cli, "nnport", 0);
			if (cli.HasOption("format"))
			{
				dfsOpts = HdfsServerConstants.StartupOption.Format;
				format = true;
			}
			else
			{
				dfsOpts = HdfsServerConstants.StartupOption.Regular;
				format = false;
			}
			// Runner
			writeDetails = cli.GetOptionValue("writeDetails");
			writeConfig = cli.GetOptionValue("writeConfig");
			// General
			conf = new HdfsConfiguration();
			UpdateConfiguration(conf, cli.GetOptionValues("D"));
			return true;
		}

		/// <summary>Updates configuration based on what's given on the command line.</summary>
		/// <param name="conf2">The configuration object</param>
		/// <param name="keyvalues">An array of interleaved key value pairs.</param>
		private void UpdateConfiguration(Configuration conf2, string[] keyvalues)
		{
			int num_confs_updated = 0;
			if (keyvalues != null)
			{
				foreach (string prop in keyvalues)
				{
					string[] keyval = prop.Split("=", 2);
					if (keyval.Length == 2)
					{
						conf2.Set(keyval[0], keyval[1]);
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
		private int IntArgument(CommandLine cli, string argName, int defaultValue)
		{
			string o = cli.GetOptionValue(argName);
			try
			{
				if (o != null)
				{
					return System.Convert.ToInt32(o);
				}
			}
			catch (FormatException)
			{
				Log.Error("Couldn't parse value (" + o + ") for option " + argName + ". Using default: "
					 + defaultValue);
			}
			return defaultValue;
		}

		/// <summary>Starts a MiniDFSClusterManager with parameters drawn from the command line.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			new MiniDFSClusterManager().Run(args);
		}
	}
}
