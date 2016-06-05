using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Cli
{
	/// <summary>Cluster CLI used to get over all information of the cluster</summary>
	public class ClusterCLI : YarnCLI
	{
		private const string Title = "yarn cluster";

		public const string ListLabelsCmd = "list-node-labels";

		public const string DirectlyAccessNodeLabelStore = "directly-access-node-label-store";

		public const string Cmd = "cluster";

		private bool accessLocal = false;

		internal static CommonNodeLabelsManager localNodeLabelsManager = null;

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			ClusterCLI cli = new ClusterCLI();
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
			opts.AddOption("lnl", ListLabelsCmd, false, "List cluster node-label collection");
			opts.AddOption("h", HelpCmd, false, "Displays help for all commands.");
			opts.AddOption("dnl", DirectlyAccessNodeLabelStore, false, "Directly access node label store, "
				 + "with this option, all node label related operations" + " will NOT connect RM. Instead, they will"
				 + " access/modify stored node labels directly." + " By default, it is false (access via RM)."
				 + " AND PLEASE NOTE: if you configured " + YarnConfiguration.FsNodeLabelsStoreRootDir
				 + " to a local directory" + " (instead of NFS or HDFS), this option will only work"
				 + " when the command run on the machine where RM is running." + " Also, this option is UNSTABLE, could be removed in future"
				 + " releases.");
			int exitCode = -1;
			CommandLine parsedCli = null;
			try
			{
				parsedCli = new GnuParser().Parse(opts, args);
			}
			catch (MissingArgumentException)
			{
				sysout.WriteLine("Missing argument for options");
				PrintUsage(opts);
				return exitCode;
			}
			if (parsedCli.HasOption(DirectlyAccessNodeLabelStore))
			{
				accessLocal = true;
			}
			if (parsedCli.HasOption(ListLabelsCmd))
			{
				PrintClusterNodeLabels();
			}
			else
			{
				if (parsedCli.HasOption(HelpCmd))
				{
					PrintUsage(opts);
					return 0;
				}
				else
				{
					syserr.WriteLine("Invalid Command Usage : ");
					PrintUsage(opts);
				}
			}
			return 0;
		}

		private IList<string> SortStrSet(ICollection<string> labels)
		{
			IList<string> list = new AList<string>();
			Sharpen.Collections.AddAll(list, labels);
			list.Sort();
			return list;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void PrintClusterNodeLabels()
		{
			ICollection<string> nodeLabels = null;
			if (accessLocal)
			{
				nodeLabels = GetNodeLabelManagerInstance(GetConf()).GetClusterNodeLabels();
			}
			else
			{
				nodeLabels = client.GetClusterNodeLabels();
			}
			sysout.WriteLine(string.Format("Node Labels: %s", StringUtils.Join(SortStrSet(nodeLabels
				).GetEnumerator(), ",")));
		}

		[VisibleForTesting]
		internal static CommonNodeLabelsManager GetNodeLabelManagerInstance(Configuration
			 conf)
		{
			lock (typeof(ClusterCLI))
			{
				if (localNodeLabelsManager == null)
				{
					localNodeLabelsManager = new CommonNodeLabelsManager();
					localNodeLabelsManager.Init(conf);
					localNodeLabelsManager.Start();
				}
				return localNodeLabelsManager;
			}
		}

		/// <exception cref="System.IO.UnsupportedEncodingException"/>
		[VisibleForTesting]
		internal virtual void PrintUsage(Options opts)
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(baos, Sharpen.Extensions.GetEncoding
				("UTF-8")));
			new HelpFormatter().PrintHelp(pw, HelpFormatter.DefaultWidth, Title, null, opts, 
				HelpFormatter.DefaultLeftPad, HelpFormatter.DefaultDescPad, null);
			pw.Close();
			sysout.WriteLine(baos.ToString("UTF-8"));
		}
	}
}
