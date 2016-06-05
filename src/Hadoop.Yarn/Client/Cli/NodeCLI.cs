using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Lang.Time;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Cli
{
	public class NodeCLI : YarnCLI
	{
		private static readonly string NodesPattern = "%16s\t%15s\t%17s\t%28s" + Runtime.
			GetProperty("line.separator");

		private const string NodeStateCmd = "states";

		private const string NodeAll = "all";

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			NodeCLI cli = new NodeCLI();
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
			opts.AddOption(HelpCmd, false, "Displays help for all commands.");
			opts.AddOption(StatusCmd, true, "Prints the status report of the node.");
			opts.AddOption(ListCmd, false, "List all running nodes. " + "Supports optional use of -states to filter nodes "
				 + "based on node state, all -all to list all nodes.");
			Option nodeStateOpt = new Option(NodeStateCmd, true, "Works with -list to filter nodes based on input comma-separated list of node states."
				);
			nodeStateOpt.SetValueSeparator(',');
			nodeStateOpt.SetArgs(Option.UnlimitedValues);
			nodeStateOpt.SetArgName("States");
			opts.AddOption(nodeStateOpt);
			Option allOpt = new Option(NodeAll, false, "Works with -list to list all nodes.");
			opts.AddOption(allOpt);
			opts.GetOption(StatusCmd).SetArgName("NodeId");
			int exitCode = -1;
			CommandLine cliParser = null;
			try
			{
				cliParser = new GnuParser().Parse(opts, args);
			}
			catch (MissingArgumentException)
			{
				sysout.WriteLine("Missing argument for options");
				PrintUsage(opts);
				return exitCode;
			}
			if (cliParser.HasOption("status"))
			{
				if (args.Length != 2)
				{
					PrintUsage(opts);
					return exitCode;
				}
				PrintNodeStatus(cliParser.GetOptionValue("status"));
			}
			else
			{
				if (cliParser.HasOption("list"))
				{
					ICollection<NodeState> nodeStates = new HashSet<NodeState>();
					if (cliParser.HasOption(NodeAll))
					{
						foreach (NodeState state in NodeState.Values())
						{
							nodeStates.AddItem(state);
						}
					}
					else
					{
						if (cliParser.HasOption(NodeStateCmd))
						{
							string[] types = cliParser.GetOptionValues(NodeStateCmd);
							if (types != null)
							{
								foreach (string type in types)
								{
									if (!type.Trim().IsEmpty())
									{
										nodeStates.AddItem(NodeState.ValueOf(StringUtils.ToUpperCase(type.Trim())));
									}
								}
							}
						}
						else
						{
							nodeStates.AddItem(NodeState.Running);
						}
					}
					ListClusterNodes(nodeStates);
				}
				else
				{
					if (cliParser.HasOption(HelpCmd))
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
			}
			return 0;
		}

		/// <summary>It prints the usage of the command</summary>
		/// <param name="opts"/>
		private void PrintUsage(Options opts)
		{
			new HelpFormatter().PrintHelp("node", opts);
		}

		/// <summary>Lists the nodes matching the given node states</summary>
		/// <param name="nodeStates"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void ListClusterNodes(ICollection<NodeState> nodeStates)
		{
			PrintWriter writer = new PrintWriter(new OutputStreamWriter(sysout, Sharpen.Extensions.GetEncoding
				("UTF-8")));
			IList<NodeReport> nodesReport = client.GetNodeReports(Sharpen.Collections.ToArray
				(nodeStates, new NodeState[0]));
			writer.WriteLine("Total Nodes:" + nodesReport.Count);
			writer.Printf(NodesPattern, "Node-Id", "Node-State", "Node-Http-Address", "Number-of-Running-Containers"
				);
			foreach (NodeReport nodeReport in nodesReport)
			{
				writer.Printf(NodesPattern, nodeReport.GetNodeId(), nodeReport.GetNodeState(), nodeReport
					.GetHttpAddress(), nodeReport.GetNumContainers());
			}
			writer.Flush();
		}

		/// <summary>Prints the node report for node id.</summary>
		/// <param name="nodeIdStr"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void PrintNodeStatus(string nodeIdStr)
		{
			NodeId nodeId = ConverterUtils.ToNodeId(nodeIdStr);
			IList<NodeReport> nodesReport = client.GetNodeReports();
			// Use PrintWriter.println, which uses correct platform line ending.
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter nodeReportStr = new PrintWriter(new OutputStreamWriter(baos, Sharpen.Extensions.GetEncoding
				("UTF-8")));
			NodeReport nodeReport = null;
			foreach (NodeReport report in nodesReport)
			{
				if (!report.GetNodeId().Equals(nodeId))
				{
					continue;
				}
				nodeReport = report;
				nodeReportStr.WriteLine("Node Report : ");
				nodeReportStr.Write("\tNode-Id : ");
				nodeReportStr.WriteLine(nodeReport.GetNodeId());
				nodeReportStr.Write("\tRack : ");
				nodeReportStr.WriteLine(nodeReport.GetRackName());
				nodeReportStr.Write("\tNode-State : ");
				nodeReportStr.WriteLine(nodeReport.GetNodeState());
				nodeReportStr.Write("\tNode-Http-Address : ");
				nodeReportStr.WriteLine(nodeReport.GetHttpAddress());
				nodeReportStr.Write("\tLast-Health-Update : ");
				nodeReportStr.WriteLine(DateFormatUtils.Format(Sharpen.Extensions.CreateDate(nodeReport
					.GetLastHealthReportTime()), "E dd/MMM/yy hh:mm:ss:SSzz"));
				nodeReportStr.Write("\tHealth-Report : ");
				nodeReportStr.WriteLine(nodeReport.GetHealthReport());
				nodeReportStr.Write("\tContainers : ");
				nodeReportStr.WriteLine(nodeReport.GetNumContainers());
				nodeReportStr.Write("\tMemory-Used : ");
				nodeReportStr.WriteLine((nodeReport.GetUsed() == null) ? "0MB" : (nodeReport.GetUsed
					().GetMemory() + "MB"));
				nodeReportStr.Write("\tMemory-Capacity : ");
				nodeReportStr.WriteLine(nodeReport.GetCapability().GetMemory() + "MB");
				nodeReportStr.Write("\tCPU-Used : ");
				nodeReportStr.WriteLine((nodeReport.GetUsed() == null) ? "0 vcores" : (nodeReport
					.GetUsed().GetVirtualCores() + " vcores"));
				nodeReportStr.Write("\tCPU-Capacity : ");
				nodeReportStr.WriteLine(nodeReport.GetCapability().GetVirtualCores() + " vcores");
				nodeReportStr.Write("\tNode-Labels : ");
				// Create a List for node labels since we need it get sorted
				IList<string> nodeLabelsList = new AList<string>(report.GetNodeLabels());
				nodeLabelsList.Sort();
				nodeReportStr.WriteLine(StringUtils.Join(nodeLabelsList.GetEnumerator(), ','));
			}
			if (nodeReport == null)
			{
				nodeReportStr.Write("Could not find the node report for node id : " + nodeIdStr);
			}
			nodeReportStr.Close();
			sysout.WriteLine(baos.ToString("UTF-8"));
		}
	}
}
