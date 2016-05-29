using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Cli;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Cli
{
	public class QueueCLI : YarnCLI
	{
		public const string Queue = "queue";

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			QueueCLI cli = new QueueCLI();
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
			opts.AddOption(StatusCmd, true, "List queue information about given queue.");
			opts.AddOption(HelpCmd, false, "Displays help for all commands.");
			opts.GetOption(StatusCmd).SetArgName("Queue Name");
			CommandLine cliParser = null;
			try
			{
				cliParser = new GnuParser().Parse(opts, args);
			}
			catch (MissingArgumentException)
			{
				sysout.WriteLine("Missing argument for options");
				PrintUsage(opts);
				return -1;
			}
			if (cliParser.HasOption(StatusCmd))
			{
				if (args.Length != 2)
				{
					PrintUsage(opts);
					return -1;
				}
				return ListQueue(cliParser.GetOptionValue(StatusCmd));
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
					return -1;
				}
			}
		}

		/// <summary>It prints the usage of the command</summary>
		/// <param name="opts"/>
		[VisibleForTesting]
		internal virtual void PrintUsage(Options opts)
		{
			new HelpFormatter().PrintHelp(Queue, opts);
		}

		/// <summary>Lists the Queue Information matching the given queue name</summary>
		/// <param name="queueName"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private int ListQueue(string queueName)
		{
			int rc;
			PrintWriter writer = new PrintWriter(new OutputStreamWriter(sysout, Sharpen.Extensions.GetEncoding
				("UTF-8")));
			QueueInfo queueInfo = client.GetQueueInfo(queueName);
			if (queueInfo != null)
			{
				writer.WriteLine("Queue Information : ");
				PrintQueueInfo(writer, queueInfo);
				rc = 0;
			}
			else
			{
				writer.WriteLine("Cannot get queue from RM by queueName = " + queueName + ", please check."
					);
				rc = -1;
			}
			writer.Flush();
			return rc;
		}

		private void PrintQueueInfo(PrintWriter writer, QueueInfo queueInfo)
		{
			writer.Write("Queue Name : ");
			writer.WriteLine(queueInfo.GetQueueName());
			writer.Write("\tState : ");
			writer.WriteLine(queueInfo.GetQueueState());
			DecimalFormat df = new DecimalFormat("#.0");
			writer.Write("\tCapacity : ");
			writer.WriteLine(df.Format(queueInfo.GetCapacity() * 100) + "%");
			writer.Write("\tCurrent Capacity : ");
			writer.WriteLine(df.Format(queueInfo.GetCurrentCapacity() * 100) + "%");
			writer.Write("\tMaximum Capacity : ");
			writer.WriteLine(df.Format(queueInfo.GetMaximumCapacity() * 100) + "%");
			writer.Write("\tDefault Node Label expression : ");
			if (null != queueInfo.GetDefaultNodeLabelExpression())
			{
				writer.WriteLine(queueInfo.GetDefaultNodeLabelExpression());
			}
			else
			{
				writer.WriteLine();
			}
			ICollection<string> nodeLabels = queueInfo.GetAccessibleNodeLabels();
			StringBuilder labelList = new StringBuilder();
			writer.Write("\tAccessible Node Labels : ");
			foreach (string nodeLabel in nodeLabels)
			{
				if (labelList.Length > 0)
				{
					labelList.Append(',');
				}
				labelList.Append(nodeLabel);
			}
			writer.WriteLine(labelList.ToString());
		}
	}
}
