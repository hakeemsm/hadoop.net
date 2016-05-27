using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Tracing
{
	/// <summary>A command-line tool for viewing and modifying tracing settings.</summary>
	public class TraceAdmin : Configured, Tool
	{
		private TraceAdminProtocolPB proxy;

		private TraceAdminProtocolTranslatorPB remote;

		private void Usage()
		{
			TextWriter err = System.Console.Error;
			err.Write("Hadoop tracing configuration commands:\n" + "  -add [-class classname] [-Ckey=value] [-Ckey2=value2] ...\n"
				 + "    Add a span receiver with the provided class name.  Configuration\n" + "    keys for the span receiver can be specified with the -C options.\n"
				 + "    The span receiver will also inherit whatever configuration keys\n" + "    exist in the daemon's configuration.\n"
				 + "  -help: Print this help message.\n" + "  -host [hostname:port]\n" + "    Specify the hostname and port of the daemon to examine.\n"
				 + "    Required for all commands.\n" + "  -list: List the current span receivers.\n"
				 + "  -remove [id]\n" + "    Remove the span receiver with the specified id.  Use -list to\n"
				 + "    find the id of each receiver.\n");
		}

		/// <exception cref="System.IO.IOException"/>
		private int ListSpanReceivers(IList<string> args)
		{
			SpanReceiverInfo[] infos = remote.ListSpanReceivers();
			if (infos.Length == 0)
			{
				System.Console.Out.WriteLine("[no span receivers found]");
				return 0;
			}
			TableListing listing = new TableListing.Builder().AddField("ID").AddField("CLASS"
				).ShowHeaders().Build();
			foreach (SpanReceiverInfo info in infos)
			{
				listing.AddRow(string.Empty + info.GetId(), info.GetClassName());
			}
			System.Console.Out.WriteLine(listing.ToString());
			return 0;
		}

		private const string ConfigPrefix = "-C";

		/// <exception cref="System.IO.IOException"/>
		private int AddSpanReceiver(IList<string> args)
		{
			string className = StringUtils.PopOptionWithArgument("-class", args);
			if (className == null)
			{
				System.Console.Error.WriteLine("You must specify the classname with -class.");
				return 1;
			}
			ByteArrayOutputStream configStream = new ByteArrayOutputStream();
			TextWriter configsOut = new TextWriter(configStream, false, "UTF-8");
			SpanReceiverInfoBuilder factory = new SpanReceiverInfoBuilder(className);
			string prefix = string.Empty;
			for (int i = 0; i < args.Count; ++i)
			{
				string str = args[i];
				if (!str.StartsWith(ConfigPrefix))
				{
					System.Console.Error.WriteLine("Can't understand argument: " + str);
					return 1;
				}
				str = Sharpen.Runtime.Substring(str, ConfigPrefix.Length);
				int equalsIndex = str.IndexOf("=");
				if (equalsIndex < 0)
				{
					System.Console.Error.WriteLine("Can't parse configuration argument " + str);
					System.Console.Error.WriteLine("Arguments must be in the form key=value");
					return 1;
				}
				string key = Sharpen.Runtime.Substring(str, 0, equalsIndex);
				string value = Sharpen.Runtime.Substring(str, equalsIndex + 1);
				factory.AddConfigurationPair(key, value);
				configsOut.Write(prefix + key + " = " + value);
				prefix = ", ";
			}
			string configStreamStr = configStream.ToString("UTF-8");
			try
			{
				long id = remote.AddSpanReceiver(factory.Build());
				System.Console.Out.WriteLine("Added trace span receiver " + id + " with configuration "
					 + configStreamStr);
			}
			catch (IOException e)
			{
				System.Console.Out.WriteLine("addSpanReceiver error with configuration " + configStreamStr
					);
				throw;
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private int RemoveSpanReceiver(IList<string> args)
		{
			string indexStr = StringUtils.PopFirstNonOption(args);
			long id = -1;
			try
			{
				id = long.Parse(indexStr);
			}
			catch (FormatException e)
			{
				System.Console.Error.WriteLine("Failed to parse ID string " + indexStr + ": " + e
					.Message);
				return 1;
			}
			remote.RemoveSpanReceiver(id);
			System.Console.Error.WriteLine("Removed trace span receiver " + id);
			return 0;
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] argv)
		{
			List<string> args = new List<string>();
			foreach (string arg in argv)
			{
				args.AddItem(arg);
			}
			if (StringUtils.PopOption("-h", args) || StringUtils.PopOption("-help", args))
			{
				Usage();
				return 0;
			}
			else
			{
				if (args.Count == 0)
				{
					Usage();
					return 0;
				}
			}
			string hostPort = StringUtils.PopOptionWithArgument("-host", args);
			if (hostPort == null)
			{
				System.Console.Error.WriteLine("You must specify a host with -host.");
				return 1;
			}
			if (args.Count < 0)
			{
				System.Console.Error.WriteLine("You must specify an operation.");
				return 1;
			}
			RPC.SetProtocolEngine(GetConf(), typeof(TraceAdminProtocolPB), typeof(ProtobufRpcEngine
				));
			IPEndPoint address = NetUtils.CreateSocketAddr(hostPort);
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			Type xface = typeof(TraceAdminProtocolPB);
			proxy = (TraceAdminProtocolPB)RPC.GetProxy(xface, RPC.GetProtocolVersion(xface), 
				address, ugi, GetConf(), NetUtils.GetDefaultSocketFactory(GetConf()), 0);
			remote = new TraceAdminProtocolTranslatorPB(proxy);
			try
			{
				if (args[0].Equals("-list"))
				{
					return ListSpanReceivers(args.SubList(1, args.Count));
				}
				else
				{
					if (args[0].Equals("-add"))
					{
						return AddSpanReceiver(args.SubList(1, args.Count));
					}
					else
					{
						if (args[0].Equals("-remove"))
						{
							return RemoveSpanReceiver(args.SubList(1, args.Count));
						}
						else
						{
							System.Console.Error.WriteLine("Unrecognized tracing command: " + args[0]);
							System.Console.Error.WriteLine("Use -help for help.");
							return 1;
						}
					}
				}
			}
			finally
			{
				remote.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			TraceAdmin admin = new TraceAdmin();
			admin.SetConf(new Configuration());
			System.Environment.Exit(admin.Run(argv));
		}
	}
}
