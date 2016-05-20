using Sharpen;

namespace org.apache.hadoop.tracing
{
	/// <summary>A command-line tool for viewing and modifying tracing settings.</summary>
	public class TraceAdmin : org.apache.hadoop.conf.Configured, org.apache.hadoop.util.Tool
	{
		private org.apache.hadoop.tracing.TraceAdminProtocolPB proxy;

		private org.apache.hadoop.tracing.TraceAdminProtocolTranslatorPB remote;

		private void usage()
		{
			System.IO.TextWriter err = System.Console.Error;
			err.Write("Hadoop tracing configuration commands:\n" + "  -add [-class classname] [-Ckey=value] [-Ckey2=value2] ...\n"
				 + "    Add a span receiver with the provided class name.  Configuration\n" + "    keys for the span receiver can be specified with the -C options.\n"
				 + "    The span receiver will also inherit whatever configuration keys\n" + "    exist in the daemon's configuration.\n"
				 + "  -help: Print this help message.\n" + "  -host [hostname:port]\n" + "    Specify the hostname and port of the daemon to examine.\n"
				 + "    Required for all commands.\n" + "  -list: List the current span receivers.\n"
				 + "  -remove [id]\n" + "    Remove the span receiver with the specified id.  Use -list to\n"
				 + "    find the id of each receiver.\n");
		}

		/// <exception cref="System.IO.IOException"/>
		private int listSpanReceivers(System.Collections.Generic.IList<string> args)
		{
			org.apache.hadoop.tracing.SpanReceiverInfo[] infos = remote.listSpanReceivers();
			if (infos.Length == 0)
			{
				System.Console.Out.WriteLine("[no span receivers found]");
				return 0;
			}
			org.apache.hadoop.tools.TableListing listing = new org.apache.hadoop.tools.TableListing.Builder
				().addField("ID").addField("CLASS").showHeaders().build();
			foreach (org.apache.hadoop.tracing.SpanReceiverInfo info in infos)
			{
				listing.addRow(string.Empty + info.getId(), info.getClassName());
			}
			System.Console.Out.WriteLine(listing.ToString());
			return 0;
		}

		private const string CONFIG_PREFIX = "-C";

		/// <exception cref="System.IO.IOException"/>
		private int addSpanReceiver(System.Collections.Generic.IList<string> args)
		{
			string className = org.apache.hadoop.util.StringUtils.popOptionWithArgument("-class"
				, args);
			if (className == null)
			{
				System.Console.Error.WriteLine("You must specify the classname with -class.");
				return 1;
			}
			java.io.ByteArrayOutputStream configStream = new java.io.ByteArrayOutputStream();
			System.IO.TextWriter configsOut = new System.IO.TextWriter(configStream, false, "UTF-8"
				);
			org.apache.hadoop.tracing.SpanReceiverInfoBuilder factory = new org.apache.hadoop.tracing.SpanReceiverInfoBuilder
				(className);
			string prefix = string.Empty;
			for (int i = 0; i < args.Count; ++i)
			{
				string str = args[i];
				if (!str.StartsWith(CONFIG_PREFIX))
				{
					System.Console.Error.WriteLine("Can't understand argument: " + str);
					return 1;
				}
				str = Sharpen.Runtime.substring(str, CONFIG_PREFIX.Length);
				int equalsIndex = str.IndexOf("=");
				if (equalsIndex < 0)
				{
					System.Console.Error.WriteLine("Can't parse configuration argument " + str);
					System.Console.Error.WriteLine("Arguments must be in the form key=value");
					return 1;
				}
				string key = Sharpen.Runtime.substring(str, 0, equalsIndex);
				string value = Sharpen.Runtime.substring(str, equalsIndex + 1);
				factory.addConfigurationPair(key, value);
				configsOut.Write(prefix + key + " = " + value);
				prefix = ", ";
			}
			string configStreamStr = configStream.toString("UTF-8");
			try
			{
				long id = remote.addSpanReceiver(factory.build());
				System.Console.Out.WriteLine("Added trace span receiver " + id + " with configuration "
					 + configStreamStr);
			}
			catch (System.IO.IOException e)
			{
				System.Console.Out.WriteLine("addSpanReceiver error with configuration " + configStreamStr
					);
				throw;
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private int removeSpanReceiver(System.Collections.Generic.IList<string> args)
		{
			string indexStr = org.apache.hadoop.util.StringUtils.popFirstNonOption(args);
			long id = -1;
			try
			{
				id = long.Parse(indexStr);
			}
			catch (java.lang.NumberFormatException e)
			{
				System.Console.Error.WriteLine("Failed to parse ID string " + indexStr + ": " + e
					.Message);
				return 1;
			}
			remote.removeSpanReceiver(id);
			System.Console.Error.WriteLine("Removed trace span receiver " + id);
			return 0;
		}

		/// <exception cref="System.Exception"/>
		public virtual int run(string[] argv)
		{
			System.Collections.Generic.LinkedList<string> args = new System.Collections.Generic.LinkedList
				<string>();
			foreach (string arg in argv)
			{
				args.add(arg);
			}
			if (org.apache.hadoop.util.StringUtils.popOption("-h", args) || org.apache.hadoop.util.StringUtils
				.popOption("-help", args))
			{
				usage();
				return 0;
			}
			else
			{
				if (args.Count == 0)
				{
					usage();
					return 0;
				}
			}
			string hostPort = org.apache.hadoop.util.StringUtils.popOptionWithArgument("-host"
				, args);
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
			org.apache.hadoop.ipc.RPC.setProtocolEngine(getConf(), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.tracing.TraceAdminProtocolPB)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine)));
			java.net.InetSocketAddress address = org.apache.hadoop.net.NetUtils.createSocketAddr
				(hostPort);
			org.apache.hadoop.security.UserGroupInformation ugi = org.apache.hadoop.security.UserGroupInformation
				.getCurrentUser();
			java.lang.Class xface = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.tracing.TraceAdminProtocolPB
				));
			proxy = (org.apache.hadoop.tracing.TraceAdminProtocolPB)org.apache.hadoop.ipc.RPC
				.getProxy(xface, org.apache.hadoop.ipc.RPC.getProtocolVersion(xface), address, ugi
				, getConf(), org.apache.hadoop.net.NetUtils.getDefaultSocketFactory(getConf()), 
				0);
			remote = new org.apache.hadoop.tracing.TraceAdminProtocolTranslatorPB(proxy);
			try
			{
				if (args[0].Equals("-list"))
				{
					return listSpanReceivers(args.subList(1, args.Count));
				}
				else
				{
					if (args[0].Equals("-add"))
					{
						return addSpanReceiver(args.subList(1, args.Count));
					}
					else
					{
						if (args[0].Equals("-remove"))
						{
							return removeSpanReceiver(args.subList(1, args.Count));
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
				remote.close();
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			org.apache.hadoop.tracing.TraceAdmin admin = new org.apache.hadoop.tracing.TraceAdmin
				();
			admin.setConf(new org.apache.hadoop.conf.Configuration());
			System.Environment.Exit(admin.run(argv));
		}
	}
}
