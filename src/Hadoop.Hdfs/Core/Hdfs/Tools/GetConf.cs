using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>Tool for getting configuration information from a configuration file.</summary>
	/// <remarks>
	/// Tool for getting configuration information from a configuration file.
	/// Adding more options:
	/// <ul>
	/// <li>
	/// If adding a simple option to get a value corresponding to a key in the
	/// configuration, use regular
	/// <see cref="CommandHandler"/>
	/// .
	/// See
	/// <see cref="Command.ExcludeFile"/>
	/// example.
	/// </li>
	/// <li>
	/// If adding an option that is does not return a value for a key, add
	/// a subclass of
	/// <see cref="CommandHandler"/>
	/// and set it up in
	/// <see cref="Command"/>
	/// .
	/// See
	/// <see cref="Command.Namenode"/>
	/// for example.
	/// Add for the new option added, a map entry with the corresponding
	/// <see cref="CommandHandler"/>
	/// .
	/// </ul>
	/// </remarks>
	public class GetConf : Configured, Tool
	{
		private const string Description = "hdfs getconf is utility for " + "getting configuration information from the config file.\n";

		[System.Serializable]
		internal sealed class Command
		{
			public static readonly GetConf.Command Namenode = new GetConf.Command("-namenodes"
				, "gets list of namenodes in the cluster.");

			public static readonly GetConf.Command Secondary = new GetConf.Command("-secondaryNameNodes"
				, "gets list of secondary namenodes in the cluster.");

			public static readonly GetConf.Command Backup = new GetConf.Command("-backupNodes"
				, "gets list of backup nodes in the cluster.");

			public static readonly GetConf.Command IncludeFile = new GetConf.Command("-includeFile"
				, "gets the include file path that defines the datanodes " + "that can join the cluster."
				);

			public static readonly GetConf.Command ExcludeFile = new GetConf.Command("-excludeFile"
				, "gets the exclude file path that defines the datanodes " + "that need to decommissioned."
				);

			public static readonly GetConf.Command Nnrpcaddresses = new GetConf.Command("-nnRpcAddresses"
				, "gets the namenode rpc addresses");

			public static readonly GetConf.Command Confkey = new GetConf.Command("-confKey [key]"
				, "gets a specific key from the configuration");

			private static readonly IDictionary<string, GetConf.CommandHandler> map;

			static Command()
			{
				GetConf.Command.map = new Dictionary<string, GetConf.CommandHandler>();
				GetConf.Command.map[StringUtils.ToLowerCase(GetConf.Command.Namenode.GetName())] 
					= new GetConf.NameNodesCommandHandler();
				GetConf.Command.map[StringUtils.ToLowerCase(GetConf.Command.Secondary.GetName())]
					 = new GetConf.SecondaryNameNodesCommandHandler();
				GetConf.Command.map[StringUtils.ToLowerCase(GetConf.Command.Backup.GetName())] = 
					new GetConf.BackupNodesCommandHandler();
				GetConf.Command.map[StringUtils.ToLowerCase(GetConf.Command.IncludeFile.GetName()
					)] = new GetConf.CommandHandler(DFSConfigKeys.DfsHosts);
				GetConf.Command.map[StringUtils.ToLowerCase(GetConf.Command.ExcludeFile.GetName()
					)] = new GetConf.CommandHandler(DFSConfigKeys.DfsHostsExclude);
				GetConf.Command.map[StringUtils.ToLowerCase(GetConf.Command.Nnrpcaddresses.GetName
					())] = new GetConf.NNRpcAddressesCommandHandler();
				GetConf.Command.map[StringUtils.ToLowerCase(GetConf.Command.Confkey.GetName())] =
					 new GetConf.PrintConfKeyCommandHandler();
			}

			private readonly string cmd;

			private readonly string description;

			internal Command(string cmd, string description)
			{
				this.cmd = cmd;
				this.description = description;
			}

			public string GetName()
			{
				return GetConf.Command.cmd.Split(" ")[0];
			}

			public string GetUsage()
			{
				return GetConf.Command.cmd;
			}

			public string GetDescription()
			{
				return GetConf.Command.description;
			}

			public static GetConf.CommandHandler GetHandler(string cmd)
			{
				return GetConf.Command.map[StringUtils.ToLowerCase(cmd)];
			}
		}

		internal static readonly string Usage;

		static GetConf()
		{
			HdfsConfiguration.Init();
			/* Initialize USAGE based on Command values */
			StringBuilder usage = new StringBuilder(Description);
			usage.Append("\nhadoop getconf \n");
			foreach (GetConf.Command cmd in GetConf.Command.Values())
			{
				usage.Append("\t[" + cmd.GetUsage() + "]\t\t\t" + cmd.GetDescription() + "\n");
			}
			Usage = usage.ToString();
		}

		/// <summary>
		/// Handler to return value for key corresponding to the
		/// <see cref="Command"/>
		/// </summary>
		internal class CommandHandler
		{
			internal string key;

			internal CommandHandler()
				: this(null)
			{
			}

			internal CommandHandler(string key)
			{
				// Configuration key to lookup
				this.key = key;
			}

			internal int DoWork(GetConf tool, string[] args)
			{
				try
				{
					CheckArgs(args);
					return DoWorkInternal(tool, args);
				}
				catch (Exception e)
				{
					tool.PrintError(e.Message);
				}
				return -1;
			}

			protected internal virtual void CheckArgs(string[] args)
			{
				if (args.Length > 0)
				{
					throw new HadoopIllegalArgumentException("Did not expect argument: " + args[0]);
				}
			}

			/// <summary>Method to be overridden by sub classes for specific behavior</summary>
			/// <exception cref="System.Exception"/>
			internal virtual int DoWorkInternal(GetConf tool, string[] args)
			{
				string value = tool.GetConf().GetTrimmed(key);
				if (value != null)
				{
					tool.PrintOut(value);
					return 0;
				}
				tool.PrintError("Configuration " + key + " is missing.");
				return -1;
			}
		}

		/// <summary>
		/// Handler for
		/// <see cref="Command.Namenode"/>
		/// </summary>
		internal class NameNodesCommandHandler : GetConf.CommandHandler
		{
			/// <exception cref="System.IO.IOException"/>
			internal override int DoWorkInternal(GetConf tool, string[] args)
			{
				tool.PrintMap(DFSUtil.GetNNServiceRpcAddressesForCluster(tool.GetConf()));
				return 0;
			}
		}

		/// <summary>
		/// Handler for
		/// <see cref="Command.Backup"/>
		/// </summary>
		internal class BackupNodesCommandHandler : GetConf.CommandHandler
		{
			/// <exception cref="System.IO.IOException"/>
			internal override int DoWorkInternal(GetConf tool, string[] args)
			{
				tool.PrintMap(DFSUtil.GetBackupNodeAddresses(tool.GetConf()));
				return 0;
			}
		}

		/// <summary>
		/// Handler for
		/// <see cref="Command.Secondary"/>
		/// </summary>
		internal class SecondaryNameNodesCommandHandler : GetConf.CommandHandler
		{
			/// <exception cref="System.IO.IOException"/>
			internal override int DoWorkInternal(GetConf tool, string[] args)
			{
				tool.PrintMap(DFSUtil.GetSecondaryNameNodeAddresses(tool.GetConf()));
				return 0;
			}
		}

		/// <summary>
		/// Handler for
		/// <see cref="Command.Nnrpcaddresses"/>
		/// If rpc addresses are defined in configuration, we return them. Otherwise,
		/// return empty string.
		/// </summary>
		internal class NNRpcAddressesCommandHandler : GetConf.CommandHandler
		{
			/// <exception cref="System.IO.IOException"/>
			internal override int DoWorkInternal(GetConf tool, string[] args)
			{
				Configuration config = tool.GetConf();
				IList<DFSUtil.ConfiguredNNAddress> cnnlist = DFSUtil.FlattenAddressMap(DFSUtil.GetNNServiceRpcAddressesForCluster
					(config));
				if (!cnnlist.IsEmpty())
				{
					foreach (DFSUtil.ConfiguredNNAddress cnn in cnnlist)
					{
						IPEndPoint rpc = cnn.GetAddress();
						tool.PrintOut(rpc.GetHostName() + ":" + rpc.Port);
					}
					return 0;
				}
				tool.PrintError("Did not get namenode service rpc addresses.");
				return -1;
			}
		}

		internal class PrintConfKeyCommandHandler : GetConf.CommandHandler
		{
			protected internal override void CheckArgs(string[] args)
			{
				if (args.Length != 1)
				{
					throw new HadoopIllegalArgumentException("usage: " + GetConf.Command.Confkey.GetUsage
						());
				}
			}

			/// <exception cref="System.Exception"/>
			internal override int DoWorkInternal(GetConf tool, string[] args)
			{
				this.key = args[0];
				return base.DoWorkInternal(tool, args);
			}
		}

		private readonly TextWriter @out;

		private readonly TextWriter err;

		internal GetConf(Configuration conf)
			: this(conf, System.Console.Out, System.Console.Error)
		{
		}

		internal GetConf(Configuration conf, TextWriter @out, TextWriter err)
			: base(conf)
		{
			// Stream for printing command output
			// Stream for printing error
			this.@out = @out;
			this.err = err;
		}

		internal virtual void PrintError(string message)
		{
			err.WriteLine(message);
		}

		internal virtual void PrintOut(string message)
		{
			@out.WriteLine(message);
		}

		internal virtual void PrintMap(IDictionary<string, IDictionary<string, IPEndPoint
			>> map)
		{
			StringBuilder buffer = new StringBuilder();
			IList<DFSUtil.ConfiguredNNAddress> cnns = DFSUtil.FlattenAddressMap(map);
			foreach (DFSUtil.ConfiguredNNAddress cnn in cnns)
			{
				IPEndPoint address = cnn.GetAddress();
				if (buffer.Length > 0)
				{
					buffer.Append(" ");
				}
				buffer.Append(address.GetHostName());
			}
			PrintOut(buffer.ToString());
		}

		private void PrintUsage()
		{
			PrintError(Usage);
		}

		/// <summary>Main method that runs the tool for given arguments.</summary>
		/// <param name="args">arguments</param>
		/// <returns>return status of the command</returns>
		private int DoWork(string[] args)
		{
			if (args.Length >= 1)
			{
				GetConf.CommandHandler handler = GetConf.Command.GetHandler(args[0]);
				if (handler != null)
				{
					return handler.DoWork(this, Arrays.CopyOfRange(args, 1, args.Length));
				}
			}
			PrintUsage();
			return -1;
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			try
			{
				return UserGroupInformation.GetCurrentUser().DoAs(new _PrivilegedExceptionAction_316
					(this, args));
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
		}

		private sealed class _PrivilegedExceptionAction_316 : PrivilegedExceptionAction<int
			>
		{
			public _PrivilegedExceptionAction_316(GetConf _enclosing, string[] args)
			{
				this._enclosing = _enclosing;
				this.args = args;
			}

			/// <exception cref="System.Exception"/>
			public int Run()
			{
				return this._enclosing.DoWork(args);
			}

			private readonly GetConf _enclosing;

			private readonly string[] args;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			if (DFSUtil.ParseHelpArgument(args, Usage, System.Console.Out, true))
			{
				System.Environment.Exit(0);
			}
			int res = ToolRunner.Run(new GetConf(new HdfsConfiguration()), args);
			System.Environment.Exit(res);
		}
	}
}
