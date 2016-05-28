using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Shell;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Ipc.ProtocolPB;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>This class provides some DFS administrative access shell commands.</summary>
	public class DFSAdmin : FsShell
	{
		static DFSAdmin()
		{
			HdfsConfiguration.Init();
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Tools.DFSAdmin
			));

		/// <summary>An abstract class for the execution of a file system command</summary>
		private abstract class DFSAdminCommand : Command
		{
			internal readonly DistributedFileSystem dfs;

			/// <summary>Constructor</summary>
			public DFSAdminCommand(FileSystem fs)
				: base(fs.GetConf())
			{
				if (!(fs is DistributedFileSystem))
				{
					throw new ArgumentException("FileSystem " + fs.GetUri() + " is not an HDFS file system"
						);
				}
				this.dfs = (DistributedFileSystem)fs;
			}
		}

		/// <summary>A class that supports command clearQuota</summary>
		private class ClearQuotaCommand : DFSAdmin.DFSAdminCommand
		{
			private const string Name = "clrQuota";

			private const string Usage = "-" + Name + " <dirname>...<dirname>";

			private const string Description = Usage + ": " + "Clear the quota for each directory <dirName>.\n"
				 + "\t\tFor each directory, attempt to clear the quota. An error will be reported if\n"
				 + "\t\t1. the directory does not exist or is a file, or\n" + "\t\t2. user is not an administrator.\n"
				 + "\t\tIt does not fault if the directory has no quota.";

			/// <summary>Constructor</summary>
			internal ClearQuotaCommand(string[] args, int pos, FileSystem fs)
				: base(fs)
			{
				CommandFormat c = new CommandFormat(1, int.MaxValue);
				IList<string> parameters = c.Parse(args, pos);
				this.args = Sharpen.Collections.ToArray(parameters, new string[parameters.Count]);
			}

			/// <summary>Check if a command is the clrQuota command</summary>
			/// <param name="cmd">A string representation of a command starting with "-"</param>
			/// <returns>true if this is a clrQuota command; false otherwise</returns>
			public static bool Matches(string cmd)
			{
				return ("-" + Name).Equals(cmd);
			}

			public override string GetCommandName()
			{
				return Name;
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void Run(Path path)
			{
				dfs.SetQuota(path, HdfsConstants.QuotaReset, HdfsConstants.QuotaDontSet);
			}
		}

		/// <summary>A class that supports command setQuota</summary>
		private class SetQuotaCommand : DFSAdmin.DFSAdminCommand
		{
			private const string Name = "setQuota";

			private const string Usage = "-" + Name + " <quota> <dirname>...<dirname>";

			private const string Description = "-setQuota <quota> <dirname>...<dirname>: " + 
				"Set the quota <quota> for each directory <dirName>.\n" + "\t\tThe directory quota is a long integer that puts a hard limit\n"
				 + "\t\ton the number of names in the directory tree\n" + "\t\tFor each directory, attempt to set the quota. An error will be reported if\n"
				 + "\t\t1. N is not a positive integer, or\n" + "\t\t2. User is not an administrator, or\n"
				 + "\t\t3. The directory does not exist or is a file.\n" + "\t\tNote: A quota of 1 would force the directory to remain empty.\n";

			private readonly long quota;

			/// <summary>Constructor</summary>
			internal SetQuotaCommand(string[] args, int pos, FileSystem fs)
				: base(fs)
			{
				// the quota to be set
				CommandFormat c = new CommandFormat(2, int.MaxValue);
				IList<string> parameters = c.Parse(args, pos);
				this.quota = long.Parse(parameters.Remove(0));
				this.args = Sharpen.Collections.ToArray(parameters, new string[parameters.Count]);
			}

			/// <summary>Check if a command is the setQuota command</summary>
			/// <param name="cmd">A string representation of a command starting with "-"</param>
			/// <returns>true if this is a count command; false otherwise</returns>
			public static bool Matches(string cmd)
			{
				return ("-" + Name).Equals(cmd);
			}

			public override string GetCommandName()
			{
				return Name;
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void Run(Path path)
			{
				dfs.SetQuota(path, quota, HdfsConstants.QuotaDontSet);
			}
		}

		/// <summary>A class that supports command clearSpaceQuota</summary>
		private class ClearSpaceQuotaCommand : DFSAdmin.DFSAdminCommand
		{
			private const string Name = "clrSpaceQuota";

			private const string Usage = "-" + Name + " [-storageType <storagetype>] <dirname>...<dirname>";

			private const string Description = Usage + ": " + "Clear the space quota for each directory <dirName>.\n"
				 + "\t\tFor each directory, attempt to clear the quota. An error will be reported if\n"
				 + "\t\t1. the directory does not exist or is a file, or\n" + "\t\t2. user is not an administrator.\n"
				 + "\t\tIt does not fault if the directory has no quota.\n" + "\t\tThe storage type specific quota is cleared when -storageType option is specified.";

			private StorageType type;

			/// <summary>Constructor</summary>
			internal ClearSpaceQuotaCommand(string[] args, int pos, FileSystem fs)
				: base(fs)
			{
				CommandFormat c = new CommandFormat(1, int.MaxValue);
				IList<string> parameters = c.Parse(args, pos);
				string storageTypeString = StringUtils.PopOptionWithArgument("-storageType", parameters
					);
				if (storageTypeString != null)
				{
					this.type = StorageType.ParseStorageType(storageTypeString);
				}
				this.args = Sharpen.Collections.ToArray(parameters, new string[parameters.Count]);
			}

			/// <summary>Check if a command is the clrQuota command</summary>
			/// <param name="cmd">A string representation of a command starting with "-"</param>
			/// <returns>true if this is a clrQuota command; false otherwise</returns>
			public static bool Matches(string cmd)
			{
				return ("-" + Name).Equals(cmd);
			}

			public override string GetCommandName()
			{
				return Name;
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void Run(Path path)
			{
				if (type != null)
				{
					dfs.SetQuotaByStorageType(path, type, HdfsConstants.QuotaReset);
				}
				else
				{
					dfs.SetQuota(path, HdfsConstants.QuotaDontSet, HdfsConstants.QuotaReset);
				}
			}
		}

		/// <summary>A class that supports command setQuota</summary>
		private class SetSpaceQuotaCommand : DFSAdmin.DFSAdminCommand
		{
			private const string Name = "setSpaceQuota";

			private const string Usage = "-" + Name + " <quota> [-storageType <storagetype>] <dirname>...<dirname>";

			private const string Description = Usage + ": " + "Set the space quota <quota> for each directory <dirName>.\n"
				 + "\t\tThe space quota is a long integer that puts a hard limit\n" + "\t\ton the total size of all the files under the directory tree.\n"
				 + "\t\tThe extra space required for replication is also counted. E.g.\n" + "\t\ta 1GB file with replication of 3 consumes 3GB of the quota.\n\n"
				 + "\t\tQuota can also be specified with a binary prefix for terabytes,\n" + "\t\tpetabytes etc (e.g. 50t is 50TB, 5m is 5MB, 3p is 3PB).\n"
				 + "\t\tFor each directory, attempt to set the quota. An error will be reported if\n"
				 + "\t\t1. N is not a positive integer, or\n" + "\t\t2. user is not an administrator, or\n"
				 + "\t\t3. the directory does not exist or is a file.\n" + "\t\tThe storage type specific quota is set when -storageType option is specified.\n";

			private long quota;

			private StorageType type;

			/// <summary>Constructor</summary>
			internal SetSpaceQuotaCommand(string[] args, int pos, FileSystem fs)
				: base(fs)
			{
				// the quota to be set
				CommandFormat c = new CommandFormat(2, int.MaxValue);
				IList<string> parameters = c.Parse(args, pos);
				string str = parameters.Remove(0).Trim();
				try
				{
					quota = StringUtils.TraditionalBinaryPrefix.String2long(str);
				}
				catch (FormatException)
				{
					throw new ArgumentException("\"" + str + "\" is not a valid value for a quota.");
				}
				string storageTypeString = StringUtils.PopOptionWithArgument("-storageType", parameters
					);
				if (storageTypeString != null)
				{
					this.type = StorageType.ParseStorageType(storageTypeString);
				}
				this.args = Sharpen.Collections.ToArray(parameters, new string[parameters.Count]);
			}

			/// <summary>Check if a command is the setQuota command</summary>
			/// <param name="cmd">A string representation of a command starting with "-"</param>
			/// <returns>true if this is a count command; false otherwise</returns>
			public static bool Matches(string cmd)
			{
				return ("-" + Name).Equals(cmd);
			}

			public override string GetCommandName()
			{
				return Name;
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void Run(Path path)
			{
				if (type != null)
				{
					dfs.SetQuotaByStorageType(path, type, quota);
				}
				else
				{
					dfs.SetQuota(path, HdfsConstants.QuotaDontSet, quota);
				}
			}
		}

		private class RollingUpgradeCommand
		{
			internal const string Name = "rollingUpgrade";

			internal const string Usage = "-" + Name + " [<query|prepare|finalize>]";

			internal const string Description = Usage + ":\n" + "     query: query the current rolling upgrade status.\n"
				 + "   prepare: prepare a new rolling upgrade.\n" + "  finalize: finalize the current rolling upgrade.";

			/// <summary>Check if a command is the rollingUpgrade command</summary>
			/// <param name="cmd">A string representation of a command starting with "-"</param>
			/// <returns>true if this is a clrQuota command; false otherwise</returns>
			internal static bool Matches(string cmd)
			{
				return ("-" + Name).Equals(cmd);
			}

			private static void PrintMessage(RollingUpgradeInfo info, TextWriter @out)
			{
				if (info != null && info.IsStarted())
				{
					if (!info.CreatedRollbackImages() && !info.IsFinalized())
					{
						@out.WriteLine("Preparing for upgrade. Data is being saved for rollback." + "\nRun \"dfsadmin -rollingUpgrade query\" to check the status"
							 + "\nfor proceeding with rolling upgrade");
						@out.WriteLine(info);
					}
					else
					{
						if (!info.IsFinalized())
						{
							@out.WriteLine("Proceed with rolling upgrade:");
							@out.WriteLine(info);
						}
						else
						{
							@out.WriteLine("Rolling upgrade is finalized.");
							@out.WriteLine(info);
						}
					}
				}
				else
				{
					@out.WriteLine("There is no rolling upgrade in progress or rolling " + "upgrade has already been finalized."
						);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal static int Run(DistributedFileSystem dfs, string[] argv, int idx)
			{
				HdfsConstants.RollingUpgradeAction action = HdfsConstants.RollingUpgradeAction.FromString
					(argv.Length >= 2 ? argv[1] : string.Empty);
				if (action == null)
				{
					throw new ArgumentException("Failed to covert \"" + argv[1] + "\" to " + typeof(HdfsConstants.RollingUpgradeAction
						).Name);
				}
				System.Console.Out.WriteLine(action + " rolling upgrade ...");
				RollingUpgradeInfo info = dfs.RollingUpgrade(action);
				switch (action)
				{
					case HdfsConstants.RollingUpgradeAction.Query:
					{
						break;
					}

					case HdfsConstants.RollingUpgradeAction.Prepare:
					{
						Preconditions.CheckState(info.IsStarted());
						break;
					}

					case HdfsConstants.RollingUpgradeAction.Finalize:
					{
						Preconditions.CheckState(info == null || info.IsFinalized());
						break;
					}
				}
				PrintMessage(info, System.Console.Out);
				return 0;
			}
		}

		/// <summary>
		/// Common usage summary shared between "hdfs dfsadmin -help" and
		/// "hdfs dfsadmin"
		/// </summary>
		private const string commonUsageSummary = "\t[-report [-live] [-dead] [-decommissioning]]\n"
			 + "\t[-safemode <enter | leave | get | wait>]\n" + "\t[-saveNamespace]\n" + "\t[-rollEdits]\n"
			 + "\t[-restoreFailedStorage true|false|check]\n" + "\t[-refreshNodes]\n" + "\t["
			 + DFSAdmin.SetQuotaCommand.Usage + "]\n" + "\t[" + DFSAdmin.ClearQuotaCommand.Usage
			 + "]\n" + "\t[" + DFSAdmin.SetSpaceQuotaCommand.Usage + "]\n" + "\t[" + DFSAdmin.ClearSpaceQuotaCommand
			.Usage + "]\n" + "\t[-finalizeUpgrade]\n" + "\t[" + DFSAdmin.RollingUpgradeCommand
			.Usage + "]\n" + "\t[-refreshServiceAcl]\n" + "\t[-refreshUserToGroupsMappings]\n"
			 + "\t[-refreshSuperUserGroupsConfiguration]\n" + "\t[-refreshCallQueue]\n" + "\t[-refresh <host:ipc_port> <key> [arg1..argn]\n"
			 + "\t[-reconfig <datanode|...> <host:ipc_port> <start|status>]\n" + "\t[-printTopology]\n"
			 + "\t[-refreshNamenodes datanode_host:ipc_port]\n" + "\t[-deleteBlockPool datanode_host:ipc_port blockpoolId [force]]\n"
			 + "\t[-setBalancerBandwidth <bandwidth in bytes per second>]\n" + "\t[-fetchImage <local directory>]\n"
			 + "\t[-allowSnapshot <snapshotDir>]\n" + "\t[-disallowSnapshot <snapshotDir>]\n"
			 + "\t[-shutdownDatanode <datanode_host:ipc_port> [upgrade]]\n" + "\t[-getDatanodeInfo <datanode_host:ipc_port>]\n"
			 + "\t[-metasave filename]\n" + "\t[-triggerBlockReport [-incremental] <datanode_host:ipc_port>]\n"
			 + "\t[-help [cmd]]\n";

		/// <summary>Construct a DFSAdmin object.</summary>
		public DFSAdmin()
			: this(new HdfsConfiguration())
		{
		}

		/// <summary>Construct a DFSAdmin object.</summary>
		public DFSAdmin(Configuration conf)
			: base(conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual DistributedFileSystem GetDFS()
		{
			FileSystem fs = GetFS();
			if (!(fs is DistributedFileSystem))
			{
				throw new ArgumentException("FileSystem " + fs.GetUri() + " is not an HDFS file system"
					);
			}
			return (DistributedFileSystem)fs;
		}

		/// <summary>Gives a report on how the FileSystem is doing.</summary>
		/// <exception>
		/// IOException
		/// if the filesystem does not exist.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Report(string[] argv, int i)
		{
			DistributedFileSystem dfs = GetDFS();
			FsStatus ds = dfs.GetStatus();
			long capacity = ds.GetCapacity();
			long used = ds.GetUsed();
			long remaining = ds.GetRemaining();
			long presentCapacity = used + remaining;
			bool mode = dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeGet);
			if (mode)
			{
				System.Console.Out.WriteLine("Safe mode is ON");
			}
			System.Console.Out.WriteLine("Configured Capacity: " + capacity + " (" + StringUtils
				.ByteDesc(capacity) + ")");
			System.Console.Out.WriteLine("Present Capacity: " + presentCapacity + " (" + StringUtils
				.ByteDesc(presentCapacity) + ")");
			System.Console.Out.WriteLine("DFS Remaining: " + remaining + " (" + StringUtils.ByteDesc
				(remaining) + ")");
			System.Console.Out.WriteLine("DFS Used: " + used + " (" + StringUtils.ByteDesc(used
				) + ")");
			System.Console.Out.WriteLine("DFS Used%: " + StringUtils.FormatPercent(used / (double
				)presentCapacity, 2));
			/* These counts are not always upto date. They are updated after
			* iteration of an internal list. Should be updated in a few seconds to
			* minutes. Use "-metaSave" to list of all such blocks and accurate
			* counts.
			*/
			System.Console.Out.WriteLine("Under replicated blocks: " + dfs.GetUnderReplicatedBlocksCount
				());
			System.Console.Out.WriteLine("Blocks with corrupt replicas: " + dfs.GetCorruptBlocksCount
				());
			System.Console.Out.WriteLine("Missing blocks: " + dfs.GetMissingBlocksCount());
			System.Console.Out.WriteLine("Missing blocks (with replication factor 1): " + dfs
				.GetMissingReplOneBlocksCount());
			System.Console.Out.WriteLine();
			System.Console.Out.WriteLine("-------------------------------------------------");
			// Parse arguments for filtering the node list
			IList<string> args = Arrays.AsList(argv);
			// Truncate already handled arguments before parsing report()-specific ones
			args = new AList<string>(args.SubList(i, args.Count));
			bool listLive = StringUtils.PopOption("-live", args);
			bool listDead = StringUtils.PopOption("-dead", args);
			bool listDecommissioning = StringUtils.PopOption("-decommissioning", args);
			// If no filter flags are found, then list all DN types
			bool listAll = (!listLive && !listDead && !listDecommissioning);
			if (listAll || listLive)
			{
				DatanodeInfo[] live = dfs.GetDataNodeStats(HdfsConstants.DatanodeReportType.Live);
				if (live.Length > 0 || listLive)
				{
					System.Console.Out.WriteLine("Live datanodes (" + live.Length + "):\n");
				}
				if (live.Length > 0)
				{
					foreach (DatanodeInfo dn in live)
					{
						System.Console.Out.WriteLine(dn.GetDatanodeReport());
						System.Console.Out.WriteLine();
					}
				}
			}
			if (listAll || listDead)
			{
				DatanodeInfo[] dead = dfs.GetDataNodeStats(HdfsConstants.DatanodeReportType.Dead);
				if (dead.Length > 0 || listDead)
				{
					System.Console.Out.WriteLine("Dead datanodes (" + dead.Length + "):\n");
				}
				if (dead.Length > 0)
				{
					foreach (DatanodeInfo dn in dead)
					{
						System.Console.Out.WriteLine(dn.GetDatanodeReport());
						System.Console.Out.WriteLine();
					}
				}
			}
			if (listAll || listDecommissioning)
			{
				DatanodeInfo[] decom = dfs.GetDataNodeStats(HdfsConstants.DatanodeReportType.Decommissioning
					);
				if (decom.Length > 0 || listDecommissioning)
				{
					System.Console.Out.WriteLine("Decommissioning datanodes (" + decom.Length + "):\n"
						);
				}
				if (decom.Length > 0)
				{
					foreach (DatanodeInfo dn in decom)
					{
						System.Console.Out.WriteLine(dn.GetDatanodeReport());
						System.Console.Out.WriteLine();
					}
				}
			}
		}

		/// <summary>Safe mode maintenance command.</summary>
		/// <remarks>
		/// Safe mode maintenance command.
		/// Usage: hdfs dfsadmin -safemode [enter | leave | get]
		/// </remarks>
		/// <param name="argv">List of of command line parameters.</param>
		/// <param name="idx">The index of the command that is being processed.</param>
		/// <exception>
		/// IOException
		/// if the filesystem does not exist.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetSafeMode(string[] argv, int idx)
		{
			if (idx != argv.Length - 1)
			{
				PrintUsage("-safemode");
				return;
			}
			HdfsConstants.SafeModeAction action;
			bool waitExitSafe = false;
			if (Sharpen.Runtime.EqualsIgnoreCase("leave", argv[idx]))
			{
				action = HdfsConstants.SafeModeAction.SafemodeLeave;
			}
			else
			{
				if (Sharpen.Runtime.EqualsIgnoreCase("enter", argv[idx]))
				{
					action = HdfsConstants.SafeModeAction.SafemodeEnter;
				}
				else
				{
					if (Sharpen.Runtime.EqualsIgnoreCase("get", argv[idx]))
					{
						action = HdfsConstants.SafeModeAction.SafemodeGet;
					}
					else
					{
						if (Sharpen.Runtime.EqualsIgnoreCase("wait", argv[idx]))
						{
							action = HdfsConstants.SafeModeAction.SafemodeGet;
							waitExitSafe = true;
						}
						else
						{
							PrintUsage("-safemode");
							return;
						}
					}
				}
			}
			DistributedFileSystem dfs = GetDFS();
			Configuration dfsConf = dfs.GetConf();
			URI dfsUri = dfs.GetUri();
			bool isHaEnabled = HAUtil.IsLogicalUri(dfsConf, dfsUri);
			if (isHaEnabled)
			{
				string nsId = dfsUri.GetHost();
				IList<NameNodeProxies.ProxyAndInfo<ClientProtocol>> proxies = HAUtil.GetProxiesForAllNameNodesInNameservice
					<ClientProtocol>(dfsConf, nsId);
				foreach (NameNodeProxies.ProxyAndInfo<ClientProtocol> proxy in proxies)
				{
					ClientProtocol haNn = proxy.GetProxy();
					bool inSafeMode = haNn.SetSafeMode(action, false);
					if (waitExitSafe)
					{
						inSafeMode = WaitExitSafeMode(haNn, inSafeMode);
					}
					System.Console.Out.WriteLine("Safe mode is " + (inSafeMode ? "ON" : "OFF") + " in "
						 + proxy.GetAddress());
				}
			}
			else
			{
				bool inSafeMode = dfs.SetSafeMode(action);
				if (waitExitSafe)
				{
					inSafeMode = WaitExitSafeMode(dfs, inSafeMode);
				}
				System.Console.Out.WriteLine("Safe mode is " + (inSafeMode ? "ON" : "OFF"));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private bool WaitExitSafeMode(DistributedFileSystem dfs, bool inSafeMode)
		{
			while (inSafeMode)
			{
				try
				{
					Sharpen.Thread.Sleep(5000);
				}
				catch (Exception)
				{
					throw new IOException("Wait Interrupted");
				}
				inSafeMode = dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeGet, false);
			}
			return inSafeMode;
		}

		/// <exception cref="System.IO.IOException"/>
		private bool WaitExitSafeMode(ClientProtocol nn, bool inSafeMode)
		{
			while (inSafeMode)
			{
				try
				{
					Sharpen.Thread.Sleep(5000);
				}
				catch (Exception)
				{
					throw new IOException("Wait Interrupted");
				}
				inSafeMode = nn.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeGet, false);
			}
			return inSafeMode;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int TriggerBlockReport(string[] argv)
		{
			IList<string> args = new List<string>();
			for (int j = 1; j < argv.Length; j++)
			{
				args.AddItem(argv[j]);
			}
			bool incremental = StringUtils.PopOption("-incremental", args);
			string hostPort = StringUtils.PopFirstNonOption(args);
			if (hostPort == null)
			{
				System.Console.Error.WriteLine("You must specify a host:port pair.");
				return 1;
			}
			if (!args.IsEmpty())
			{
				System.Console.Error.Write("Can't understand arguments: " + Joiner.On(" ").Join(args
					) + "\n");
				return 1;
			}
			ClientDatanodeProtocol dnProxy = GetDataNodeProxy(hostPort);
			try
			{
				dnProxy.TriggerBlockReport(new BlockReportOptions.Factory().SetIncremental(incremental
					).Build());
			}
			catch (IOException e)
			{
				System.Console.Error.WriteLine("triggerBlockReport error: " + e);
				return 1;
			}
			System.Console.Out.WriteLine("Triggering " + (incremental ? "an incremental " : "a full "
				) + "block report on " + hostPort + ".");
			return 0;
		}

		/// <summary>Allow snapshot on a directory.</summary>
		/// <remarks>
		/// Allow snapshot on a directory.
		/// Usage: hdfs dfsadmin -allowSnapshot snapshotDir
		/// </remarks>
		/// <param name="argv">List of of command line parameters.</param>
		/// <exception>IOException</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AllowSnapshot(string[] argv)
		{
			DistributedFileSystem dfs = GetDFS();
			try
			{
				dfs.AllowSnapshot(new Path(argv[1]));
			}
			catch (SnapshotException e)
			{
				throw new RemoteException(e.GetType().FullName, e.Message);
			}
			System.Console.Out.WriteLine("Allowing snaphot on " + argv[1] + " succeeded");
		}

		/// <summary>Allow snapshot on a directory.</summary>
		/// <remarks>
		/// Allow snapshot on a directory.
		/// Usage: hdfs dfsadmin -disallowSnapshot snapshotDir
		/// </remarks>
		/// <param name="argv">List of of command line parameters.</param>
		/// <exception>IOException</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void DisallowSnapshot(string[] argv)
		{
			DistributedFileSystem dfs = GetDFS();
			try
			{
				dfs.DisallowSnapshot(new Path(argv[1]));
			}
			catch (SnapshotException e)
			{
				throw new RemoteException(e.GetType().FullName, e.Message);
			}
			System.Console.Out.WriteLine("Disallowing snaphot on " + argv[1] + " succeeded");
		}

		/// <summary>Command to ask the namenode to save the namespace.</summary>
		/// <remarks>
		/// Command to ask the namenode to save the namespace.
		/// Usage: hdfs dfsadmin -saveNamespace
		/// </remarks>
		/// <exception>
		/// IOException
		/// 
		/// </exception>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SaveNamespace()"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual int SaveNamespace()
		{
			int exitCode = -1;
			DistributedFileSystem dfs = GetDFS();
			Configuration dfsConf = dfs.GetConf();
			URI dfsUri = dfs.GetUri();
			bool isHaEnabled = HAUtil.IsLogicalUri(dfsConf, dfsUri);
			if (isHaEnabled)
			{
				string nsId = dfsUri.GetHost();
				IList<NameNodeProxies.ProxyAndInfo<ClientProtocol>> proxies = HAUtil.GetProxiesForAllNameNodesInNameservice
					<ClientProtocol>(dfsConf, nsId);
				foreach (NameNodeProxies.ProxyAndInfo<ClientProtocol> proxy in proxies)
				{
					proxy.GetProxy().SaveNamespace();
					System.Console.Out.WriteLine("Save namespace successful for " + proxy.GetAddress(
						));
				}
			}
			else
			{
				dfs.SaveNamespace();
				System.Console.Out.WriteLine("Save namespace successful");
			}
			exitCode = 0;
			return exitCode;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int RollEdits()
		{
			DistributedFileSystem dfs = GetDFS();
			long txid = dfs.RollEdits();
			System.Console.Out.WriteLine("Successfully rolled edit logs.");
			System.Console.Out.WriteLine("New segment starts at txid " + txid);
			return 0;
		}

		/// <summary>Command to enable/disable/check restoring of failed storage replicas in the namenode.
		/// 	</summary>
		/// <remarks>
		/// Command to enable/disable/check restoring of failed storage replicas in the namenode.
		/// Usage: hdfs dfsadmin -restoreFailedStorage true|false|check
		/// </remarks>
		/// <exception>
		/// IOException
		/// 
		/// </exception>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RestoreFailedStorage(string)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual int RestoreFailedStorage(string arg)
		{
			int exitCode = -1;
			if (!arg.Equals("check") && !arg.Equals("true") && !arg.Equals("false"))
			{
				System.Console.Error.WriteLine("restoreFailedStorage valid args are true|false|check"
					);
				return exitCode;
			}
			DistributedFileSystem dfs = GetDFS();
			Configuration dfsConf = dfs.GetConf();
			URI dfsUri = dfs.GetUri();
			bool isHaEnabled = HAUtil.IsLogicalUri(dfsConf, dfsUri);
			if (isHaEnabled)
			{
				string nsId = dfsUri.GetHost();
				IList<NameNodeProxies.ProxyAndInfo<ClientProtocol>> proxies = HAUtil.GetProxiesForAllNameNodesInNameservice
					<ClientProtocol>(dfsConf, nsId);
				foreach (NameNodeProxies.ProxyAndInfo<ClientProtocol> proxy in proxies)
				{
					bool res = proxy.GetProxy().RestoreFailedStorage(arg);
					System.Console.Out.WriteLine("restoreFailedStorage is set to " + res + " for " + 
						proxy.GetAddress());
				}
			}
			else
			{
				bool res = dfs.RestoreFailedStorage(arg);
				System.Console.Out.WriteLine("restoreFailedStorage is set to " + res);
			}
			exitCode = 0;
			return exitCode;
		}

		/// <summary>
		/// Command to ask the namenode to reread the hosts and excluded hosts
		/// file.
		/// </summary>
		/// <remarks>
		/// Command to ask the namenode to reread the hosts and excluded hosts
		/// file.
		/// Usage: hdfs dfsadmin -refreshNodes
		/// </remarks>
		/// <exception>
		/// IOException
		/// 
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual int RefreshNodes()
		{
			int exitCode = -1;
			DistributedFileSystem dfs = GetDFS();
			Configuration dfsConf = dfs.GetConf();
			URI dfsUri = dfs.GetUri();
			bool isHaEnabled = HAUtil.IsLogicalUri(dfsConf, dfsUri);
			if (isHaEnabled)
			{
				string nsId = dfsUri.GetHost();
				IList<NameNodeProxies.ProxyAndInfo<ClientProtocol>> proxies = HAUtil.GetProxiesForAllNameNodesInNameservice
					<ClientProtocol>(dfsConf, nsId);
				foreach (NameNodeProxies.ProxyAndInfo<ClientProtocol> proxy in proxies)
				{
					proxy.GetProxy().RefreshNodes();
					System.Console.Out.WriteLine("Refresh nodes successful for " + proxy.GetAddress()
						);
				}
			}
			else
			{
				dfs.RefreshNodes();
				System.Console.Out.WriteLine("Refresh nodes successful");
			}
			exitCode = 0;
			return exitCode;
		}

		/// <summary>
		/// Command to ask the namenode to set the balancer bandwidth for all of the
		/// datanodes.
		/// </summary>
		/// <remarks>
		/// Command to ask the namenode to set the balancer bandwidth for all of the
		/// datanodes.
		/// Usage: hdfs dfsadmin -setBalancerBandwidth bandwidth
		/// </remarks>
		/// <param name="argv">List of of command line parameters.</param>
		/// <param name="idx">The index of the command that is being processed.</param>
		/// <exception>
		/// IOException
		/// 
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual int SetBalancerBandwidth(string[] argv, int idx)
		{
			long bandwidth;
			int exitCode = -1;
			try
			{
				bandwidth = long.Parse(argv[idx]);
			}
			catch (FormatException nfe)
			{
				System.Console.Error.WriteLine("NumberFormatException: " + nfe.Message);
				System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-setBalancerBandwidth <bandwidth in bytes per second>]"
					);
				return exitCode;
			}
			FileSystem fs = GetFS();
			if (!(fs is DistributedFileSystem))
			{
				System.Console.Error.WriteLine("FileSystem is " + fs.GetUri());
				return exitCode;
			}
			DistributedFileSystem dfs = (DistributedFileSystem)fs;
			Configuration dfsConf = dfs.GetConf();
			URI dfsUri = dfs.GetUri();
			bool isHaEnabled = HAUtil.IsLogicalUri(dfsConf, dfsUri);
			if (isHaEnabled)
			{
				string nsId = dfsUri.GetHost();
				IList<NameNodeProxies.ProxyAndInfo<ClientProtocol>> proxies = HAUtil.GetProxiesForAllNameNodesInNameservice
					<ClientProtocol>(dfsConf, nsId);
				foreach (NameNodeProxies.ProxyAndInfo<ClientProtocol> proxy in proxies)
				{
					proxy.GetProxy().SetBalancerBandwidth(bandwidth);
					System.Console.Out.WriteLine("Balancer bandwidth is set to " + bandwidth + " for "
						 + proxy.GetAddress());
				}
			}
			else
			{
				dfs.SetBalancerBandwidth(bandwidth);
				System.Console.Out.WriteLine("Balancer bandwidth is set to " + bandwidth);
			}
			exitCode = 0;
			return exitCode;
		}

		/// <summary>
		/// Download the most recent fsimage from the name node, and save it to a local
		/// file in the given directory.
		/// </summary>
		/// <param name="argv">List of of command line parameters.</param>
		/// <param name="idx">The index of the command that is being processed.</param>
		/// <returns>an exit code indicating success or failure.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual int FetchImage(string[] argv, int idx)
		{
			Configuration conf = GetConf();
			Uri infoServer = DFSUtil.GetInfoServer(HAUtil.GetAddressOfActive(GetDFS()), conf, 
				DFSUtil.GetHttpClientScheme(conf)).ToURL();
			SecurityUtil.DoAsCurrentUser(new _PrivilegedExceptionAction_872(infoServer, argv, 
				idx));
			return 0;
		}

		private sealed class _PrivilegedExceptionAction_872 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_872(Uri infoServer, string[] argv, int idx)
			{
				this.infoServer = infoServer;
				this.argv = argv;
				this.idx = idx;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				TransferFsImage.DownloadMostRecentImageToDirectory(infoServer, new FilePath(argv[
					idx]));
				return null;
			}

			private readonly Uri infoServer;

			private readonly string[] argv;

			private readonly int idx;
		}

		private void PrintHelp(string cmd)
		{
			string summary = "hdfs dfsadmin performs DFS administrative commands.\n" + "Note: Administrative commands can only be run with superuser permission.\n"
				 + "The full syntax is: \n\n" + "hdfs dfsadmin\n" + commonUsageSummary;
			string report = "-report [-live] [-dead] [-decommissioning]:\n" + "\tReports basic filesystem information and statistics.\n"
				 + "\tOptional flags may be used to filter the list of displayed DNs.\n";
			string safemode = "-safemode <enter|leave|get|wait>:  Safe mode maintenance command.\n"
				 + "\t\tSafe mode is a Namenode state in which it\n" + "\t\t\t1.  does not accept changes to the name space (read-only)\n"
				 + "\t\t\t2.  does not replicate or delete blocks.\n" + "\t\tSafe mode is entered automatically at Namenode startup, and\n"
				 + "\t\tleaves safe mode automatically when the configured minimum\n" + "\t\tpercentage of blocks satisfies the minimum replication\n"
				 + "\t\tcondition.  Safe mode can also be entered manually, but then\n" + "\t\tit can only be turned off manually as well.\n";
			string saveNamespace = "-saveNamespace:\t" + "Save current namespace into storage directories and reset edits log.\n"
				 + "\t\tRequires safe mode.\n";
			string rollEdits = "-rollEdits:\t" + "Rolls the edit log.\n";
			string restoreFailedStorage = "-restoreFailedStorage:\t" + "Set/Unset/Check flag to attempt restore of failed storage replicas if they become available.\n";
			string refreshNodes = "-refreshNodes: \tUpdates the namenode with the " + "set of datanodes allowed to connect to the namenode.\n\n"
				 + "\t\tNamenode re-reads datanode hostnames from the file defined by \n" + "\t\tdfs.hosts, dfs.hosts.exclude configuration parameters.\n"
				 + "\t\tHosts defined in dfs.hosts are the datanodes that are part of \n" + "\t\tthe cluster. If there are entries in dfs.hosts, only the hosts \n"
				 + "\t\tin it are allowed to register with the namenode.\n\n" + "\t\tEntries in dfs.hosts.exclude are datanodes that need to be \n"
				 + "\t\tdecommissioned. Datanodes complete decommissioning when \n" + "\t\tall the replicas from them are replicated to other datanodes.\n"
				 + "\t\tDecommissioned nodes are not automatically shutdown and \n" + "\t\tare not chosen for writing new replicas.\n";
			string finalizeUpgrade = "-finalizeUpgrade: Finalize upgrade of HDFS.\n" + "\t\tDatanodes delete their previous version working directories,\n"
				 + "\t\tfollowed by Namenode doing the same.\n" + "\t\tThis completes the upgrade process.\n";
			string metaSave = "-metasave <filename>: \tSave Namenode's primary data structures\n"
				 + "\t\tto <filename> in the directory specified by hadoop.log.dir property.\n" 
				+ "\t\t<filename> is overwritten if it exists.\n" + "\t\t<filename> will contain one line for each of the following\n"
				 + "\t\t\t1. Datanodes heart beating with Namenode\n" + "\t\t\t2. Blocks waiting to be replicated\n"
				 + "\t\t\t3. Blocks currrently being replicated\n" + "\t\t\t4. Blocks waiting to be deleted\n";
			string refreshServiceAcl = "-refreshServiceAcl: Reload the service-level authorization policy file\n"
				 + "\t\tNamenode will reload the authorization policy file.\n";
			string refreshUserToGroupsMappings = "-refreshUserToGroupsMappings: Refresh user-to-groups mappings\n";
			string refreshSuperUserGroupsConfiguration = "-refreshSuperUserGroupsConfiguration: Refresh superuser proxy groups mappings\n";
			string refreshCallQueue = "-refreshCallQueue: Reload the call queue from config\n";
			string reconfig = "-reconfig <datanode|...> <host:ipc_port> <start|status>:\n" + 
				"\tStarts reconfiguration or gets the status of an ongoing reconfiguration.\n" +
				 "\tThe second parameter specifies the node type.\n" + "\tCurrently, only reloading DataNode's configuration is supported.\n";
			string genericRefresh = "-refresh: Arguments are <hostname:port> <resource_identifier> [arg1..argn]\n"
				 + "\tTriggers a runtime-refresh of the resource specified by <resource_identifier>\n"
				 + "\ton <hostname:port>. All other args after are sent to the host.\n";
			string printTopology = "-printTopology: Print a tree of the racks and their\n" + 
				"\t\tnodes as reported by the Namenode\n";
			string refreshNamenodes = "-refreshNamenodes: Takes a datanodehost:port as argument,\n"
				 + "\t\tFor the given datanode, reloads the configuration files,\n" + "\t\tstops serving the removed block-pools\n"
				 + "\t\tand starts serving new block-pools\n";
			string deleteBlockPool = "-deleteBlockPool: Arguments are datanodehost:port, blockpool id\n"
				 + "\t\t and an optional argument \"force\". If force is passed,\n" + "\t\t block pool directory for the given blockpool id on the given\n"
				 + "\t\t datanode is deleted along with its contents, otherwise\n" + "\t\t the directory is deleted only if it is empty. The command\n"
				 + "\t\t will fail if datanode is still serving the block pool.\n" + "\t\t   Refer to refreshNamenodes to shutdown a block pool\n"
				 + "\t\t service on a datanode.\n";
			string setBalancerBandwidth = "-setBalancerBandwidth <bandwidth>:\n" + "\tChanges the network bandwidth used by each datanode during\n"
				 + "\tHDFS block balancing.\n\n" + "\t\t<bandwidth> is the maximum number of bytes per second\n"
				 + "\t\tthat will be used by each datanode. This value overrides\n" + "\t\tthe dfs.balance.bandwidthPerSec parameter.\n\n"
				 + "\t\t--- NOTE: The new value is not persistent on the DataNode.---\n";
			string fetchImage = "-fetchImage <local directory>:\n" + "\tDownloads the most recent fsimage from the Name Node and saves it in"
				 + "\tthe specified local directory.\n";
			string allowSnapshot = "-allowSnapshot <snapshotDir>:\n" + "\tAllow snapshots to be taken on a directory.\n";
			string disallowSnapshot = "-disallowSnapshot <snapshotDir>:\n" + "\tDo not allow snapshots to be taken on a directory any more.\n";
			string shutdownDatanode = "-shutdownDatanode <datanode_host:ipc_port> [upgrade]\n"
				 + "\tSubmit a shutdown request for the given datanode. If an optional\n" + "\t\"upgrade\" argument is specified, clients accessing the datanode\n"
				 + "\twill be advised to wait for it to restart and the fast start-up\n" + "\tmode will be enabled. When the restart does not happen in time,\n"
				 + "\tclients will timeout and ignore the datanode. In such case, the\n" + "\tfast start-up mode will also be disabled.\n";
			string getDatanodeInfo = "-getDatanodeInfo <datanode_host:ipc_port>\n" + "\tGet the information about the given datanode. This command can\n"
				 + "\tbe used for checking if a datanode is alive.\n";
			string triggerBlockReport = "-triggerBlockReport [-incremental] <datanode_host:ipc_port>\n"
				 + "\tTrigger a block report for the datanode.\n" + "\tIf 'incremental' is specified, it will be an incremental\n"
				 + "\tblock report; otherwise, it will be a full block report.\n";
			string help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n"
				 + "\t\tis specified.\n";
			if ("report".Equals(cmd))
			{
				System.Console.Out.WriteLine(report);
			}
			else
			{
				if ("safemode".Equals(cmd))
				{
					System.Console.Out.WriteLine(safemode);
				}
				else
				{
					if ("saveNamespace".Equals(cmd))
					{
						System.Console.Out.WriteLine(saveNamespace);
					}
					else
					{
						if ("rollEdits".Equals(cmd))
						{
							System.Console.Out.WriteLine(rollEdits);
						}
						else
						{
							if ("restoreFailedStorage".Equals(cmd))
							{
								System.Console.Out.WriteLine(restoreFailedStorage);
							}
							else
							{
								if ("refreshNodes".Equals(cmd))
								{
									System.Console.Out.WriteLine(refreshNodes);
								}
								else
								{
									if ("finalizeUpgrade".Equals(cmd))
									{
										System.Console.Out.WriteLine(finalizeUpgrade);
									}
									else
									{
										if (DFSAdmin.RollingUpgradeCommand.Matches("-" + cmd))
										{
											System.Console.Out.WriteLine(DFSAdmin.RollingUpgradeCommand.Description);
										}
										else
										{
											if ("metasave".Equals(cmd))
											{
												System.Console.Out.WriteLine(metaSave);
											}
											else
											{
												if (DFSAdmin.SetQuotaCommand.Matches("-" + cmd))
												{
													System.Console.Out.WriteLine(DFSAdmin.SetQuotaCommand.Description);
												}
												else
												{
													if (DFSAdmin.ClearQuotaCommand.Matches("-" + cmd))
													{
														System.Console.Out.WriteLine(DFSAdmin.ClearQuotaCommand.Description);
													}
													else
													{
														if (DFSAdmin.SetSpaceQuotaCommand.Matches("-" + cmd))
														{
															System.Console.Out.WriteLine(DFSAdmin.SetSpaceQuotaCommand.Description);
														}
														else
														{
															if (DFSAdmin.ClearSpaceQuotaCommand.Matches("-" + cmd))
															{
																System.Console.Out.WriteLine(DFSAdmin.ClearSpaceQuotaCommand.Description);
															}
															else
															{
																if ("refreshServiceAcl".Equals(cmd))
																{
																	System.Console.Out.WriteLine(refreshServiceAcl);
																}
																else
																{
																	if ("refreshUserToGroupsMappings".Equals(cmd))
																	{
																		System.Console.Out.WriteLine(refreshUserToGroupsMappings);
																	}
																	else
																	{
																		if ("refreshSuperUserGroupsConfiguration".Equals(cmd))
																		{
																			System.Console.Out.WriteLine(refreshSuperUserGroupsConfiguration);
																		}
																		else
																		{
																			if ("refreshCallQueue".Equals(cmd))
																			{
																				System.Console.Out.WriteLine(refreshCallQueue);
																			}
																			else
																			{
																				if ("refresh".Equals(cmd))
																				{
																					System.Console.Out.WriteLine(genericRefresh);
																				}
																				else
																				{
																					if ("reconfig".Equals(cmd))
																					{
																						System.Console.Out.WriteLine(reconfig);
																					}
																					else
																					{
																						if ("printTopology".Equals(cmd))
																						{
																							System.Console.Out.WriteLine(printTopology);
																						}
																						else
																						{
																							if ("refreshNamenodes".Equals(cmd))
																							{
																								System.Console.Out.WriteLine(refreshNamenodes);
																							}
																							else
																							{
																								if ("deleteBlockPool".Equals(cmd))
																								{
																									System.Console.Out.WriteLine(deleteBlockPool);
																								}
																								else
																								{
																									if ("setBalancerBandwidth".Equals(cmd))
																									{
																										System.Console.Out.WriteLine(setBalancerBandwidth);
																									}
																									else
																									{
																										if ("fetchImage".Equals(cmd))
																										{
																											System.Console.Out.WriteLine(fetchImage);
																										}
																										else
																										{
																											if (Sharpen.Runtime.EqualsIgnoreCase("allowSnapshot", cmd))
																											{
																												System.Console.Out.WriteLine(allowSnapshot);
																											}
																											else
																											{
																												if (Sharpen.Runtime.EqualsIgnoreCase("disallowSnapshot", cmd))
																												{
																													System.Console.Out.WriteLine(disallowSnapshot);
																												}
																												else
																												{
																													if (Sharpen.Runtime.EqualsIgnoreCase("shutdownDatanode", cmd))
																													{
																														System.Console.Out.WriteLine(shutdownDatanode);
																													}
																													else
																													{
																														if (Sharpen.Runtime.EqualsIgnoreCase("getDatanodeInfo", cmd))
																														{
																															System.Console.Out.WriteLine(getDatanodeInfo);
																														}
																														else
																														{
																															if ("help".Equals(cmd))
																															{
																																System.Console.Out.WriteLine(help);
																															}
																															else
																															{
																																System.Console.Out.WriteLine(summary);
																																System.Console.Out.WriteLine(report);
																																System.Console.Out.WriteLine(safemode);
																																System.Console.Out.WriteLine(saveNamespace);
																																System.Console.Out.WriteLine(rollEdits);
																																System.Console.Out.WriteLine(restoreFailedStorage);
																																System.Console.Out.WriteLine(refreshNodes);
																																System.Console.Out.WriteLine(finalizeUpgrade);
																																System.Console.Out.WriteLine(DFSAdmin.RollingUpgradeCommand.Description);
																																System.Console.Out.WriteLine(metaSave);
																																System.Console.Out.WriteLine(DFSAdmin.SetQuotaCommand.Description);
																																System.Console.Out.WriteLine(DFSAdmin.ClearQuotaCommand.Description);
																																System.Console.Out.WriteLine(DFSAdmin.SetSpaceQuotaCommand.Description);
																																System.Console.Out.WriteLine(DFSAdmin.ClearSpaceQuotaCommand.Description);
																																System.Console.Out.WriteLine(refreshServiceAcl);
																																System.Console.Out.WriteLine(refreshUserToGroupsMappings);
																																System.Console.Out.WriteLine(refreshSuperUserGroupsConfiguration);
																																System.Console.Out.WriteLine(refreshCallQueue);
																																System.Console.Out.WriteLine(genericRefresh);
																																System.Console.Out.WriteLine(reconfig);
																																System.Console.Out.WriteLine(printTopology);
																																System.Console.Out.WriteLine(refreshNamenodes);
																																System.Console.Out.WriteLine(deleteBlockPool);
																																System.Console.Out.WriteLine(setBalancerBandwidth);
																																System.Console.Out.WriteLine(fetchImage);
																																System.Console.Out.WriteLine(allowSnapshot);
																																System.Console.Out.WriteLine(disallowSnapshot);
																																System.Console.Out.WriteLine(shutdownDatanode);
																																System.Console.Out.WriteLine(getDatanodeInfo);
																																System.Console.Out.WriteLine(triggerBlockReport);
																																System.Console.Out.WriteLine(help);
																																System.Console.Out.WriteLine();
																																ToolRunner.PrintGenericCommandUsage(System.Console.Out);
																															}
																														}
																													}
																												}
																											}
																										}
																									}
																								}
																							}
																						}
																					}
																				}
																			}
																		}
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		/// <summary>Command to ask the namenode to finalize previously performed upgrade.</summary>
		/// <remarks>
		/// Command to ask the namenode to finalize previously performed upgrade.
		/// Usage: hdfs dfsadmin -finalizeUpgrade
		/// </remarks>
		/// <exception>
		/// IOException
		/// 
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual int FinalizeUpgrade()
		{
			DistributedFileSystem dfs = GetDFS();
			Configuration dfsConf = dfs.GetConf();
			URI dfsUri = dfs.GetUri();
			bool isHaAndLogicalUri = HAUtil.IsLogicalUri(dfsConf, dfsUri);
			if (isHaAndLogicalUri)
			{
				// In the case of HA and logical URI, run finalizeUpgrade for all
				// NNs in this nameservice.
				string nsId = dfsUri.GetHost();
				IList<ClientProtocol> namenodes = HAUtil.GetProxiesForAllNameNodesInNameservice(dfsConf
					, nsId);
				if (!HAUtil.IsAtLeastOneActive(namenodes))
				{
					throw new IOException("Cannot finalize with no NameNode active");
				}
				IList<NameNodeProxies.ProxyAndInfo<ClientProtocol>> proxies = HAUtil.GetProxiesForAllNameNodesInNameservice
					<ClientProtocol>(dfsConf, nsId);
				foreach (NameNodeProxies.ProxyAndInfo<ClientProtocol> proxy in proxies)
				{
					proxy.GetProxy().FinalizeUpgrade();
					System.Console.Out.WriteLine("Finalize upgrade successful for " + proxy.GetAddress
						());
				}
			}
			else
			{
				dfs.FinalizeUpgrade();
				System.Console.Out.WriteLine("Finalize upgrade successful");
			}
			return 0;
		}

		/// <summary>Dumps DFS data structures into specified file.</summary>
		/// <remarks>
		/// Dumps DFS data structures into specified file.
		/// Usage: hdfs dfsadmin -metasave filename
		/// </remarks>
		/// <param name="argv">List of of command line parameters.</param>
		/// <param name="idx">The index of the command that is being processed.</param>
		/// <exception>
		/// IOException
		/// if an error occurred while accessing
		/// the file or path.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual int MetaSave(string[] argv, int idx)
		{
			string pathname = argv[idx];
			DistributedFileSystem dfs = GetDFS();
			Configuration dfsConf = dfs.GetConf();
			URI dfsUri = dfs.GetUri();
			bool isHaEnabled = HAUtil.IsLogicalUri(dfsConf, dfsUri);
			if (isHaEnabled)
			{
				string nsId = dfsUri.GetHost();
				IList<NameNodeProxies.ProxyAndInfo<ClientProtocol>> proxies = HAUtil.GetProxiesForAllNameNodesInNameservice
					<ClientProtocol>(dfsConf, nsId);
				foreach (NameNodeProxies.ProxyAndInfo<ClientProtocol> proxy in proxies)
				{
					proxy.GetProxy().MetaSave(pathname);
					System.Console.Out.WriteLine("Created metasave file " + pathname + " in the log "
						 + "directory of namenode " + proxy.GetAddress());
				}
			}
			else
			{
				dfs.MetaSave(pathname);
				System.Console.Out.WriteLine("Created metasave file " + pathname + " in the log "
					 + "directory of namenode " + dfs.GetUri());
			}
			return 0;
		}

		/// <summary>
		/// Display each rack and the nodes assigned to that rack, as determined
		/// by the NameNode, in a hierarchical manner.
		/// </summary>
		/// <remarks>
		/// Display each rack and the nodes assigned to that rack, as determined
		/// by the NameNode, in a hierarchical manner.  The nodes and racks are
		/// sorted alphabetically.
		/// </remarks>
		/// <exception cref="System.IO.IOException">If an error while getting datanode report
		/// 	</exception>
		public virtual int PrintTopology()
		{
			DistributedFileSystem dfs = GetDFS();
			DatanodeInfo[] report = dfs.GetDataNodeStats();
			// Build a map of rack -> nodes from the datanode report
			Dictionary<string, TreeSet<string>> tree = new Dictionary<string, TreeSet<string>
				>();
			foreach (DatanodeInfo dni in report)
			{
				string location = dni.GetNetworkLocation();
				string name = dni.GetName();
				if (!tree.Contains(location))
				{
					tree[location] = new TreeSet<string>();
				}
				tree[location].AddItem(name);
			}
			// Sort the racks (and nodes) alphabetically, display in order
			AList<string> racks = new AList<string>(tree.Keys);
			racks.Sort();
			foreach (string r in racks)
			{
				System.Console.Out.WriteLine("Rack: " + r);
				TreeSet<string> nodes = tree[r];
				foreach (string n in nodes)
				{
					System.Console.Out.Write("   " + n);
					string hostname = NetUtils.GetHostNameOfIP(n);
					if (hostname != null)
					{
						System.Console.Out.Write(" (" + hostname + ")");
					}
					System.Console.Out.WriteLine();
				}
				System.Console.Out.WriteLine();
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private static UserGroupInformation GetUGI()
		{
			return UserGroupInformation.GetCurrentUser();
		}

		/// <summary>
		/// Refresh the authorization policy on the
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode"/>
		/// .
		/// </summary>
		/// <returns>exitcode 0 on success, non-zero on failure</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual int RefreshServiceAcl()
		{
			// Get the current configuration
			Configuration conf = GetConf();
			// for security authorization
			// server principal for this call   
			// should be NN's one.
			conf.Set(CommonConfigurationKeys.HadoopSecurityServiceUserNameKey, conf.Get(DFSConfigKeys
				.DfsNamenodeKerberosPrincipalKey, string.Empty));
			DistributedFileSystem dfs = GetDFS();
			URI dfsUri = dfs.GetUri();
			bool isHaEnabled = HAUtil.IsLogicalUri(conf, dfsUri);
			if (isHaEnabled)
			{
				// Run refreshServiceAcl for all NNs if HA is enabled
				string nsId = dfsUri.GetHost();
				IList<NameNodeProxies.ProxyAndInfo<RefreshAuthorizationPolicyProtocol>> proxies = 
					HAUtil.GetProxiesForAllNameNodesInNameservice<RefreshAuthorizationPolicyProtocol
					>(conf, nsId);
				foreach (NameNodeProxies.ProxyAndInfo<RefreshAuthorizationPolicyProtocol> proxy in 
					proxies)
				{
					proxy.GetProxy().RefreshServiceAcl();
					System.Console.Out.WriteLine("Refresh service acl successful for " + proxy.GetAddress
						());
				}
			}
			else
			{
				// Create the client
				RefreshAuthorizationPolicyProtocol refreshProtocol = NameNodeProxies.CreateProxy<
					RefreshAuthorizationPolicyProtocol>(conf, FileSystem.GetDefaultUri(conf)).GetProxy
					();
				// Refresh the authorization policy in-effect
				refreshProtocol.RefreshServiceAcl();
				System.Console.Out.WriteLine("Refresh service acl successful");
			}
			return 0;
		}

		/// <summary>
		/// Refresh the user-to-groups mappings on the
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode"/>
		/// .
		/// </summary>
		/// <returns>exitcode 0 on success, non-zero on failure</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual int RefreshUserToGroupsMappings()
		{
			// Get the current configuration
			Configuration conf = GetConf();
			// for security authorization
			// server principal for this call   
			// should be NN's one.
			conf.Set(CommonConfigurationKeys.HadoopSecurityServiceUserNameKey, conf.Get(DFSConfigKeys
				.DfsNamenodeKerberosPrincipalKey, string.Empty));
			DistributedFileSystem dfs = GetDFS();
			URI dfsUri = dfs.GetUri();
			bool isHaEnabled = HAUtil.IsLogicalUri(conf, dfsUri);
			if (isHaEnabled)
			{
				// Run refreshUserToGroupsMapings for all NNs if HA is enabled
				string nsId = dfsUri.GetHost();
				IList<NameNodeProxies.ProxyAndInfo<RefreshUserMappingsProtocol>> proxies = HAUtil
					.GetProxiesForAllNameNodesInNameservice<RefreshUserMappingsProtocol>(conf, nsId);
				foreach (NameNodeProxies.ProxyAndInfo<RefreshUserMappingsProtocol> proxy in proxies)
				{
					proxy.GetProxy().RefreshUserToGroupsMappings();
					System.Console.Out.WriteLine("Refresh user to groups mapping successful for " + proxy
						.GetAddress());
				}
			}
			else
			{
				// Create the client
				RefreshUserMappingsProtocol refreshProtocol = NameNodeProxies.CreateProxy<RefreshUserMappingsProtocol
					>(conf, FileSystem.GetDefaultUri(conf)).GetProxy();
				// Refresh the user-to-groups mappings
				refreshProtocol.RefreshUserToGroupsMappings();
				System.Console.Out.WriteLine("Refresh user to groups mapping successful");
			}
			return 0;
		}

		/// <summary>
		/// refreshSuperUserGroupsConfiguration
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode"/>
		/// .
		/// </summary>
		/// <returns>exitcode 0 on success, non-zero on failure</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual int RefreshSuperUserGroupsConfiguration()
		{
			// Get the current configuration
			Configuration conf = GetConf();
			// for security authorization
			// server principal for this call 
			// should be NAMENODE's one.
			conf.Set(CommonConfigurationKeys.HadoopSecurityServiceUserNameKey, conf.Get(DFSConfigKeys
				.DfsNamenodeKerberosPrincipalKey, string.Empty));
			DistributedFileSystem dfs = GetDFS();
			URI dfsUri = dfs.GetUri();
			bool isHaEnabled = HAUtil.IsLogicalUri(conf, dfsUri);
			if (isHaEnabled)
			{
				// Run refreshSuperUserGroupsConfiguration for all NNs if HA is enabled
				string nsId = dfsUri.GetHost();
				IList<NameNodeProxies.ProxyAndInfo<RefreshUserMappingsProtocol>> proxies = HAUtil
					.GetProxiesForAllNameNodesInNameservice<RefreshUserMappingsProtocol>(conf, nsId);
				foreach (NameNodeProxies.ProxyAndInfo<RefreshUserMappingsProtocol> proxy in proxies)
				{
					proxy.GetProxy().RefreshSuperUserGroupsConfiguration();
					System.Console.Out.WriteLine("Refresh super user groups configuration " + "successful for "
						 + proxy.GetAddress());
				}
			}
			else
			{
				// Create the client
				RefreshUserMappingsProtocol refreshProtocol = NameNodeProxies.CreateProxy<RefreshUserMappingsProtocol
					>(conf, FileSystem.GetDefaultUri(conf)).GetProxy();
				// Refresh the user-to-groups mappings
				refreshProtocol.RefreshSuperUserGroupsConfiguration();
				System.Console.Out.WriteLine("Refresh super user groups configuration successful"
					);
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int RefreshCallQueue()
		{
			// Get the current configuration
			Configuration conf = GetConf();
			// for security authorization
			// server principal for this call   
			// should be NN's one.
			conf.Set(CommonConfigurationKeys.HadoopSecurityServiceUserNameKey, conf.Get(DFSConfigKeys
				.DfsNamenodeKerberosPrincipalKey, string.Empty));
			DistributedFileSystem dfs = GetDFS();
			URI dfsUri = dfs.GetUri();
			bool isHaEnabled = HAUtil.IsLogicalUri(conf, dfsUri);
			if (isHaEnabled)
			{
				// Run refreshCallQueue for all NNs if HA is enabled
				string nsId = dfsUri.GetHost();
				IList<NameNodeProxies.ProxyAndInfo<RefreshCallQueueProtocol>> proxies = HAUtil.GetProxiesForAllNameNodesInNameservice
					<RefreshCallQueueProtocol>(conf, nsId);
				foreach (NameNodeProxies.ProxyAndInfo<RefreshCallQueueProtocol> proxy in proxies)
				{
					proxy.GetProxy().RefreshCallQueue();
					System.Console.Out.WriteLine("Refresh call queue successful for " + proxy.GetAddress
						());
				}
			}
			else
			{
				// Create the client
				RefreshCallQueueProtocol refreshProtocol = NameNodeProxies.CreateProxy<RefreshCallQueueProtocol
					>(conf, FileSystem.GetDefaultUri(conf)).GetProxy();
				// Refresh the call queue
				refreshProtocol.RefreshCallQueue();
				System.Console.Out.WriteLine("Refresh call queue successful");
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Reconfig(string[] argv, int i)
		{
			string nodeType = argv[i];
			string address = argv[i + 1];
			string op = argv[i + 2];
			if ("start".Equals(op))
			{
				return StartReconfiguration(nodeType, address);
			}
			else
			{
				if ("status".Equals(op))
				{
					return GetReconfigurationStatus(nodeType, address, System.Console.Out, System.Console.Error
						);
				}
			}
			System.Console.Error.WriteLine("Unknown operation: " + op);
			return -1;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual int StartReconfiguration(string nodeType, string address)
		{
			if ("datanode".Equals(nodeType))
			{
				ClientDatanodeProtocol dnProxy = GetDataNodeProxy(address);
				dnProxy.StartReconfiguration();
				System.Console.Out.WriteLine("Started reconfiguration task on DataNode " + address
					);
				return 0;
			}
			else
			{
				System.Console.Error.WriteLine("Node type " + nodeType + " does not support reconfiguration."
					);
				return 1;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual int GetReconfigurationStatus(string nodeType, string address, TextWriter
			 @out, TextWriter err)
		{
			if ("datanode".Equals(nodeType))
			{
				ClientDatanodeProtocol dnProxy = GetDataNodeProxy(address);
				try
				{
					ReconfigurationTaskStatus status = dnProxy.GetReconfigurationStatus();
					@out.Write("Reconfiguring status for DataNode[" + address + "]: ");
					if (!status.HasTask())
					{
						@out.WriteLine("no task was found.");
						return 0;
					}
					@out.Write("started at " + Sharpen.Extensions.CreateDate(status.GetStartTime()));
					if (!status.Stopped())
					{
						@out.WriteLine(" and is still running.");
						return 0;
					}
					@out.WriteLine(" and finished at " + Sharpen.Extensions.CreateDate(status.GetEndTime
						()).ToString() + ".");
					foreach (KeyValuePair<ReconfigurationUtil.PropertyChange, Optional<string>> result
						 in status.GetStatus())
					{
						if (!result.Value.IsPresent())
						{
							@out.Write("SUCCESS: ");
						}
						else
						{
							@out.Write("FAILED: ");
						}
						@out.Printf("Change property %s%n\tFrom: \"%s\"%n\tTo: \"%s\"%n", result.Key.prop
							, result.Key.oldVal, result.Key.newVal);
						if (result.Value.IsPresent())
						{
							@out.WriteLine("\tError: " + result.Value.Get() + ".");
						}
					}
				}
				catch (IOException e)
				{
					err.WriteLine("DataNode reloading configuration: " + e + ".");
					return 1;
				}
			}
			else
			{
				err.WriteLine("Node type " + nodeType + " does not support reconfiguration.");
				return 1;
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int GenericRefresh(string[] argv, int i)
		{
			string hostport = argv[i++];
			string identifier = argv[i++];
			string[] args = Arrays.CopyOfRange(argv, i, argv.Length);
			// Get the current configuration
			Configuration conf = GetConf();
			// for security authorization
			// server principal for this call
			// should be NN's one.
			conf.Set(CommonConfigurationKeys.HadoopSecurityServiceUserNameKey, conf.Get(DFSConfigKeys
				.DfsNamenodeKerberosPrincipalKey, string.Empty));
			// Create the client
			Type xface = typeof(GenericRefreshProtocolPB);
			IPEndPoint address = NetUtils.CreateSocketAddr(hostport);
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			RPC.SetProtocolEngine(conf, xface, typeof(ProtobufRpcEngine));
			GenericRefreshProtocolPB proxy = (GenericRefreshProtocolPB)RPC.GetProxy(xface, RPC
				.GetProtocolVersion(xface), address, ugi, conf, NetUtils.GetDefaultSocketFactory
				(conf), 0);
			ICollection<RefreshResponse> responses = null;
			try
			{
				using (GenericRefreshProtocolClientSideTranslatorPB xlator = new GenericRefreshProtocolClientSideTranslatorPB
					(proxy))
				{
					// Refresh
					responses = xlator.Refresh(identifier, args);
					int returnCode = 0;
					// Print refresh responses
					System.Console.Out.WriteLine("Refresh Responses:\n");
					foreach (RefreshResponse response in responses)
					{
						System.Console.Out.WriteLine(response.ToString());
						if (returnCode == 0 && response.GetReturnCode() != 0)
						{
							// This is the first non-zero return code, so we should return this
							returnCode = response.GetReturnCode();
						}
						else
						{
							if (returnCode != 0 && response.GetReturnCode() != 0)
							{
								// Then now we have multiple non-zero return codes,
								// so we merge them into -1
								returnCode = -1;
							}
						}
					}
					return returnCode;
				}
			}
			finally
			{
				if (responses == null)
				{
					System.Console.Out.WriteLine("Failed to get response.\n");
					return -1;
				}
			}
		}

		/// <summary>Displays format of commands.</summary>
		/// <param name="cmd">The command that is being executed.</param>
		private static void PrintUsage(string cmd)
		{
			if ("-report".Equals(cmd))
			{
				System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-report] [-live] [-dead] [-decommissioning]"
					);
			}
			else
			{
				if ("-safemode".Equals(cmd))
				{
					System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-safemode enter | leave | get | wait]"
						);
				}
				else
				{
					if (Sharpen.Runtime.EqualsIgnoreCase("-allowSnapshot", cmd))
					{
						System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-allowSnapshot <snapshotDir>]"
							);
					}
					else
					{
						if (Sharpen.Runtime.EqualsIgnoreCase("-disallowSnapshot", cmd))
						{
							System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-disallowSnapshot <snapshotDir>]"
								);
						}
						else
						{
							if ("-saveNamespace".Equals(cmd))
							{
								System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-saveNamespace]");
							}
							else
							{
								if ("-rollEdits".Equals(cmd))
								{
									System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-rollEdits]");
								}
								else
								{
									if ("-restoreFailedStorage".Equals(cmd))
									{
										System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-restoreFailedStorage true|false|check ]"
											);
									}
									else
									{
										if ("-refreshNodes".Equals(cmd))
										{
											System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-refreshNodes]");
										}
										else
										{
											if ("-finalizeUpgrade".Equals(cmd))
											{
												System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-finalizeUpgrade]");
											}
											else
											{
												if (DFSAdmin.RollingUpgradeCommand.Matches(cmd))
												{
													System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [" + DFSAdmin.RollingUpgradeCommand
														.Usage + "]");
												}
												else
												{
													if ("-metasave".Equals(cmd))
													{
														System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-metasave filename]");
													}
													else
													{
														if (DFSAdmin.SetQuotaCommand.Matches(cmd))
														{
															System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [" + DFSAdmin.SetQuotaCommand
																.Usage + "]");
														}
														else
														{
															if (DFSAdmin.ClearQuotaCommand.Matches(cmd))
															{
																System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [" + DFSAdmin.ClearQuotaCommand
																	.Usage + "]");
															}
															else
															{
																if (DFSAdmin.SetSpaceQuotaCommand.Matches(cmd))
																{
																	System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [" + DFSAdmin.SetSpaceQuotaCommand
																		.Usage + "]");
																}
																else
																{
																	if (DFSAdmin.ClearSpaceQuotaCommand.Matches(cmd))
																	{
																		System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [" + DFSAdmin.ClearSpaceQuotaCommand
																			.Usage + "]");
																	}
																	else
																	{
																		if ("-refreshServiceAcl".Equals(cmd))
																		{
																			System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-refreshServiceAcl]");
																		}
																		else
																		{
																			if ("-refreshUserToGroupsMappings".Equals(cmd))
																			{
																				System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-refreshUserToGroupsMappings]"
																					);
																			}
																			else
																			{
																				if ("-refreshSuperUserGroupsConfiguration".Equals(cmd))
																				{
																					System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-refreshSuperUserGroupsConfiguration]"
																						);
																				}
																				else
																				{
																					if ("-refreshCallQueue".Equals(cmd))
																					{
																						System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-refreshCallQueue]");
																					}
																					else
																					{
																						if ("-reconfig".Equals(cmd))
																						{
																							System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-reconfig <datanode|...> <host:port> <start|status>]"
																								);
																						}
																						else
																						{
																							if ("-refresh".Equals(cmd))
																							{
																								System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-refresh <hostname:port> <resource_identifier> [arg1..argn]"
																									);
																							}
																							else
																							{
																								if ("-printTopology".Equals(cmd))
																								{
																									System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-printTopology]");
																								}
																								else
																								{
																									if ("-refreshNamenodes".Equals(cmd))
																									{
																										System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-refreshNamenodes datanode-host:port]"
																											);
																									}
																									else
																									{
																										if ("-deleteBlockPool".Equals(cmd))
																										{
																											System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-deleteBlockPool datanode-host:port blockpoolId [force]]"
																												);
																										}
																										else
																										{
																											if ("-setBalancerBandwidth".Equals(cmd))
																											{
																												System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-setBalancerBandwidth <bandwidth in bytes per second>]"
																													);
																											}
																											else
																											{
																												if ("-fetchImage".Equals(cmd))
																												{
																													System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-fetchImage <local directory>]"
																														);
																												}
																												else
																												{
																													if ("-shutdownDatanode".Equals(cmd))
																													{
																														System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-shutdownDatanode <datanode_host:ipc_port> [upgrade]]"
																															);
																													}
																													else
																													{
																														if ("-getDatanodeInfo".Equals(cmd))
																														{
																															System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-getDatanodeInfo <datanode_host:ipc_port>]"
																																);
																														}
																														else
																														{
																															if ("-triggerBlockReport".Equals(cmd))
																															{
																																System.Console.Error.WriteLine("Usage: hdfs dfsadmin" + " [-triggerBlockReport [-incremental] <datanode_host:ipc_port>]"
																																	);
																															}
																															else
																															{
																																System.Console.Error.WriteLine("Usage: hdfs dfsadmin");
																																System.Console.Error.WriteLine("Note: Administrative commands can only be run as the HDFS superuser."
																																	);
																																System.Console.Error.WriteLine(commonUsageSummary);
																																ToolRunner.PrintGenericCommandUsage(System.Console.Error);
																															}
																														}
																													}
																												}
																											}
																										}
																									}
																								}
																							}
																						}
																					}
																				}
																			}
																		}
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		/// <param name="argv">The parameters passed to this program.</param>
		/// <exception>
		/// Exception
		/// if the filesystem does not exist.
		/// </exception>
		/// <returns>0 on success, non zero on error.</returns>
		/// <exception cref="System.Exception"/>
		public override int Run(string[] argv)
		{
			if (argv.Length < 1)
			{
				PrintUsage(string.Empty);
				return -1;
			}
			int exitCode = -1;
			int i = 0;
			string cmd = argv[i++];
			//
			// verify that we have enough command line parameters
			//
			if ("-safemode".Equals(cmd))
			{
				if (argv.Length != 2)
				{
					PrintUsage(cmd);
					return exitCode;
				}
			}
			else
			{
				if (Sharpen.Runtime.EqualsIgnoreCase("-allowSnapshot", cmd))
				{
					if (argv.Length != 2)
					{
						PrintUsage(cmd);
						return exitCode;
					}
				}
				else
				{
					if (Sharpen.Runtime.EqualsIgnoreCase("-disallowSnapshot", cmd))
					{
						if (argv.Length != 2)
						{
							PrintUsage(cmd);
							return exitCode;
						}
					}
					else
					{
						if ("-report".Equals(cmd))
						{
							if (argv.Length < 1)
							{
								PrintUsage(cmd);
								return exitCode;
							}
						}
						else
						{
							if ("-saveNamespace".Equals(cmd))
							{
								if (argv.Length != 1)
								{
									PrintUsage(cmd);
									return exitCode;
								}
							}
							else
							{
								if ("-rollEdits".Equals(cmd))
								{
									if (argv.Length != 1)
									{
										PrintUsage(cmd);
										return exitCode;
									}
								}
								else
								{
									if ("-restoreFailedStorage".Equals(cmd))
									{
										if (argv.Length != 2)
										{
											PrintUsage(cmd);
											return exitCode;
										}
									}
									else
									{
										if ("-refreshNodes".Equals(cmd))
										{
											if (argv.Length != 1)
											{
												PrintUsage(cmd);
												return exitCode;
											}
										}
										else
										{
											if ("-finalizeUpgrade".Equals(cmd))
											{
												if (argv.Length != 1)
												{
													PrintUsage(cmd);
													return exitCode;
												}
											}
											else
											{
												if (DFSAdmin.RollingUpgradeCommand.Matches(cmd))
												{
													if (argv.Length < 1 || argv.Length > 2)
													{
														PrintUsage(cmd);
														return exitCode;
													}
												}
												else
												{
													if ("-metasave".Equals(cmd))
													{
														if (argv.Length != 2)
														{
															PrintUsage(cmd);
															return exitCode;
														}
													}
													else
													{
														if ("-refreshServiceAcl".Equals(cmd))
														{
															if (argv.Length != 1)
															{
																PrintUsage(cmd);
																return exitCode;
															}
														}
														else
														{
															if ("-refresh".Equals(cmd))
															{
																if (argv.Length < 3)
																{
																	PrintUsage(cmd);
																	return exitCode;
																}
															}
															else
															{
																if ("-refreshUserToGroupsMappings".Equals(cmd))
																{
																	if (argv.Length != 1)
																	{
																		PrintUsage(cmd);
																		return exitCode;
																	}
																}
																else
																{
																	if ("-printTopology".Equals(cmd))
																	{
																		if (argv.Length != 1)
																		{
																			PrintUsage(cmd);
																			return exitCode;
																		}
																	}
																	else
																	{
																		if ("-refreshNamenodes".Equals(cmd))
																		{
																			if (argv.Length != 2)
																			{
																				PrintUsage(cmd);
																				return exitCode;
																			}
																		}
																		else
																		{
																			if ("-reconfig".Equals(cmd))
																			{
																				if (argv.Length != 4)
																				{
																					PrintUsage(cmd);
																					return exitCode;
																				}
																			}
																			else
																			{
																				if ("-deleteBlockPool".Equals(cmd))
																				{
																					if ((argv.Length != 3) && (argv.Length != 4))
																					{
																						PrintUsage(cmd);
																						return exitCode;
																					}
																				}
																				else
																				{
																					if ("-setBalancerBandwidth".Equals(cmd))
																					{
																						if (argv.Length != 2)
																						{
																							PrintUsage(cmd);
																							return exitCode;
																						}
																					}
																					else
																					{
																						if ("-fetchImage".Equals(cmd))
																						{
																							if (argv.Length != 2)
																							{
																								PrintUsage(cmd);
																								return exitCode;
																							}
																						}
																						else
																						{
																							if ("-shutdownDatanode".Equals(cmd))
																							{
																								if ((argv.Length != 2) && (argv.Length != 3))
																								{
																									PrintUsage(cmd);
																									return exitCode;
																								}
																							}
																							else
																							{
																								if ("-getDatanodeInfo".Equals(cmd))
																								{
																									if (argv.Length != 2)
																									{
																										PrintUsage(cmd);
																										return exitCode;
																									}
																								}
																								else
																								{
																									if ("-triggerBlockReport".Equals(cmd))
																									{
																										if (argv.Length < 1)
																										{
																											PrintUsage(cmd);
																											return exitCode;
																										}
																									}
																								}
																							}
																						}
																					}
																				}
																			}
																		}
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			// initialize DFSAdmin
			try
			{
				Init();
			}
			catch (RPC.VersionMismatch)
			{
				System.Console.Error.WriteLine("Version Mismatch between client and server" + "... command aborted."
					);
				return exitCode;
			}
			catch (IOException)
			{
				System.Console.Error.WriteLine("Bad connection to DFS... command aborted.");
				return exitCode;
			}
			Exception debugException = null;
			exitCode = 0;
			try
			{
				if ("-report".Equals(cmd))
				{
					Report(argv, i);
				}
				else
				{
					if ("-safemode".Equals(cmd))
					{
						SetSafeMode(argv, i);
					}
					else
					{
						if (Sharpen.Runtime.EqualsIgnoreCase("-allowSnapshot", cmd))
						{
							AllowSnapshot(argv);
						}
						else
						{
							if (Sharpen.Runtime.EqualsIgnoreCase("-disallowSnapshot", cmd))
							{
								DisallowSnapshot(argv);
							}
							else
							{
								if ("-saveNamespace".Equals(cmd))
								{
									exitCode = SaveNamespace();
								}
								else
								{
									if ("-rollEdits".Equals(cmd))
									{
										exitCode = RollEdits();
									}
									else
									{
										if ("-restoreFailedStorage".Equals(cmd))
										{
											exitCode = RestoreFailedStorage(argv[i]);
										}
										else
										{
											if ("-refreshNodes".Equals(cmd))
											{
												exitCode = RefreshNodes();
											}
											else
											{
												if ("-finalizeUpgrade".Equals(cmd))
												{
													exitCode = FinalizeUpgrade();
												}
												else
												{
													if (DFSAdmin.RollingUpgradeCommand.Matches(cmd))
													{
														exitCode = DFSAdmin.RollingUpgradeCommand.Run(GetDFS(), argv, i);
													}
													else
													{
														if ("-metasave".Equals(cmd))
														{
															exitCode = MetaSave(argv, i);
														}
														else
														{
															if (DFSAdmin.ClearQuotaCommand.Matches(cmd))
															{
																exitCode = new DFSAdmin.ClearQuotaCommand(argv, i, GetDFS()).RunAll();
															}
															else
															{
																if (DFSAdmin.SetQuotaCommand.Matches(cmd))
																{
																	exitCode = new DFSAdmin.SetQuotaCommand(argv, i, GetDFS()).RunAll();
																}
																else
																{
																	if (DFSAdmin.ClearSpaceQuotaCommand.Matches(cmd))
																	{
																		exitCode = new DFSAdmin.ClearSpaceQuotaCommand(argv, i, GetDFS()).RunAll();
																	}
																	else
																	{
																		if (DFSAdmin.SetSpaceQuotaCommand.Matches(cmd))
																		{
																			exitCode = new DFSAdmin.SetSpaceQuotaCommand(argv, i, GetDFS()).RunAll();
																		}
																		else
																		{
																			if ("-refreshServiceAcl".Equals(cmd))
																			{
																				exitCode = RefreshServiceAcl();
																			}
																			else
																			{
																				if ("-refreshUserToGroupsMappings".Equals(cmd))
																				{
																					exitCode = RefreshUserToGroupsMappings();
																				}
																				else
																				{
																					if ("-refreshSuperUserGroupsConfiguration".Equals(cmd))
																					{
																						exitCode = RefreshSuperUserGroupsConfiguration();
																					}
																					else
																					{
																						if ("-refreshCallQueue".Equals(cmd))
																						{
																							exitCode = RefreshCallQueue();
																						}
																						else
																						{
																							if ("-refresh".Equals(cmd))
																							{
																								exitCode = GenericRefresh(argv, i);
																							}
																							else
																							{
																								if ("-printTopology".Equals(cmd))
																								{
																									exitCode = PrintTopology();
																								}
																								else
																								{
																									if ("-refreshNamenodes".Equals(cmd))
																									{
																										exitCode = RefreshNamenodes(argv, i);
																									}
																									else
																									{
																										if ("-deleteBlockPool".Equals(cmd))
																										{
																											exitCode = DeleteBlockPool(argv, i);
																										}
																										else
																										{
																											if ("-setBalancerBandwidth".Equals(cmd))
																											{
																												exitCode = SetBalancerBandwidth(argv, i);
																											}
																											else
																											{
																												if ("-fetchImage".Equals(cmd))
																												{
																													exitCode = FetchImage(argv, i);
																												}
																												else
																												{
																													if ("-shutdownDatanode".Equals(cmd))
																													{
																														exitCode = ShutdownDatanode(argv, i);
																													}
																													else
																													{
																														if ("-getDatanodeInfo".Equals(cmd))
																														{
																															exitCode = GetDatanodeInfo(argv, i);
																														}
																														else
																														{
																															if ("-reconfig".Equals(cmd))
																															{
																																exitCode = Reconfig(argv, i);
																															}
																															else
																															{
																																if ("-triggerBlockReport".Equals(cmd))
																																{
																																	exitCode = TriggerBlockReport(argv);
																																}
																																else
																																{
																																	if ("-help".Equals(cmd))
																																	{
																																		if (i < argv.Length)
																																		{
																																			PrintHelp(argv[i]);
																																		}
																																		else
																																		{
																																			PrintHelp(string.Empty);
																																		}
																																	}
																																	else
																																	{
																																		exitCode = -1;
																																		System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": Unknown command"
																																			);
																																		PrintUsage(string.Empty);
																																	}
																																}
																															}
																														}
																													}
																												}
																											}
																										}
																									}
																								}
																							}
																						}
																					}
																				}
																			}
																		}
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			catch (ArgumentException arge)
			{
				debugException = arge;
				exitCode = -1;
				System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": " + arge.GetLocalizedMessage
					());
				PrintUsage(cmd);
			}
			catch (RemoteException e)
			{
				//
				// This is a error returned by hadoop server. Print
				// out the first line of the error message, ignore the stack trace.
				exitCode = -1;
				debugException = e;
				try
				{
					string[] content;
					content = e.GetLocalizedMessage().Split("\n");
					System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": " + content
						[0]);
				}
				catch (Exception ex)
				{
					System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": " + ex.GetLocalizedMessage
						());
					debugException = ex;
				}
			}
			catch (Exception e)
			{
				exitCode = -1;
				debugException = e;
				System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": " + e.GetLocalizedMessage
					());
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Exception encountered:", debugException);
			}
			return exitCode;
		}

		/// <exception cref="System.IO.IOException"/>
		private ClientDatanodeProtocol GetDataNodeProxy(string datanode)
		{
			IPEndPoint datanodeAddr = NetUtils.CreateSocketAddr(datanode);
			// Get the current configuration
			Configuration conf = GetConf();
			// For datanode proxy the server principal should be DN's one.
			conf.Set(CommonConfigurationKeys.HadoopSecurityServiceUserNameKey, conf.Get(DFSConfigKeys
				.DfsDatanodeKerberosPrincipalKey, string.Empty));
			// Create the client
			ClientDatanodeProtocol dnProtocol = DFSUtil.CreateClientDatanodeProtocolProxy(datanodeAddr
				, GetUGI(), conf, NetUtils.GetSocketFactory(conf, typeof(ClientDatanodeProtocol)
				));
			return dnProtocol;
		}

		/// <exception cref="System.IO.IOException"/>
		private int DeleteBlockPool(string[] argv, int i)
		{
			ClientDatanodeProtocol dnProxy = GetDataNodeProxy(argv[i]);
			bool force = false;
			if (argv.Length - 1 == i + 2)
			{
				if ("force".Equals(argv[i + 2]))
				{
					force = true;
				}
				else
				{
					PrintUsage("-deleteBlockPool");
					return -1;
				}
			}
			dnProxy.DeleteBlockPool(argv[i + 1], force);
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private int RefreshNamenodes(string[] argv, int i)
		{
			string datanode = argv[i];
			ClientDatanodeProtocol refreshProtocol = GetDataNodeProxy(datanode);
			refreshProtocol.RefreshNamenodes();
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private int ShutdownDatanode(string[] argv, int i)
		{
			string dn = argv[i];
			ClientDatanodeProtocol dnProxy = GetDataNodeProxy(dn);
			bool upgrade = false;
			if (argv.Length - 1 == i + 1)
			{
				if (Sharpen.Runtime.EqualsIgnoreCase("upgrade", argv[i + 1]))
				{
					upgrade = true;
				}
				else
				{
					PrintUsage("-shutdownDatanode");
					return -1;
				}
			}
			dnProxy.ShutdownDatanode(upgrade);
			System.Console.Out.WriteLine("Submitted a shutdown request to datanode " + dn);
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private int GetDatanodeInfo(string[] argv, int i)
		{
			ClientDatanodeProtocol dnProxy = GetDataNodeProxy(argv[i]);
			try
			{
				DatanodeLocalInfo dnInfo = dnProxy.GetDatanodeInfo();
				System.Console.Out.WriteLine(dnInfo.GetDatanodeLocalReport());
			}
			catch (IOException)
			{
				System.Console.Error.WriteLine("Datanode unreachable.");
				return -1;
			}
			return 0;
		}

		/// <summary>main() has some simple utility methods.</summary>
		/// <param name="argv">Command line parameters.</param>
		/// <exception>
		/// Exception
		/// if the filesystem does not exist.
		/// </exception>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			int res = ToolRunner.Run(new DFSAdmin(), argv);
			System.Environment.Exit(res);
		}
	}
}
