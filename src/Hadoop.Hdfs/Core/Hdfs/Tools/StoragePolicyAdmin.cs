using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>This class implements block storage policy operations.</summary>
	public class StoragePolicyAdmin : Configured, Tool
	{
		/// <exception cref="System.Exception"/>
		public static void Main(string[] argsArray)
		{
			Org.Apache.Hadoop.Hdfs.Tools.StoragePolicyAdmin admin = new Org.Apache.Hadoop.Hdfs.Tools.StoragePolicyAdmin
				(new Configuration());
			System.Environment.Exit(admin.Run(argsArray));
		}

		public StoragePolicyAdmin(Configuration conf)
			: base(conf)
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length == 0)
			{
				AdminHelper.PrintUsage(false, "storagepolicies", Commands);
				return 1;
			}
			AdminHelper.Command command = AdminHelper.DetermineCommand(args[0], Commands);
			if (command == null)
			{
				System.Console.Error.WriteLine("Can't understand command '" + args[0] + "'");
				if (!args[0].StartsWith("-"))
				{
					System.Console.Error.WriteLine("Command names must start with dashes.");
				}
				AdminHelper.PrintUsage(false, "storagepolicies", Commands);
				return 1;
			}
			IList<string> argsList = new List<string>();
			Sharpen.Collections.AddAll(argsList, Arrays.AsList(args).SubList(1, args.Length));
			try
			{
				return command.Run(GetConf(), argsList);
			}
			catch (ArgumentException e)
			{
				System.Console.Error.WriteLine(AdminHelper.PrettifyException(e));
				return -1;
			}
		}

		/// <summary>Command to list all the existing storage policies</summary>
		private class ListStoragePoliciesCommand : AdminHelper.Command
		{
			public virtual string GetName()
			{
				return "-listPolicies";
			}

			public virtual string GetShortUsage()
			{
				return "[" + GetName() + "]\n";
			}

			public virtual string GetLongUsage()
			{
				return GetShortUsage() + "\n" + "List all the existing block storage policies.\n";
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
				try
				{
					BlockStoragePolicy[] policies = dfs.GetStoragePolicies();
					System.Console.Out.WriteLine("Block Storage Policies:");
					foreach (BlockStoragePolicy policy in policies)
					{
						if (policy != null)
						{
							System.Console.Out.WriteLine("\t" + policy);
						}
					}
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine(AdminHelper.PrettifyException(e));
					return 2;
				}
				return 0;
			}
		}

		/// <summary>Command to get the storage policy of a file/directory</summary>
		private class GetStoragePolicyCommand : AdminHelper.Command
		{
			public virtual string GetName()
			{
				return "-getStoragePolicy";
			}

			public virtual string GetShortUsage()
			{
				return "[" + GetName() + " -path <path>]\n";
			}

			public virtual string GetLongUsage()
			{
				TableListing listing = AdminHelper.GetOptionDescriptionListing();
				listing.AddRow("<path>", "The path of the file/directory for getting the storage policy"
					);
				return GetShortUsage() + "\n" + "Get the storage policy of a file/directory.\n\n"
					 + listing.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				string path = StringUtils.PopOptionWithArgument("-path", args);
				if (path == null)
				{
					System.Console.Error.WriteLine("Please specify the path with -path.\nUsage:" + GetLongUsage
						());
					return 1;
				}
				DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
				try
				{
					HdfsFileStatus status = dfs.GetClient().GetFileInfo(path);
					if (status == null)
					{
						System.Console.Error.WriteLine("File/Directory does not exist: " + path);
						return 2;
					}
					byte storagePolicyId = status.GetStoragePolicy();
					if (storagePolicyId == BlockStoragePolicySuite.IdUnspecified)
					{
						System.Console.Out.WriteLine("The storage policy of " + path + " is unspecified");
						return 0;
					}
					BlockStoragePolicy[] policies = dfs.GetStoragePolicies();
					foreach (BlockStoragePolicy p in policies)
					{
						if (p.GetId() == storagePolicyId)
						{
							System.Console.Out.WriteLine("The storage policy of " + path + ":\n" + p);
							return 0;
						}
					}
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine(AdminHelper.PrettifyException(e));
					return 2;
				}
				System.Console.Error.WriteLine("Cannot identify the storage policy for " + path);
				return 2;
			}
		}

		/// <summary>Command to set the storage policy to a file/directory</summary>
		private class SetStoragePolicyCommand : AdminHelper.Command
		{
			public virtual string GetName()
			{
				return "-setStoragePolicy";
			}

			public virtual string GetShortUsage()
			{
				return "[" + GetName() + " -path <path> -policy <policy>]\n";
			}

			public virtual string GetLongUsage()
			{
				TableListing listing = AdminHelper.GetOptionDescriptionListing();
				listing.AddRow("<path>", "The path of the file/directory to set storage" + " policy"
					);
				listing.AddRow("<policy>", "The name of the block storage policy");
				return GetShortUsage() + "\n" + "Set the storage policy to a file/directory.\n\n"
					 + listing.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				string path = StringUtils.PopOptionWithArgument("-path", args);
				if (path == null)
				{
					System.Console.Error.WriteLine("Please specify the path for setting the storage "
						 + "policy.\nUsage: " + GetLongUsage());
					return 1;
				}
				string policyName = StringUtils.PopOptionWithArgument("-policy", args);
				if (policyName == null)
				{
					System.Console.Error.WriteLine("Please specify the policy name.\nUsage: " + GetLongUsage
						());
					return 1;
				}
				DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
				try
				{
					dfs.SetStoragePolicy(new Path(path), policyName);
					System.Console.Out.WriteLine("Set storage policy " + policyName + " on " + path);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine(AdminHelper.PrettifyException(e));
					return 2;
				}
				return 0;
			}
		}

		private static readonly AdminHelper.Command[] Commands = new AdminHelper.Command[
			] { new StoragePolicyAdmin.ListStoragePoliciesCommand(), new StoragePolicyAdmin.SetStoragePolicyCommand
			(), new StoragePolicyAdmin.GetStoragePolicyCommand() };
	}
}
