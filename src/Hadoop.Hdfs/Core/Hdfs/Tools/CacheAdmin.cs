using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>This class implements command-line operations on the HDFS Cache.</summary>
	public class CacheAdmin : Configured, Tool
	{
		public CacheAdmin()
			: this(null)
		{
		}

		public CacheAdmin(Configuration conf)
			: base(conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Run(string[] args)
		{
			if (args.Length == 0)
			{
				AdminHelper.PrintUsage(false, "cacheadmin", Commands);
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
				AdminHelper.PrintUsage(false, "cacheadmin", Commands);
				return 1;
			}
			IList<string> argsList = new List<string>();
			for (int j = 1; j < args.Length; j++)
			{
				argsList.AddItem(args[j]);
			}
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

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] argsArray)
		{
			Org.Apache.Hadoop.Hdfs.Tools.CacheAdmin cacheAdmin = new Org.Apache.Hadoop.Hdfs.Tools.CacheAdmin
				(new Configuration());
			System.Environment.Exit(cacheAdmin.Run(argsArray));
		}

		/// <exception cref="System.IO.IOException"/>
		private static CacheDirectiveInfo.Expiration ParseExpirationString(string ttlString
			)
		{
			CacheDirectiveInfo.Expiration ex = null;
			if (ttlString != null)
			{
				if (Sharpen.Runtime.EqualsIgnoreCase(ttlString, "never"))
				{
					ex = CacheDirectiveInfo.Expiration.Never;
				}
				else
				{
					long ttl = DFSUtil.ParseRelativeTime(ttlString);
					ex = CacheDirectiveInfo.Expiration.NewRelative(ttl);
				}
			}
			return ex;
		}

		private class AddCacheDirectiveInfoCommand : AdminHelper.Command
		{
			public virtual string GetName()
			{
				return "-addDirective";
			}

			public virtual string GetShortUsage()
			{
				return "[" + GetName() + " -path <path> -pool <pool-name> " + "[-force] " + "[-replication <replication>] [-ttl <time-to-live>]]\n";
			}

			public virtual string GetLongUsage()
			{
				TableListing listing = AdminHelper.GetOptionDescriptionListing();
				listing.AddRow("<path>", "A path to cache. The path can be " + "a directory or a file."
					);
				listing.AddRow("<pool-name>", "The pool to which the directive will be " + "added. You must have write permission on the cache pool "
					 + "in order to add new directives.");
				listing.AddRow("-force", "Skips checking of cache pool resource limits.");
				listing.AddRow("<replication>", "The cache replication factor to use. " + "Defaults to 1."
					);
				listing.AddRow("<time-to-live>", "How long the directive is " + "valid. Can be specified in minutes, hours, and days, e.g. "
					 + "30m, 4h, 2d. Valid units are [smhd]." + " \"never\" indicates a directive that never expires."
					 + " If unspecified, the directive never expires.");
				return GetShortUsage() + "\n" + "Add a new cache directive.\n\n" + listing.ToString
					();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
				string path = StringUtils.PopOptionWithArgument("-path", args);
				if (path == null)
				{
					System.Console.Error.WriteLine("You must specify a path with -path.");
					return 1;
				}
				builder.SetPath(new Path(path));
				string poolName = StringUtils.PopOptionWithArgument("-pool", args);
				if (poolName == null)
				{
					System.Console.Error.WriteLine("You must specify a pool name with -pool.");
					return 1;
				}
				builder.SetPool(poolName);
				bool force = StringUtils.PopOption("-force", args);
				string replicationString = StringUtils.PopOptionWithArgument("-replication", args
					);
				if (replicationString != null)
				{
					short replication = short.ParseShort(replicationString);
					builder.SetReplication(replication);
				}
				string ttlString = StringUtils.PopOptionWithArgument("-ttl", args);
				try
				{
					CacheDirectiveInfo.Expiration ex = ParseExpirationString(ttlString);
					if (ex != null)
					{
						builder.SetExpiration(ex);
					}
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine("Error while parsing ttl value: " + e.Message);
					return 1;
				}
				if (!args.IsEmpty())
				{
					System.Console.Error.WriteLine("Can't understand argument: " + args[0]);
					return 1;
				}
				DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
				CacheDirectiveInfo directive = builder.Build();
				EnumSet<CacheFlag> flags = EnumSet.NoneOf<CacheFlag>();
				if (force)
				{
					flags.AddItem(CacheFlag.Force);
				}
				try
				{
					long id = dfs.AddCacheDirective(directive, flags);
					System.Console.Out.WriteLine("Added cache directive " + id);
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine(AdminHelper.PrettifyException(e));
					return 2;
				}
				return 0;
			}
		}

		private class RemoveCacheDirectiveInfoCommand : AdminHelper.Command
		{
			public virtual string GetName()
			{
				return "-removeDirective";
			}

			public virtual string GetShortUsage()
			{
				return "[" + GetName() + " <id>]\n";
			}

			public virtual string GetLongUsage()
			{
				TableListing listing = AdminHelper.GetOptionDescriptionListing();
				listing.AddRow("<id>", "The id of the cache directive to remove.  " + "You must have write permission on the pool of the "
					 + "directive in order to remove it.  To see a list " + "of cache directive IDs, use the -listDirectives command."
					);
				return GetShortUsage() + "\n" + "Remove a cache directive.\n\n" + listing.ToString
					();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				string idString = StringUtils.PopFirstNonOption(args);
				if (idString == null)
				{
					System.Console.Error.WriteLine("You must specify a directive ID to remove.");
					return 1;
				}
				long id;
				try
				{
					id = long.Parse(idString);
				}
				catch (FormatException)
				{
					System.Console.Error.WriteLine("Invalid directive ID " + idString + ": expected "
						 + "a numeric value.");
					return 1;
				}
				if (id <= 0)
				{
					System.Console.Error.WriteLine("Invalid directive ID " + id + ": ids must " + "be greater than 0."
						);
					return 1;
				}
				if (!args.IsEmpty())
				{
					System.Console.Error.WriteLine("Can't understand argument: " + args[0]);
					System.Console.Error.WriteLine("Usage is " + GetShortUsage());
					return 1;
				}
				DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
				try
				{
					dfs.GetClient().RemoveCacheDirective(id);
					System.Console.Out.WriteLine("Removed cached directive " + id);
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine(AdminHelper.PrettifyException(e));
					return 2;
				}
				return 0;
			}
		}

		private class ModifyCacheDirectiveInfoCommand : AdminHelper.Command
		{
			public virtual string GetName()
			{
				return "-modifyDirective";
			}

			public virtual string GetShortUsage()
			{
				return "[" + GetName() + " -id <id> [-path <path>] [-force] [-replication <replication>] "
					 + "[-pool <pool-name>] [-ttl <time-to-live>]]\n";
			}

			public virtual string GetLongUsage()
			{
				TableListing listing = AdminHelper.GetOptionDescriptionListing();
				listing.AddRow("<id>", "The ID of the directive to modify (required)");
				listing.AddRow("<path>", "A path to cache. The path can be " + "a directory or a file. (optional)"
					);
				listing.AddRow("-force", "Skips checking of cache pool resource limits.");
				listing.AddRow("<replication>", "The cache replication factor to use. " + "(optional)"
					);
				listing.AddRow("<pool-name>", "The pool to which the directive will be " + "added. You must have write permission on the cache pool "
					 + "in order to move a directive into it. (optional)");
				listing.AddRow("<time-to-live>", "How long the directive is " + "valid. Can be specified in minutes, hours, and days, e.g. "
					 + "30m, 4h, 2d. Valid units are [smhd]." + " \"never\" indicates a directive that never expires."
					);
				return GetShortUsage() + "\n" + "Modify a cache directive.\n\n" + listing.ToString
					();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
				bool modified = false;
				string idString = StringUtils.PopOptionWithArgument("-id", args);
				if (idString == null)
				{
					System.Console.Error.WriteLine("You must specify a directive ID with -id.");
					return 1;
				}
				builder.SetId(long.Parse(idString));
				string path = StringUtils.PopOptionWithArgument("-path", args);
				if (path != null)
				{
					builder.SetPath(new Path(path));
					modified = true;
				}
				bool force = StringUtils.PopOption("-force", args);
				string replicationString = StringUtils.PopOptionWithArgument("-replication", args
					);
				if (replicationString != null)
				{
					builder.SetReplication(short.ParseShort(replicationString));
					modified = true;
				}
				string poolName = StringUtils.PopOptionWithArgument("-pool", args);
				if (poolName != null)
				{
					builder.SetPool(poolName);
					modified = true;
				}
				string ttlString = StringUtils.PopOptionWithArgument("-ttl", args);
				try
				{
					CacheDirectiveInfo.Expiration ex = ParseExpirationString(ttlString);
					if (ex != null)
					{
						builder.SetExpiration(ex);
						modified = true;
					}
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine("Error while parsing ttl value: " + e.Message);
					return 1;
				}
				if (!args.IsEmpty())
				{
					System.Console.Error.WriteLine("Can't understand argument: " + args[0]);
					System.Console.Error.WriteLine("Usage is " + GetShortUsage());
					return 1;
				}
				if (!modified)
				{
					System.Console.Error.WriteLine("No modifications were specified.");
					return 1;
				}
				DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
				EnumSet<CacheFlag> flags = EnumSet.NoneOf<CacheFlag>();
				if (force)
				{
					flags.AddItem(CacheFlag.Force);
				}
				try
				{
					dfs.ModifyCacheDirective(builder.Build(), flags);
					System.Console.Out.WriteLine("Modified cache directive " + idString);
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine(AdminHelper.PrettifyException(e));
					return 2;
				}
				return 0;
			}
		}

		private class RemoveCacheDirectiveInfosCommand : AdminHelper.Command
		{
			public virtual string GetName()
			{
				return "-removeDirectives";
			}

			public virtual string GetShortUsage()
			{
				return "[" + GetName() + " -path <path>]\n";
			}

			public virtual string GetLongUsage()
			{
				TableListing listing = AdminHelper.GetOptionDescriptionListing();
				listing.AddRow("-path <path>", "The path of the cache directives to remove.  " + 
					"You must have write permission on the pool of the directive in order " + "to remove it.  To see a list of cache directives, use the "
					 + "-listDirectives command.");
				return GetShortUsage() + "\n" + "Remove every cache directive with the specified path.\n\n"
					 + listing.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				string path = StringUtils.PopOptionWithArgument("-path", args);
				if (path == null)
				{
					System.Console.Error.WriteLine("You must specify a path with -path.");
					return 1;
				}
				if (!args.IsEmpty())
				{
					System.Console.Error.WriteLine("Can't understand argument: " + args[0]);
					System.Console.Error.WriteLine("Usage is " + GetShortUsage());
					return 1;
				}
				int exitCode = 0;
				try
				{
					DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
					RemoteIterator<CacheDirectiveEntry> iter = dfs.ListCacheDirectives(new CacheDirectiveInfo.Builder
						().SetPath(new Path(path)).Build());
					while (iter.HasNext())
					{
						CacheDirectiveEntry entry = iter.Next();
						try
						{
							dfs.RemoveCacheDirective(entry.GetInfo().GetId());
							System.Console.Out.WriteLine("Removed cache directive " + entry.GetInfo().GetId()
								);
						}
						catch (IOException e)
						{
							System.Console.Error.WriteLine(AdminHelper.PrettifyException(e));
							exitCode = 2;
						}
					}
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine(AdminHelper.PrettifyException(e));
					exitCode = 2;
				}
				if (exitCode == 0)
				{
					System.Console.Out.WriteLine("Removed every cache directive with path " + path);
				}
				return exitCode;
			}
		}

		private class ListCacheDirectiveInfoCommand : AdminHelper.Command
		{
			public virtual string GetName()
			{
				return "-listDirectives";
			}

			public virtual string GetShortUsage()
			{
				return "[" + GetName() + " [-stats] [-path <path>] [-pool <pool>] [-id <id>]\n";
			}

			public virtual string GetLongUsage()
			{
				TableListing listing = AdminHelper.GetOptionDescriptionListing();
				listing.AddRow("-stats", "List path-based cache directive statistics.");
				listing.AddRow("<path>", "List only " + "cache directives with this path. " + "Note that if there is a cache directive for <path> "
					 + "in a cache pool that we don't have read access for, it " + "will not be listed."
					);
				listing.AddRow("<pool>", "List only path cache directives in that pool.");
				listing.AddRow("<id>", "List the cache directive with this id.");
				return GetShortUsage() + "\n" + "List cache directives.\n\n" + listing.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
				string pathFilter = StringUtils.PopOptionWithArgument("-path", args);
				if (pathFilter != null)
				{
					builder.SetPath(new Path(pathFilter));
				}
				string poolFilter = StringUtils.PopOptionWithArgument("-pool", args);
				if (poolFilter != null)
				{
					builder.SetPool(poolFilter);
				}
				bool printStats = StringUtils.PopOption("-stats", args);
				string idFilter = StringUtils.PopOptionWithArgument("-id", args);
				if (idFilter != null)
				{
					builder.SetId(long.Parse(idFilter));
				}
				if (!args.IsEmpty())
				{
					System.Console.Error.WriteLine("Can't understand argument: " + args[0]);
					return 1;
				}
				TableListing.Builder tableBuilder = new TableListing.Builder().AddField("ID", TableListing.Justification
					.Right).AddField("POOL", TableListing.Justification.Left).AddField("REPL", TableListing.Justification
					.Right).AddField("EXPIRY", TableListing.Justification.Left).AddField("PATH", TableListing.Justification
					.Left);
				if (printStats)
				{
					tableBuilder.AddField("BYTES_NEEDED", TableListing.Justification.Right).AddField(
						"BYTES_CACHED", TableListing.Justification.Right).AddField("FILES_NEEDED", TableListing.Justification
						.Right).AddField("FILES_CACHED", TableListing.Justification.Right);
				}
				TableListing tableListing = tableBuilder.Build();
				try
				{
					DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
					RemoteIterator<CacheDirectiveEntry> iter = dfs.ListCacheDirectives(builder.Build(
						));
					int numEntries = 0;
					while (iter.HasNext())
					{
						CacheDirectiveEntry entry = iter.Next();
						CacheDirectiveInfo directive = entry.GetInfo();
						CacheDirectiveStats stats = entry.GetStats();
						IList<string> row = new List<string>();
						row.AddItem(string.Empty + directive.GetId());
						row.AddItem(directive.GetPool());
						row.AddItem(string.Empty + directive.GetReplication());
						string expiry;
						// This is effectively never, round for nice printing
						if (directive.GetExpiration().GetMillis() > CacheDirectiveInfo.Expiration.MaxRelativeExpiryMs
							 / 2)
						{
							expiry = "never";
						}
						else
						{
							expiry = directive.GetExpiration().ToString();
						}
						row.AddItem(expiry);
						row.AddItem(directive.GetPath().ToUri().GetPath());
						if (printStats)
						{
							row.AddItem(string.Empty + stats.GetBytesNeeded());
							row.AddItem(string.Empty + stats.GetBytesCached());
							row.AddItem(string.Empty + stats.GetFilesNeeded());
							row.AddItem(string.Empty + stats.GetFilesCached());
						}
						tableListing.AddRow(Sharpen.Collections.ToArray(row, new string[row.Count]));
						numEntries++;
					}
					System.Console.Out.Write(string.Format("Found %d entr%s%n", numEntries, numEntries
						 == 1 ? "y" : "ies"));
					if (numEntries > 0)
					{
						System.Console.Out.Write(tableListing);
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

		private class AddCachePoolCommand : AdminHelper.Command
		{
			private const string Name = "-addPool";

			public virtual string GetName()
			{
				return Name;
			}

			public virtual string GetShortUsage()
			{
				return "[" + Name + " <name> [-owner <owner>] " + "[-group <group>] [-mode <mode>] [-limit <limit>] "
					 + "[-maxTtl <maxTtl>]\n";
			}

			public virtual string GetLongUsage()
			{
				TableListing listing = AdminHelper.GetOptionDescriptionListing();
				listing.AddRow("<name>", "Name of the new pool.");
				listing.AddRow("<owner>", "Username of the owner of the pool. " + "Defaults to the current user."
					);
				listing.AddRow("<group>", "Group of the pool. " + "Defaults to the primary group name of the current user."
					);
				listing.AddRow("<mode>", "UNIX-style permissions for the pool. " + "Permissions are specified in octal, e.g. 0755. "
					 + "By default, this is set to " + string.Format("0%03o", FsPermission.GetCachePoolDefault
					().ToShort()) + ".");
				listing.AddRow("<limit>", "The maximum number of bytes that can be " + "cached by directives in this pool, in aggregate. By default, "
					 + "no limit is set.");
				listing.AddRow("<maxTtl>", "The maximum allowed time-to-live for " + "directives being added to the pool. This can be specified in "
					 + "seconds, minutes, hours, and days, e.g. 120s, 30m, 4h, 2d. " + "Valid units are [smhd]. By default, no maximum is set. "
					 + "A value of \"never\" specifies that there is no limit.");
				return GetShortUsage() + "\n" + "Add a new cache pool.\n\n" + listing.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				string name = StringUtils.PopFirstNonOption(args);
				if (name == null)
				{
					System.Console.Error.WriteLine("You must specify a name when creating a " + "cache pool."
						);
					return 1;
				}
				CachePoolInfo info = new CachePoolInfo(name);
				string owner = StringUtils.PopOptionWithArgument("-owner", args);
				if (owner != null)
				{
					info.SetOwnerName(owner);
				}
				string group = StringUtils.PopOptionWithArgument("-group", args);
				if (group != null)
				{
					info.SetGroupName(group);
				}
				string modeString = StringUtils.PopOptionWithArgument("-mode", args);
				if (modeString != null)
				{
					short mode = short.ParseShort(modeString, 8);
					info.SetMode(new FsPermission(mode));
				}
				string limitString = StringUtils.PopOptionWithArgument("-limit", args);
				long limit = AdminHelper.ParseLimitString(limitString);
				if (limit != null)
				{
					info.SetLimit(limit);
				}
				string maxTtlString = StringUtils.PopOptionWithArgument("-maxTtl", args);
				try
				{
					long maxTtl = AdminHelper.ParseTtlString(maxTtlString);
					if (maxTtl != null)
					{
						info.SetMaxRelativeExpiryMs(maxTtl);
					}
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine("Error while parsing maxTtl value: " + e.Message);
					return 1;
				}
				if (!args.IsEmpty())
				{
					System.Console.Error.Write("Can't understand arguments: " + Joiner.On(" ").Join(args
						) + "\n");
					System.Console.Error.WriteLine("Usage is " + GetShortUsage());
					return 1;
				}
				DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
				try
				{
					dfs.AddCachePool(info);
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine(AdminHelper.PrettifyException(e));
					return 2;
				}
				System.Console.Out.WriteLine("Successfully added cache pool " + name + ".");
				return 0;
			}
		}

		private class ModifyCachePoolCommand : AdminHelper.Command
		{
			public virtual string GetName()
			{
				return "-modifyPool";
			}

			public virtual string GetShortUsage()
			{
				return "[" + GetName() + " <name> [-owner <owner>] " + "[-group <group>] [-mode <mode>] [-limit <limit>] "
					 + "[-maxTtl <maxTtl>]]\n";
			}

			public virtual string GetLongUsage()
			{
				TableListing listing = AdminHelper.GetOptionDescriptionListing();
				listing.AddRow("<name>", "Name of the pool to modify.");
				listing.AddRow("<owner>", "Username of the owner of the pool");
				listing.AddRow("<group>", "Groupname of the group of the pool.");
				listing.AddRow("<mode>", "Unix-style permissions of the pool in octal.");
				listing.AddRow("<limit>", "Maximum number of bytes that can be cached " + "by this pool."
					);
				listing.AddRow("<maxTtl>", "The maximum allowed time-to-live for " + "directives being added to the pool."
					);
				return GetShortUsage() + "\n" + WordUtils.Wrap("Modifies the metadata of an existing cache pool. "
					 + "See usage of " + CacheAdmin.AddCachePoolCommand.Name + " for more details.", 
					AdminHelper.MaxLineWidth) + "\n\n" + listing.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				string owner = StringUtils.PopOptionWithArgument("-owner", args);
				string group = StringUtils.PopOptionWithArgument("-group", args);
				string modeString = StringUtils.PopOptionWithArgument("-mode", args);
				int mode = (modeString == null) ? null : System.Convert.ToInt32(modeString, 8);
				string limitString = StringUtils.PopOptionWithArgument("-limit", args);
				long limit = AdminHelper.ParseLimitString(limitString);
				string maxTtlString = StringUtils.PopOptionWithArgument("-maxTtl", args);
				long maxTtl;
				try
				{
					maxTtl = AdminHelper.ParseTtlString(maxTtlString);
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine("Error while parsing maxTtl value: " + e.Message);
					return 1;
				}
				string name = StringUtils.PopFirstNonOption(args);
				if (name == null)
				{
					System.Console.Error.WriteLine("You must specify a name when creating a " + "cache pool."
						);
					return 1;
				}
				if (!args.IsEmpty())
				{
					System.Console.Error.Write("Can't understand arguments: " + Joiner.On(" ").Join(args
						) + "\n");
					System.Console.Error.WriteLine("Usage is " + GetShortUsage());
					return 1;
				}
				bool changed = false;
				CachePoolInfo info = new CachePoolInfo(name);
				if (owner != null)
				{
					info.SetOwnerName(owner);
					changed = true;
				}
				if (group != null)
				{
					info.SetGroupName(group);
					changed = true;
				}
				if (mode != null)
				{
					info.SetMode(new FsPermission(mode));
					changed = true;
				}
				if (limit != null)
				{
					info.SetLimit(limit);
					changed = true;
				}
				if (maxTtl != null)
				{
					info.SetMaxRelativeExpiryMs(maxTtl);
					changed = true;
				}
				if (!changed)
				{
					System.Console.Error.WriteLine("You must specify at least one attribute to " + "change in the cache pool."
						);
					return 1;
				}
				DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
				try
				{
					dfs.ModifyCachePool(info);
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine(AdminHelper.PrettifyException(e));
					return 2;
				}
				System.Console.Out.Write("Successfully modified cache pool " + name);
				string prefix = " to have ";
				if (owner != null)
				{
					System.Console.Out.Write(prefix + "owner name " + owner);
					prefix = " and ";
				}
				if (group != null)
				{
					System.Console.Out.Write(prefix + "group name " + group);
					prefix = " and ";
				}
				if (mode != null)
				{
					System.Console.Out.Write(prefix + "mode " + new FsPermission(mode));
					prefix = " and ";
				}
				if (limit != null)
				{
					System.Console.Out.Write(prefix + "limit " + limit);
					prefix = " and ";
				}
				if (maxTtl != null)
				{
					System.Console.Out.Write(prefix + "max time-to-live " + maxTtlString);
				}
				System.Console.Out.Write("\n");
				return 0;
			}
		}

		private class RemoveCachePoolCommand : AdminHelper.Command
		{
			public virtual string GetName()
			{
				return "-removePool";
			}

			public virtual string GetShortUsage()
			{
				return "[" + GetName() + " <name>]\n";
			}

			public virtual string GetLongUsage()
			{
				return GetShortUsage() + "\n" + WordUtils.Wrap("Remove a cache pool. This also uncaches paths "
					 + "associated with the pool.\n\n", AdminHelper.MaxLineWidth) + "<name>  Name of the cache pool to remove.\n";
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				string name = StringUtils.PopFirstNonOption(args);
				if (name == null)
				{
					System.Console.Error.WriteLine("You must specify a name when deleting a " + "cache pool."
						);
					return 1;
				}
				if (!args.IsEmpty())
				{
					System.Console.Error.Write("Can't understand arguments: " + Joiner.On(" ").Join(args
						) + "\n");
					System.Console.Error.WriteLine("Usage is " + GetShortUsage());
					return 1;
				}
				DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
				try
				{
					dfs.RemoveCachePool(name);
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine(AdminHelper.PrettifyException(e));
					return 2;
				}
				System.Console.Out.WriteLine("Successfully removed cache pool " + name + ".");
				return 0;
			}
		}

		private class ListCachePoolsCommand : AdminHelper.Command
		{
			public virtual string GetName()
			{
				return "-listPools";
			}

			public virtual string GetShortUsage()
			{
				return "[" + GetName() + " [-stats] [<name>]]\n";
			}

			public virtual string GetLongUsage()
			{
				TableListing listing = AdminHelper.GetOptionDescriptionListing();
				listing.AddRow("-stats", "Display additional cache pool statistics.");
				listing.AddRow("<name>", "If specified, list only the named cache pool.");
				return GetShortUsage() + "\n" + WordUtils.Wrap("Display information about one or more cache pools, "
					 + "e.g. name, owner, group, permissions, etc.", AdminHelper.MaxLineWidth) + "\n\n"
					 + listing.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				string name = StringUtils.PopFirstNonOption(args);
				bool printStats = StringUtils.PopOption("-stats", args);
				if (!args.IsEmpty())
				{
					System.Console.Error.Write("Can't understand arguments: " + Joiner.On(" ").Join(args
						) + "\n");
					System.Console.Error.WriteLine("Usage is " + GetShortUsage());
					return 1;
				}
				DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
				TableListing.Builder builder = new TableListing.Builder().AddField("NAME", TableListing.Justification
					.Left).AddField("OWNER", TableListing.Justification.Left).AddField("GROUP", TableListing.Justification
					.Left).AddField("MODE", TableListing.Justification.Left).AddField("LIMIT", TableListing.Justification
					.Right).AddField("MAXTTL", TableListing.Justification.Right);
				if (printStats)
				{
					builder.AddField("BYTES_NEEDED", TableListing.Justification.Right).AddField("BYTES_CACHED"
						, TableListing.Justification.Right).AddField("BYTES_OVERLIMIT", TableListing.Justification
						.Right).AddField("FILES_NEEDED", TableListing.Justification.Right).AddField("FILES_CACHED"
						, TableListing.Justification.Right);
				}
				TableListing listing = builder.Build();
				int numResults = 0;
				try
				{
					RemoteIterator<CachePoolEntry> iter = dfs.ListCachePools();
					while (iter.HasNext())
					{
						CachePoolEntry entry = iter.Next();
						CachePoolInfo info = entry.GetInfo();
						List<string> row = new List<string>();
						if (name == null || info.GetPoolName().Equals(name))
						{
							row.AddItem(info.GetPoolName());
							row.AddItem(info.GetOwnerName());
							row.AddItem(info.GetGroupName());
							row.AddItem(info.GetMode() != null ? info.GetMode().ToString() : null);
							long limit = info.GetLimit();
							string limitString;
							if (limit != null && limit.Equals(CachePoolInfo.LimitUnlimited))
							{
								limitString = "unlimited";
							}
							else
							{
								limitString = string.Empty + limit;
							}
							row.AddItem(limitString);
							long maxTtl = info.GetMaxRelativeExpiryMs();
							string maxTtlString = null;
							if (maxTtl != null)
							{
								if (maxTtl == CachePoolInfo.RelativeExpiryNever)
								{
									maxTtlString = "never";
								}
								else
								{
									maxTtlString = DFSUtil.DurationToString(maxTtl);
								}
							}
							row.AddItem(maxTtlString);
							if (printStats)
							{
								CachePoolStats stats = entry.GetStats();
								row.AddItem(System.Convert.ToString(stats.GetBytesNeeded()));
								row.AddItem(System.Convert.ToString(stats.GetBytesCached()));
								row.AddItem(System.Convert.ToString(stats.GetBytesOverlimit()));
								row.AddItem(System.Convert.ToString(stats.GetFilesNeeded()));
								row.AddItem(System.Convert.ToString(stats.GetFilesCached()));
							}
							listing.AddRow(Sharpen.Collections.ToArray(row, new string[row.Count]));
							++numResults;
							if (name != null)
							{
								break;
							}
						}
					}
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine(AdminHelper.PrettifyException(e));
					return 2;
				}
				System.Console.Out.Write(string.Format("Found %d result%s.%n", numResults, (numResults
					 == 1 ? string.Empty : "s")));
				if (numResults > 0)
				{
					System.Console.Out.Write(listing);
				}
				// If list pools succeed, we return 0 (success exit code)
				return 0;
			}
		}

		private static readonly AdminHelper.Command[] Commands = new AdminHelper.Command[
			] { new CacheAdmin.AddCacheDirectiveInfoCommand(), new CacheAdmin.ModifyCacheDirectiveInfoCommand
			(), new CacheAdmin.ListCacheDirectiveInfoCommand(), new CacheAdmin.RemoveCacheDirectiveInfoCommand
			(), new CacheAdmin.RemoveCacheDirectiveInfosCommand(), new CacheAdmin.AddCachePoolCommand
			(), new CacheAdmin.ModifyCachePoolCommand(), new CacheAdmin.RemoveCachePoolCommand
			(), new CacheAdmin.ListCachePoolsCommand() };
	}
}
