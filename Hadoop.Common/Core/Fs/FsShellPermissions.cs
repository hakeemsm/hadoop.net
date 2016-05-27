using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.FS.Shell;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>This class is the home for file permissions related commands.</summary>
	/// <remarks>
	/// This class is the home for file permissions related commands.
	/// Moved to this separate class since FsShell is getting too large.
	/// </remarks>
	public class FsShellPermissions : FsCommand
	{
		internal static Log Log = FsShell.Log;

		/// <summary>Register the permission related commands with the factory</summary>
		/// <param name="factory">the command factory</param>
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(FsShellPermissions.Chmod), "-chmod");
			factory.AddClass(typeof(FsShellPermissions.Chown), "-chown");
			factory.AddClass(typeof(FsShellPermissions.Chgrp), "-chgrp");
		}

		/// <summary>The pattern is almost as flexible as mode allowed by chmod shell command.
		/// 	</summary>
		/// <remarks>
		/// The pattern is almost as flexible as mode allowed by chmod shell command.
		/// The main restriction is that we recognize only rwxXt. To reduce errors we
		/// also enforce octal mode specifications of either 3 digits without a sticky
		/// bit setting or four digits with a sticky bit setting.
		/// </remarks>
		public class Chmod : FsShellPermissions
		{
			public const string Name = "chmod";

			public const string Usage = "[-R] <MODE[,MODE]... | OCTALMODE> PATH...";

			public const string Description = "Changes permissions of a file. " + "This works similar to the shell's chmod command with a few exceptions.\n"
				 + "-R: modifies the files recursively. This is the only option" + " currently supported.\n"
				 + "<MODE>: Mode is the same as mode used for the shell's command. " + "The only letters recognized are 'rwxXt', e.g. +t,a+r,g-w,+rwx,o=r.\n"
				 + "<OCTALMODE>: Mode specifed in 3 or 4 digits. If 4 digits, the first " + "may be 1 or 0 to turn the sticky bit on or off, respectively.  Unlike "
				 + "the shell command, it is not possible to specify only part of the " + "mode, e.g. 754 is same as u=rwx,g=rx,o=r.\n\n"
				 + "If none of 'augo' is specified, 'a' is assumed and unlike the " + "shell command, no umask is applied.";

			protected internal ChmodParser pp;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				CommandFormat cf = new CommandFormat(2, int.MaxValue, "R", null);
				cf.Parse(args);
				SetRecursive(cf.GetOpt("R"));
				string modeStr = args.RemoveFirst();
				try
				{
					pp = new ChmodParser(modeStr);
				}
				catch (ArgumentException)
				{
					// TODO: remove "chmod : " so it's not doubled up in output, but it's
					// here for backwards compatibility...
					throw new ArgumentException("chmod : mode '" + modeStr + "' does not match the expected pattern."
						);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				short newperms = pp.ApplyNewPermission(item.stat);
				if (item.stat.GetPermission().ToShort() != newperms)
				{
					try
					{
						item.fs.SetPermission(item.path, new FsPermission(newperms));
					}
					catch (IOException e)
					{
						Log.Debug("Error changing permissions of " + item, e);
						throw new IOException("changing permissions of '" + item + "': " + e.Message);
					}
				}
			}
		}

		private static string allowedChars = Org.Apache.Hadoop.Util.Shell.Windows ? "[-_./@a-zA-Z0-9 ]"
			 : "[-_./@a-zA-Z0-9]";

		/// <summary>Used to change owner and/or group of files</summary>
		public class Chown : FsShellPermissions
		{
			public const string Name = "chown";

			public const string Usage = "[-R] [OWNER][:[GROUP]] PATH...";

			public static readonly string Description = "Changes owner and group of a file. "
				 + "This is similar to the shell's chown command with a few exceptions.\n" + "-R: modifies the files recursively. This is the only option "
				 + "currently supported.\n\n" + "If only the owner or group is specified, then only the owner or "
				 + "group is modified. " + "The owner and group names may only consist of digits, alphabet, "
				 + "and any of " + allowedChars + ". The names are case sensitive.\n\n" + "WARNING: Avoid using '.' to separate user name and group though "
				 + "Linux allows it. If user names have dots in them and you are " + "using local file system, you might see surprising results since "
				 + "the shell command 'chown' is used for local files.";

			private static readonly Sharpen.Pattern chownPattern = Sharpen.Pattern.Compile("^\\s*("
				 + allowedChars + "+)?([:](" + allowedChars + "*))?\\s*$");

			protected internal string owner = null;

			protected internal string group = null;

			// used by chown/chgrp
			///allows only "allowedChars" above in names for owner and group
			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				CommandFormat cf = new CommandFormat(2, int.MaxValue, "R");
				cf.Parse(args);
				SetRecursive(cf.GetOpt("R"));
				ParseOwnerGroup(args.RemoveFirst());
			}

			/// <summary>Parse the first argument into an owner and group</summary>
			/// <param name="ownerStr">string describing new ownership</param>
			protected internal virtual void ParseOwnerGroup(string ownerStr)
			{
				Matcher matcher = chownPattern.Matcher(ownerStr);
				if (!matcher.Matches())
				{
					throw new ArgumentException("'" + ownerStr + "' does not match expected pattern for [owner][:group]."
						);
				}
				owner = matcher.Group(1);
				group = matcher.Group(3);
				if (group != null && group.Length == 0)
				{
					group = null;
				}
				if (owner == null && group == null)
				{
					throw new ArgumentException("'" + ownerStr + "' does not specify owner or group."
						);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				//Should we do case insensitive match?
				string newOwner = (owner == null || owner.Equals(item.stat.GetOwner())) ? null : 
					owner;
				string newGroup = (group == null || group.Equals(item.stat.GetGroup())) ? null : 
					group;
				if (newOwner != null || newGroup != null)
				{
					try
					{
						item.fs.SetOwner(item.path, newOwner, newGroup);
					}
					catch (IOException e)
					{
						Log.Debug("Error changing ownership of " + item, e);
						throw new IOException("changing ownership of '" + item + "': " + e.Message);
					}
				}
			}
		}

		/// <summary>Used to change group of files</summary>
		public class Chgrp : FsShellPermissions.Chown
		{
			public const string Name = "chgrp";

			public const string Usage = "[-R] GROUP PATH...";

			public const string Description = "This is equivalent to -chown ... :GROUP ...";

			private static readonly Sharpen.Pattern chgrpPattern = Sharpen.Pattern.Compile("^\\s*("
				 + allowedChars + "+)\\s*$");

			protected internal override void ParseOwnerGroup(string groupStr)
			{
				Matcher matcher = chgrpPattern.Matcher(groupStr);
				if (!matcher.Matches())
				{
					throw new ArgumentException("'" + groupStr + "' does not match expected pattern for group"
						);
				}
				owner = null;
				group = matcher.Group(1);
			}
		}
	}
}
