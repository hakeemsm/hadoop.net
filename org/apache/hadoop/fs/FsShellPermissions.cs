using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>This class is the home for file permissions related commands.</summary>
	/// <remarks>
	/// This class is the home for file permissions related commands.
	/// Moved to this separate class since FsShell is getting too large.
	/// </remarks>
	public class FsShellPermissions : org.apache.hadoop.fs.shell.FsCommand
	{
		internal static org.apache.commons.logging.Log LOG = org.apache.hadoop.fs.FsShell
			.LOG;

		/// <summary>Register the permission related commands with the factory</summary>
		/// <param name="factory">the command factory</param>
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FsShellPermissions.Chmod
				)), "-chmod");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FsShellPermissions.Chown
				)), "-chown");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FsShellPermissions.Chgrp
				)), "-chgrp");
		}

		/// <summary>The pattern is almost as flexible as mode allowed by chmod shell command.
		/// 	</summary>
		/// <remarks>
		/// The pattern is almost as flexible as mode allowed by chmod shell command.
		/// The main restriction is that we recognize only rwxXt. To reduce errors we
		/// also enforce octal mode specifications of either 3 digits without a sticky
		/// bit setting or four digits with a sticky bit setting.
		/// </remarks>
		public class Chmod : org.apache.hadoop.fs.FsShellPermissions
		{
			public const string NAME = "chmod";

			public const string USAGE = "[-R] <MODE[,MODE]... | OCTALMODE> PATH...";

			public const string DESCRIPTION = "Changes permissions of a file. " + "This works similar to the shell's chmod command with a few exceptions.\n"
				 + "-R: modifies the files recursively. This is the only option" + " currently supported.\n"
				 + "<MODE>: Mode is the same as mode used for the shell's command. " + "The only letters recognized are 'rwxXt', e.g. +t,a+r,g-w,+rwx,o=r.\n"
				 + "<OCTALMODE>: Mode specifed in 3 or 4 digits. If 4 digits, the first " + "may be 1 or 0 to turn the sticky bit on or off, respectively.  Unlike "
				 + "the shell command, it is not possible to specify only part of the " + "mode, e.g. 754 is same as u=rwx,g=rx,o=r.\n\n"
				 + "If none of 'augo' is specified, 'a' is assumed and unlike the " + "shell command, no umask is applied.";

			protected internal org.apache.hadoop.fs.permission.ChmodParser pp;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(2, int.MaxValue, "R", null);
				cf.parse(args);
				setRecursive(cf.getOpt("R"));
				string modeStr = args.removeFirst();
				try
				{
					pp = new org.apache.hadoop.fs.permission.ChmodParser(modeStr);
				}
				catch (System.ArgumentException)
				{
					// TODO: remove "chmod : " so it's not doubled up in output, but it's
					// here for backwards compatibility...
					throw new System.ArgumentException("chmod : mode '" + modeStr + "' does not match the expected pattern."
						);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				short newperms = pp.applyNewPermission(item.stat);
				if (item.stat.getPermission().toShort() != newperms)
				{
					try
					{
						item.fs.setPermission(item.path, new org.apache.hadoop.fs.permission.FsPermission
							(newperms));
					}
					catch (System.IO.IOException e)
					{
						LOG.debug("Error changing permissions of " + item, e);
						throw new System.IO.IOException("changing permissions of '" + item + "': " + e.Message
							);
					}
				}
			}
		}

		private static string allowedChars = org.apache.hadoop.util.Shell.WINDOWS ? "[-_./@a-zA-Z0-9 ]"
			 : "[-_./@a-zA-Z0-9]";

		/// <summary>Used to change owner and/or group of files</summary>
		public class Chown : org.apache.hadoop.fs.FsShellPermissions
		{
			public const string NAME = "chown";

			public const string USAGE = "[-R] [OWNER][:[GROUP]] PATH...";

			public static readonly string DESCRIPTION = "Changes owner and group of a file. "
				 + "This is similar to the shell's chown command with a few exceptions.\n" + "-R: modifies the files recursively. This is the only option "
				 + "currently supported.\n\n" + "If only the owner or group is specified, then only the owner or "
				 + "group is modified. " + "The owner and group names may only consist of digits, alphabet, "
				 + "and any of " + allowedChars + ". The names are case sensitive.\n\n" + "WARNING: Avoid using '.' to separate user name and group though "
				 + "Linux allows it. If user names have dots in them and you are " + "using local file system, you might see surprising results since "
				 + "the shell command 'chown' is used for local files.";

			private static readonly java.util.regex.Pattern chownPattern = java.util.regex.Pattern
				.compile("^\\s*(" + allowedChars + "+)?([:](" + allowedChars + "*))?\\s*$");

			protected internal string owner = null;

			protected internal string group = null;

			// used by chown/chgrp
			///allows only "allowedChars" above in names for owner and group
			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(2, int.MaxValue, "R");
				cf.parse(args);
				setRecursive(cf.getOpt("R"));
				parseOwnerGroup(args.removeFirst());
			}

			/// <summary>Parse the first argument into an owner and group</summary>
			/// <param name="ownerStr">string describing new ownership</param>
			protected internal virtual void parseOwnerGroup(string ownerStr)
			{
				java.util.regex.Matcher matcher = chownPattern.matcher(ownerStr);
				if (!matcher.matches())
				{
					throw new System.ArgumentException("'" + ownerStr + "' does not match expected pattern for [owner][:group]."
						);
				}
				owner = matcher.group(1);
				group = matcher.group(3);
				if (group != null && group.Length == 0)
				{
					group = null;
				}
				if (owner == null && group == null)
				{
					throw new System.ArgumentException("'" + ownerStr + "' does not specify owner or group."
						);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				//Should we do case insensitive match?
				string newOwner = (owner == null || owner.Equals(item.stat.getOwner())) ? null : 
					owner;
				string newGroup = (group == null || group.Equals(item.stat.getGroup())) ? null : 
					group;
				if (newOwner != null || newGroup != null)
				{
					try
					{
						item.fs.setOwner(item.path, newOwner, newGroup);
					}
					catch (System.IO.IOException e)
					{
						LOG.debug("Error changing ownership of " + item, e);
						throw new System.IO.IOException("changing ownership of '" + item + "': " + e.Message
							);
					}
				}
			}
		}

		/// <summary>Used to change group of files</summary>
		public class Chgrp : org.apache.hadoop.fs.FsShellPermissions.Chown
		{
			public const string NAME = "chgrp";

			public const string USAGE = "[-R] GROUP PATH...";

			public const string DESCRIPTION = "This is equivalent to -chown ... :GROUP ...";

			private static readonly java.util.regex.Pattern chgrpPattern = java.util.regex.Pattern
				.compile("^\\s*(" + allowedChars + "+)\\s*$");

			protected internal override void parseOwnerGroup(string groupStr)
			{
				java.util.regex.Matcher matcher = chgrpPattern.matcher(groupStr);
				if (!matcher.matches())
				{
					throw new System.ArgumentException("'" + groupStr + "' does not match expected pattern for group"
						);
				}
				owner = null;
				group = matcher.group(1);
			}
		}
	}
}
