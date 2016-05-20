using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Acl related operations</summary>
	internal class AclCommands : org.apache.hadoop.fs.shell.FsCommand
	{
		private static string GET_FACL = "getfacl";

		private static string SET_FACL = "setfacl";

		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.AclCommands.GetfaclCommand
				)), "-" + GET_FACL);
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.AclCommands.SetfaclCommand
				)), "-" + SET_FACL);
		}

		/// <summary>Implementing the '-getfacl' command for the the FsShell.</summary>
		public class GetfaclCommand : org.apache.hadoop.fs.shell.FsCommand
		{
			public static string NAME = GET_FACL;

			public static string USAGE = "[-R] <path>";

			public static string DESCRIPTION = "Displays the Access Control Lists" + " (ACLs) of files and directories. If a directory has a default ACL,"
				 + " then getfacl also displays the default ACL.\n" + "  -R: List the ACLs of all files and directories recursively.\n"
				 + "  <path>: File or directory to list.\n";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(0, int.MaxValue, "R");
				cf.parse(args);
				setRecursive(cf.getOpt("R"));
				if (args.isEmpty())
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("<path> is missing");
				}
				if (args.Count > 1)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("Too many arguments");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				@out.WriteLine("# file: " + item);
				@out.WriteLine("# owner: " + item.stat.getOwner());
				@out.WriteLine("# group: " + item.stat.getGroup());
				org.apache.hadoop.fs.permission.FsPermission perm = item.stat.getPermission();
				if (perm.getStickyBit())
				{
					@out.WriteLine("# flags: --" + (perm.getOtherAction().implies(org.apache.hadoop.fs.permission.FsAction
						.EXECUTE) ? "t" : "T"));
				}
				org.apache.hadoop.fs.permission.AclStatus aclStatus = item.fs.getAclStatus(item.path
					);
				System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> entries
					 = perm.getAclBit() ? aclStatus.getEntries() : java.util.Collections.emptyList<org.apache.hadoop.fs.permission.AclEntry
					>();
				org.apache.hadoop.fs.permission.ScopedAclEntries scopedEntries = new org.apache.hadoop.fs.permission.ScopedAclEntries
					(org.apache.hadoop.fs.permission.AclUtil.getAclFromPermAndEntries(perm, entries)
					);
				printAclEntriesForSingleScope(aclStatus, perm, scopedEntries.getAccessEntries());
				printAclEntriesForSingleScope(aclStatus, perm, scopedEntries.getDefaultEntries());
				@out.WriteLine();
			}

			/// <summary>Prints all the ACL entries in a single scope.</summary>
			/// <param name="aclStatus">AclStatus for the path</param>
			/// <param name="fsPerm">FsPermission for the path</param>
			/// <param name="entries">List<AclEntry> containing ACL entries of file</param>
			private void printAclEntriesForSingleScope(org.apache.hadoop.fs.permission.AclStatus
				 aclStatus, org.apache.hadoop.fs.permission.FsPermission fsPerm, System.Collections.Generic.IList
				<org.apache.hadoop.fs.permission.AclEntry> entries)
			{
				if (entries.isEmpty())
				{
					return;
				}
				if (org.apache.hadoop.fs.permission.AclUtil.isMinimalAcl(entries))
				{
					foreach (org.apache.hadoop.fs.permission.AclEntry entry in entries)
					{
						@out.WriteLine(entry);
					}
				}
				else
				{
					foreach (org.apache.hadoop.fs.permission.AclEntry entry in entries)
					{
						printExtendedAclEntry(aclStatus, fsPerm, entry);
					}
				}
			}

			/// <summary>Prints a single extended ACL entry.</summary>
			/// <remarks>
			/// Prints a single extended ACL entry.  If the mask restricts the
			/// permissions of the entry, then also prints the restricted version as the
			/// effective permissions.  The mask applies to all named entries and also
			/// the unnamed group entry.
			/// </remarks>
			/// <param name="aclStatus">AclStatus for the path</param>
			/// <param name="fsPerm">FsPermission for the path</param>
			/// <param name="entry">AclEntry extended ACL entry to print</param>
			private void printExtendedAclEntry(org.apache.hadoop.fs.permission.AclStatus aclStatus
				, org.apache.hadoop.fs.permission.FsPermission fsPerm, org.apache.hadoop.fs.permission.AclEntry
				 entry)
			{
				if (entry.getName() != null || entry.getType() == org.apache.hadoop.fs.permission.AclEntryType
					.GROUP)
				{
					org.apache.hadoop.fs.permission.FsAction entryPerm = entry.getPermission();
					org.apache.hadoop.fs.permission.FsAction effectivePerm = aclStatus.getEffectivePermission
						(entry, fsPerm);
					if (entryPerm != effectivePerm)
					{
						@out.WriteLine(string.format("%s\t#effective:%s", entry, effectivePerm.SYMBOL));
					}
					else
					{
						@out.WriteLine(entry);
					}
				}
				else
				{
					@out.WriteLine(entry);
				}
			}
		}

		/// <summary>Implementing the '-setfacl' command for the the FsShell.</summary>
		public class SetfaclCommand : org.apache.hadoop.fs.shell.FsCommand
		{
			public static string NAME = SET_FACL;

			public static string USAGE = "[-R] [{-b|-k} {-m|-x <acl_spec>} <path>]" + "|[--set <acl_spec> <path>]";

			public static string DESCRIPTION = "Sets Access Control Lists (ACLs)" + " of files and directories.\n"
				 + "Options:\n" + "  -b :Remove all but the base ACL entries. The entries for user,"
				 + " group and others are retained for compatibility with permission " + "bits.\n"
				 + "  -k :Remove the default ACL.\n" + "  -R :Apply operations to all files and directories recursively.\n"
				 + "  -m :Modify ACL. New entries are added to the ACL, and existing" + " entries are retained.\n"
				 + "  -x :Remove specified ACL entries. Other ACL entries are retained.\n" + "  --set :Fully replace the ACL, discarding all existing entries."
				 + " The <acl_spec> must include entries for user, group, and others" + " for compatibility with permission bits.\n"
				 + "  <acl_spec>: Comma separated list of ACL entries.\n" + "  <path>: File or directory to modify.\n";

			internal org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
				(0, int.MaxValue, "b", "k", "R", "m", "x", "-set");

			internal System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
				> aclEntries = null;

			internal System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
				> accessAclEntries = null;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				cf.parse(args);
				setRecursive(cf.getOpt("R"));
				// Mix of remove and modify acl flags are not allowed
				bool bothRemoveOptions = cf.getOpt("b") && cf.getOpt("k");
				bool bothModifyOptions = cf.getOpt("m") && cf.getOpt("x");
				bool oneRemoveOption = cf.getOpt("b") || cf.getOpt("k");
				bool oneModifyOption = cf.getOpt("m") || cf.getOpt("x");
				bool setOption = cf.getOpt("-set");
				if ((bothRemoveOptions || bothModifyOptions) || (oneRemoveOption && oneModifyOption
					) || (setOption && (oneRemoveOption || oneModifyOption)))
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("Specified flags contains both remove and modify flags"
						);
				}
				// Only -m, -x and --set expects <acl_spec>
				if (oneModifyOption || setOption)
				{
					if (args.Count < 2)
					{
						throw new org.apache.hadoop.HadoopIllegalArgumentException("<acl_spec> is missing"
							);
					}
					aclEntries = org.apache.hadoop.fs.permission.AclEntry.parseAclSpec(args.removeFirst
						(), !cf.getOpt("x"));
				}
				if (args.isEmpty())
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("<path> is missing");
				}
				if (args.Count > 1)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("Too many arguments");
				}
				// In recursive mode, save a separate list of just the access ACL entries.
				// Only directories may have a default ACL.  When a recursive operation
				// encounters a file under the specified path, it must pass only the
				// access ACL entries.
				if (isRecursive() && (oneModifyOption || setOption))
				{
					accessAclEntries = com.google.common.collect.Lists.newArrayList();
					foreach (org.apache.hadoop.fs.permission.AclEntry entry in aclEntries)
					{
						if (entry.getScope() == org.apache.hadoop.fs.permission.AclEntryScope.ACCESS)
						{
							accessAclEntries.add(entry);
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				if (cf.getOpt("b"))
				{
					item.fs.removeAcl(item.path);
				}
				else
				{
					if (cf.getOpt("k"))
					{
						item.fs.removeDefaultAcl(item.path);
					}
					else
					{
						if (cf.getOpt("m"))
						{
							System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> entries
								 = getAclEntries(item);
							if (!entries.isEmpty())
							{
								item.fs.modifyAclEntries(item.path, entries);
							}
						}
						else
						{
							if (cf.getOpt("x"))
							{
								System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> entries
									 = getAclEntries(item);
								if (!entries.isEmpty())
								{
									item.fs.removeAclEntries(item.path, entries);
								}
							}
							else
							{
								if (cf.getOpt("-set"))
								{
									System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> entries
										 = getAclEntries(item);
									if (!entries.isEmpty())
									{
										item.fs.setAcl(item.path, entries);
									}
								}
							}
						}
					}
				}
			}

			/// <summary>Returns the ACL entries to use in the API call for the given path.</summary>
			/// <remarks>
			/// Returns the ACL entries to use in the API call for the given path.  For a
			/// recursive operation, returns all specified ACL entries if the item is a
			/// directory or just the access ACL entries if the item is a file.  For a
			/// non-recursive operation, returns all specified ACL entries.
			/// </remarks>
			/// <param name="item">PathData path to check</param>
			/// <returns>List<AclEntry> ACL entries to use in the API call</returns>
			private System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
				> getAclEntries(org.apache.hadoop.fs.shell.PathData item)
			{
				if (isRecursive())
				{
					return item.stat.isDirectory() ? aclEntries : accessAclEntries;
				}
				else
				{
					return aclEntries;
				}
			}
		}
	}
}
