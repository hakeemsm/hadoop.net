using System.Collections.Generic;
using Com.Google.Common.Collect;
using Hadoop.Common.Core.Fs.Shell;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Acl related operations</summary>
	internal class AclCommands : FsCommand
	{
		private static string GetFacl = "getfacl";

		private static string SetFacl = "setfacl";

		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(AclCommands.GetfaclCommand), "-" + GetFacl);
			factory.AddClass(typeof(AclCommands.SetfaclCommand), "-" + SetFacl);
		}

		/// <summary>Implementing the '-getfacl' command for the the FsShell.</summary>
		public class GetfaclCommand : FsCommand
		{
			public static string Name = GetFacl;

			public static string Usage = "[-R] <path>";

			public static string Description = "Displays the Access Control Lists" + " (ACLs) of files and directories. If a directory has a default ACL,"
				 + " then getfacl also displays the default ACL.\n" + "  -R: List the ACLs of all files and directories recursively.\n"
				 + "  <path>: File or directory to list.\n";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				CommandFormat cf = new CommandFormat(0, int.MaxValue, "R");
				cf.Parse(args);
				SetRecursive(cf.GetOpt("R"));
				if (args.IsEmpty())
				{
					throw new HadoopIllegalArgumentException("<path> is missing");
				}
				if (args.Count > 1)
				{
					throw new HadoopIllegalArgumentException("Too many arguments");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				@out.WriteLine("# file: " + item);
				@out.WriteLine("# owner: " + item.stat.GetOwner());
				@out.WriteLine("# group: " + item.stat.GetGroup());
				FsPermission perm = item.stat.GetPermission();
				if (perm.GetStickyBit())
				{
					@out.WriteLine("# flags: --" + (perm.GetOtherAction().Implies(FsAction.Execute) ? 
						"t" : "T"));
				}
				AclStatus aclStatus = item.fs.GetAclStatus(item.path);
				IList<AclEntry> entries = perm.GetAclBit() ? aclStatus.GetEntries() : Sharpen.Collections
					.EmptyList<AclEntry>();
				ScopedAclEntries scopedEntries = new ScopedAclEntries(AclUtil.GetAclFromPermAndEntries
					(perm, entries));
				PrintAclEntriesForSingleScope(aclStatus, perm, scopedEntries.GetAccessEntries());
				PrintAclEntriesForSingleScope(aclStatus, perm, scopedEntries.GetDefaultEntries());
				@out.WriteLine();
			}

			/// <summary>Prints all the ACL entries in a single scope.</summary>
			/// <param name="aclStatus">AclStatus for the path</param>
			/// <param name="fsPerm">FsPermission for the path</param>
			/// <param name="entries">List<AclEntry> containing ACL entries of file</param>
			private void PrintAclEntriesForSingleScope(AclStatus aclStatus, FsPermission fsPerm
				, IList<AclEntry> entries)
			{
				if (entries.IsEmpty())
				{
					return;
				}
				if (AclUtil.IsMinimalAcl(entries))
				{
					foreach (AclEntry entry in entries)
					{
						@out.WriteLine(entry);
					}
				}
				else
				{
					foreach (AclEntry entry in entries)
					{
						PrintExtendedAclEntry(aclStatus, fsPerm, entry);
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
			private void PrintExtendedAclEntry(AclStatus aclStatus, FsPermission fsPerm, AclEntry
				 entry)
			{
				if (entry.GetName() != null || entry.GetType() == AclEntryType.Group)
				{
					FsAction entryPerm = entry.GetPermission();
					FsAction effectivePerm = aclStatus.GetEffectivePermission(entry, fsPerm);
					if (entryPerm != effectivePerm)
					{
						@out.WriteLine(string.Format("%s\t#effective:%s", entry, effectivePerm.Symbol));
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
		public class SetfaclCommand : FsCommand
		{
			public static string Name = SetFacl;

			public static string Usage = "[-R] [{-b|-k} {-m|-x <acl_spec>} <path>]" + "|[--set <acl_spec> <path>]";

			public static string Description = "Sets Access Control Lists (ACLs)" + " of files and directories.\n"
				 + "Options:\n" + "  -b :Remove all but the base ACL entries. The entries for user,"
				 + " group and others are retained for compatibility with permission " + "bits.\n"
				 + "  -k :Remove the default ACL.\n" + "  -R :Apply operations to all files and directories recursively.\n"
				 + "  -m :Modify ACL. New entries are added to the ACL, and existing" + " entries are retained.\n"
				 + "  -x :Remove specified ACL entries. Other ACL entries are retained.\n" + "  --set :Fully replace the ACL, discarding all existing entries."
				 + " The <acl_spec> must include entries for user, group, and others" + " for compatibility with permission bits.\n"
				 + "  <acl_spec>: Comma separated list of ACL entries.\n" + "  <path>: File or directory to modify.\n";

			internal CommandFormat cf = new CommandFormat(0, int.MaxValue, "b", "k", "R", "m"
				, "x", "-set");

			internal IList<AclEntry> aclEntries = null;

			internal IList<AclEntry> accessAclEntries = null;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				cf.Parse(args);
				SetRecursive(cf.GetOpt("R"));
				// Mix of remove and modify acl flags are not allowed
				bool bothRemoveOptions = cf.GetOpt("b") && cf.GetOpt("k");
				bool bothModifyOptions = cf.GetOpt("m") && cf.GetOpt("x");
				bool oneRemoveOption = cf.GetOpt("b") || cf.GetOpt("k");
				bool oneModifyOption = cf.GetOpt("m") || cf.GetOpt("x");
				bool setOption = cf.GetOpt("-set");
				if ((bothRemoveOptions || bothModifyOptions) || (oneRemoveOption && oneModifyOption
					) || (setOption && (oneRemoveOption || oneModifyOption)))
				{
					throw new HadoopIllegalArgumentException("Specified flags contains both remove and modify flags"
						);
				}
				// Only -m, -x and --set expects <acl_spec>
				if (oneModifyOption || setOption)
				{
					if (args.Count < 2)
					{
						throw new HadoopIllegalArgumentException("<acl_spec> is missing");
					}
					aclEntries = AclEntry.ParseAclSpec(args.RemoveFirst(), !cf.GetOpt("x"));
				}
				if (args.IsEmpty())
				{
					throw new HadoopIllegalArgumentException("<path> is missing");
				}
				if (args.Count > 1)
				{
					throw new HadoopIllegalArgumentException("Too many arguments");
				}
				// In recursive mode, save a separate list of just the access ACL entries.
				// Only directories may have a default ACL.  When a recursive operation
				// encounters a file under the specified path, it must pass only the
				// access ACL entries.
				if (IsRecursive() && (oneModifyOption || setOption))
				{
					accessAclEntries = Lists.NewArrayList();
					foreach (AclEntry entry in aclEntries)
					{
						if (entry.GetScope() == AclEntryScope.Access)
						{
							accessAclEntries.AddItem(entry);
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				if (cf.GetOpt("b"))
				{
					item.fs.RemoveAcl(item.path);
				}
				else
				{
					if (cf.GetOpt("k"))
					{
						item.fs.RemoveDefaultAcl(item.path);
					}
					else
					{
						if (cf.GetOpt("m"))
						{
							IList<AclEntry> entries = GetAclEntries(item);
							if (!entries.IsEmpty())
							{
								item.fs.ModifyAclEntries(item.path, entries);
							}
						}
						else
						{
							if (cf.GetOpt("x"))
							{
								IList<AclEntry> entries = GetAclEntries(item);
								if (!entries.IsEmpty())
								{
									item.fs.RemoveAclEntries(item.path, entries);
								}
							}
							else
							{
								if (cf.GetOpt("-set"))
								{
									IList<AclEntry> entries = GetAclEntries(item);
									if (!entries.IsEmpty())
									{
										item.fs.SetAcl(item.path, entries);
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
			private IList<AclEntry> GetAclEntries(PathData item)
			{
				if (IsRecursive())
				{
					return item.stat.IsDirectory() ? aclEntries : accessAclEntries;
				}
				else
				{
					return aclEntries;
				}
			}
		}
	}
}
