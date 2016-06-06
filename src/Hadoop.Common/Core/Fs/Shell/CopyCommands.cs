using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Fs.Shell;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Various commands for copy files</summary>
	internal class CopyCommands
	{
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(CopyCommands.Merge), "-getmerge");
			factory.AddClass(typeof(CopyCommands.CP), "-cp");
			factory.AddClass(typeof(CopyCommands.CopyFromLocal), "-copyFromLocal");
			factory.AddClass(typeof(CopyCommands.CopyToLocal), "-copyToLocal");
			factory.AddClass(typeof(CopyCommands.Get), "-get");
			factory.AddClass(typeof(CopyCommands.Put), "-put");
			factory.AddClass(typeof(CopyCommands.AppendToFile), "-appendToFile");
		}

		/// <summary>merge multiple files together</summary>
		public class Merge : FsCommand
		{
			public const string Name = "getmerge";

			public const string Usage = "[-nl] <src> <localdst>";

			public const string Description = "Get all the files in the directories that " + 
				"match the source file pattern and merge and sort them to only " + "one file on local fs. <src> is kept.\n"
				 + "-nl: Add a newline character at the end of each file.";

			protected internal PathData dst = null;

			protected internal string delimiter = null;

			protected internal IList<PathData> srcs = null;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				try
				{
					CommandFormat cf = new CommandFormat(2, int.MaxValue, "nl");
					cf.Parse(args);
					delimiter = cf.GetOpt("nl") ? "\n" : null;
					dst = new PathData(new URI(args.RemoveLast()), GetConf());
					if (dst.exists && dst.stat.IsDirectory())
					{
						throw new PathIsDirectoryException(dst.ToString());
					}
					srcs = new List<PathData>();
				}
				catch (URISyntaxException e)
				{
					throw new IOException("unexpected URISyntaxException", e);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessArguments(List<PathData> items)
			{
				base.ProcessArguments(items);
				if (exitCode != 0)
				{
					// check for error collecting paths
					return;
				}
				FSDataOutputStream @out = dst.fs.Create(dst.path);
				try
				{
					foreach (PathData src in srcs)
					{
						FSDataInputStream @in = src.fs.Open(src.path);
						try
						{
							IOUtils.CopyBytes(@in, @out, GetConf(), false);
							if (delimiter != null)
							{
								@out.Write(Runtime.GetBytesForString(delimiter, "UTF-8"));
							}
						}
						finally
						{
							@in.Close();
						}
					}
				}
				finally
				{
					@out.Close();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessNonexistentPath(PathData item)
			{
				exitCode = 1;
				// flag that a path is bad
				base.ProcessNonexistentPath(item);
			}

			// this command is handled a bit differently than others.  the paths
			// are batched up instead of actually being processed.  this avoids
			// unnecessarily streaming into the merge file and then encountering
			// a path error that should abort the merge
			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData src)
			{
				// for directories, recurse one level to get its files, else skip it
				if (src.stat.IsDirectory())
				{
					if (GetDepth() == 0)
					{
						RecursePath(src);
					}
				}
				else
				{
					// skip subdirs
					srcs.AddItem(src);
				}
			}
		}

		internal class CP : CommandWithDestination
		{
			public const string Name = "cp";

			public const string Usage = "[-f] [-p | -p[topax]] <src> ... <dst>";

			public const string Description = "Copy files that match the file pattern <src> to a "
				 + "destination.  When copying multiple files, the destination " + "must be a directory. Passing -p preserves status "
				 + "[topax] (timestamps, ownership, permission, ACLs, XAttr). " + "If -p is specified with no <arg>, then preserves "
				 + "timestamps, ownership, permission. If -pa is specified, " + "then preserves permission also because ACL is a super-set of "
				 + "permission. Passing -f overwrites the destination if it " + "already exists. raw namespace extended attributes are preserved "
				 + "if (1) they are supported (HDFS only) and, (2) all of the source and " + "target pathnames are in the /.reserved/raw hierarchy. raw namespace "
				 + "xattr preservation is determined solely by the presence (or absence) " + "of the /.reserved/raw prefix and not by the -p option.\n";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				PopPreserveOption(args);
				CommandFormat cf = new CommandFormat(2, int.MaxValue, "f");
				cf.Parse(args);
				SetOverwrite(cf.GetOpt("f"));
				// should have a -r option
				SetRecursive(true);
				GetRemoteDestination(args);
			}

			private void PopPreserveOption(IList<string> args)
			{
				for (IEnumerator<string> iter = args.GetEnumerator(); iter.HasNext(); )
				{
					string cur = iter.Next();
					if (cur.Equals("--"))
					{
						// stop parsing arguments when you see --
						break;
					}
					else
					{
						if (cur.StartsWith("-p"))
						{
							iter.Remove();
							if (cur.Length == 2)
							{
								SetPreserve(true);
							}
							else
							{
								string attributes = Runtime.Substring(cur, 2);
								for (int index = 0; index < attributes.Length; index++)
								{
									Preserve(CommandWithDestination.FileAttribute.GetAttribute(attributes[index]));
								}
							}
							return;
						}
					}
				}
			}
		}

		/// <summary>Copy local files to a remote filesystem</summary>
		public class Get : CommandWithDestination
		{
			public const string Name = "get";

			public const string Usage = "[-p] [-ignoreCrc] [-crc] <src> ... <localdst>";

			public const string Description = "Copy files that match the file pattern <src> "
				 + "to the local name.  <src> is kept.  When copying multiple " + "files, the destination must be a directory. Passing "
				 + "-p preserves access and modification times, " + "ownership and the mode.\n";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				CommandFormat cf = new CommandFormat(1, int.MaxValue, "crc", "ignoreCrc", "p");
				cf.Parse(args);
				SetWriteChecksum(cf.GetOpt("crc"));
				SetVerifyChecksum(!cf.GetOpt("ignoreCrc"));
				SetPreserve(cf.GetOpt("p"));
				SetRecursive(true);
				GetLocalDestination(args);
			}
		}

		/// <summary>Copy local files to a remote filesystem</summary>
		public class Put : CommandWithDestination
		{
			public const string Name = "put";

			public const string Usage = "[-f] [-p] [-l] <localsrc> ... <dst>";

			public const string Description = "Copy files from the local file system " + "into fs. Copying fails if the file already "
				 + "exists, unless the -f flag is given.\n" + "Flags:\n" + "  -p : Preserves access and modification times, ownership and the mode.\n"
				 + "  -f : Overwrites the destination if it already exists.\n" + "  -l : Allow DataNode to lazily persist the file to disk. Forces\n"
				 + "       replication factor of 1. This flag will result in reduced\n" + "       durability. Use with care.\n";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				CommandFormat cf = new CommandFormat(1, int.MaxValue, "f", "p", "l");
				cf.Parse(args);
				SetOverwrite(cf.GetOpt("f"));
				SetPreserve(cf.GetOpt("p"));
				SetLazyPersist(cf.GetOpt("l"));
				GetRemoteDestination(args);
				// should have a -r option
				SetRecursive(true);
			}

			// commands operating on local paths have no need for glob expansion
			/// <exception cref="System.IO.IOException"/>
			protected internal override IList<PathData> ExpandArgument(string arg)
			{
				IList<PathData> items = new List<PathData>();
				try
				{
					items.AddItem(new PathData(new URI(arg), GetConf()));
				}
				catch (URISyntaxException e)
				{
					if (Path.Windows)
					{
						// Unlike URI, PathData knows how to parse Windows drive-letter paths.
						items.AddItem(new PathData(arg, GetConf()));
					}
					else
					{
						throw new IOException("unexpected URISyntaxException", e);
					}
				}
				return items;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessArguments(List<PathData> args)
			{
				// NOTE: this logic should be better, mimics previous implementation
				if (args.Count == 1 && args[0].ToString().Equals("-"))
				{
					CopyStreamToTarget(Runtime.@in, GetTargetPath(args[0]));
					return;
				}
				base.ProcessArguments(args);
			}
		}

		public class CopyFromLocal : CopyCommands.Put
		{
			public const string Name = "copyFromLocal";

			public const string Usage = CopyCommands.Put.Usage;

			public const string Description = "Identical to the -put command.";
		}

		public class CopyToLocal : CopyCommands.Get
		{
			public const string Name = "copyToLocal";

			public const string Usage = CopyCommands.Get.Usage;

			public const string Description = "Identical to the -get command.";
		}

		/// <summary>
		/// Append the contents of one or more local files to a remote
		/// file.
		/// </summary>
		public class AppendToFile : CommandWithDestination
		{
			public const string Name = "appendToFile";

			public const string Usage = "<localsrc> ... <dst>";

			public const string Description = "Appends the contents of all the given local files to the "
				 + "given dst file. The dst file will be created if it does " + "not exist. If <localSrc> is -, then the input is read "
				 + "from stdin.";

			private const int DefaultIoLength = 1024 * 1024;

			internal bool readStdin = false;

			// commands operating on local paths have no need for glob expansion
			/// <exception cref="System.IO.IOException"/>
			protected internal override IList<PathData> ExpandArgument(string arg)
			{
				IList<PathData> items = new List<PathData>();
				if (arg.Equals("-"))
				{
					readStdin = true;
				}
				else
				{
					try
					{
						items.AddItem(new PathData(new URI(arg), GetConf()));
					}
					catch (URISyntaxException e)
					{
						if (Path.Windows)
						{
							// Unlike URI, PathData knows how to parse Windows drive-letter paths.
							items.AddItem(new PathData(arg, GetConf()));
						}
						else
						{
							throw new IOException("Unexpected URISyntaxException: " + e.ToString());
						}
					}
				}
				return items;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				if (args.Count < 2)
				{
					throw new IOException("missing destination argument");
				}
				GetRemoteDestination(args);
				base.ProcessOptions(args);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessArguments(List<PathData> args)
			{
				if (!dst.exists)
				{
					dst.fs.Create(dst.path, false).Close();
				}
				InputStream @is = null;
				FSDataOutputStream fos = dst.fs.Append(dst.path);
				try
				{
					if (readStdin)
					{
						if (args.Count == 0)
						{
							IOUtils.CopyBytes(Runtime.@in, fos, DefaultIoLength);
						}
						else
						{
							throw new IOException("stdin (-) must be the sole input argument when present");
						}
					}
					// Read in each input file and write to the target.
					foreach (PathData source in args)
					{
						@is = new FileInputStream(source.ToFile());
						IOUtils.CopyBytes(@is, fos, DefaultIoLength);
						IOUtils.CloseStream(@is);
						@is = null;
					}
				}
				finally
				{
					if (@is != null)
					{
						IOUtils.CloseStream(@is);
					}
					if (fos != null)
					{
						IOUtils.CloseStream(fos);
					}
				}
			}
		}
	}
}
