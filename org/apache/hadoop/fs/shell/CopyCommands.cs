using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Various commands for copy files</summary>
	internal class CopyCommands
	{
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CopyCommands.Merge
				)), "-getmerge");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CopyCommands.Cp
				)), "-cp");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CopyCommands.CopyFromLocal
				)), "-copyFromLocal");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CopyCommands.CopyToLocal
				)), "-copyToLocal");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CopyCommands.Get
				)), "-get");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CopyCommands.Put
				)), "-put");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CopyCommands.AppendToFile
				)), "-appendToFile");
		}

		/// <summary>merge multiple files together</summary>
		public class Merge : org.apache.hadoop.fs.shell.FsCommand
		{
			public const string NAME = "getmerge";

			public const string USAGE = "[-nl] <src> <localdst>";

			public const string DESCRIPTION = "Get all the files in the directories that " + 
				"match the source file pattern and merge and sort them to only " + "one file on local fs. <src> is kept.\n"
				 + "-nl: Add a newline character at the end of each file.";

			protected internal org.apache.hadoop.fs.shell.PathData dst = null;

			protected internal string delimiter = null;

			protected internal System.Collections.Generic.IList<org.apache.hadoop.fs.shell.PathData
				> srcs = null;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				try
				{
					org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
						(2, int.MaxValue, "nl");
					cf.parse(args);
					delimiter = cf.getOpt("nl") ? "\n" : null;
					dst = new org.apache.hadoop.fs.shell.PathData(new java.net.URI(args.removeLast())
						, getConf());
					if (dst.exists && dst.stat.isDirectory())
					{
						throw new org.apache.hadoop.fs.PathIsDirectoryException(dst.ToString());
					}
					srcs = new System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData
						>();
				}
				catch (java.net.URISyntaxException e)
				{
					throw new System.IO.IOException("unexpected URISyntaxException", e);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processArguments(System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.PathData> items)
			{
				base.processArguments(items);
				if (exitCode != 0)
				{
					// check for error collecting paths
					return;
				}
				org.apache.hadoop.fs.FSDataOutputStream @out = dst.fs.create(dst.path);
				try
				{
					foreach (org.apache.hadoop.fs.shell.PathData src in srcs)
					{
						org.apache.hadoop.fs.FSDataInputStream @in = src.fs.open(src.path);
						try
						{
							org.apache.hadoop.io.IOUtils.copyBytes(@in, @out, getConf(), false);
							if (delimiter != null)
							{
								@out.write(Sharpen.Runtime.getBytesForString(delimiter, "UTF-8"));
							}
						}
						finally
						{
							@in.close();
						}
					}
				}
				finally
				{
					@out.close();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processNonexistentPath(org.apache.hadoop.fs.shell.PathData
				 item)
			{
				exitCode = 1;
				// flag that a path is bad
				base.processNonexistentPath(item);
			}

			// this command is handled a bit differently than others.  the paths
			// are batched up instead of actually being processed.  this avoids
			// unnecessarily streaming into the merge file and then encountering
			// a path error that should abort the merge
			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				src)
			{
				// for directories, recurse one level to get its files, else skip it
				if (src.stat.isDirectory())
				{
					if (getDepth() == 0)
					{
						recursePath(src);
					}
				}
				else
				{
					// skip subdirs
					srcs.add(src);
				}
			}
		}

		internal class Cp : org.apache.hadoop.fs.shell.CommandWithDestination
		{
			public const string NAME = "cp";

			public const string USAGE = "[-f] [-p | -p[topax]] <src> ... <dst>";

			public const string DESCRIPTION = "Copy files that match the file pattern <src> to a "
				 + "destination.  When copying multiple files, the destination " + "must be a directory. Passing -p preserves status "
				 + "[topax] (timestamps, ownership, permission, ACLs, XAttr). " + "If -p is specified with no <arg>, then preserves "
				 + "timestamps, ownership, permission. If -pa is specified, " + "then preserves permission also because ACL is a super-set of "
				 + "permission. Passing -f overwrites the destination if it " + "already exists. raw namespace extended attributes are preserved "
				 + "if (1) they are supported (HDFS only) and, (2) all of the source and " + "target pathnames are in the /.reserved/raw hierarchy. raw namespace "
				 + "xattr preservation is determined solely by the presence (or absence) " + "of the /.reserved/raw prefix and not by the -p option.\n";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				popPreserveOption(args);
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(2, int.MaxValue, "f");
				cf.parse(args);
				setOverwrite(cf.getOpt("f"));
				// should have a -r option
				setRecursive(true);
				getRemoteDestination(args);
			}

			private void popPreserveOption(System.Collections.Generic.IList<string> args)
			{
				for (System.Collections.Generic.IEnumerator<string> iter = args.GetEnumerator(); 
					iter.MoveNext(); )
				{
					string cur = iter.Current;
					if (cur.Equals("--"))
					{
						// stop parsing arguments when you see --
						break;
					}
					else
					{
						if (cur.StartsWith("-p"))
						{
							iter.remove();
							if (cur.Length == 2)
							{
								setPreserve(true);
							}
							else
							{
								string attributes = Sharpen.Runtime.substring(cur, 2);
								for (int index = 0; index < attributes.Length; index++)
								{
									preserve(org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute.getAttribute
										(attributes[index]));
								}
							}
							return;
						}
					}
				}
			}
		}

		/// <summary>Copy local files to a remote filesystem</summary>
		public class Get : org.apache.hadoop.fs.shell.CommandWithDestination
		{
			public const string NAME = "get";

			public const string USAGE = "[-p] [-ignoreCrc] [-crc] <src> ... <localdst>";

			public const string DESCRIPTION = "Copy files that match the file pattern <src> "
				 + "to the local name.  <src> is kept.  When copying multiple " + "files, the destination must be a directory. Passing "
				 + "-p preserves access and modification times, " + "ownership and the mode.\n";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(1, int.MaxValue, "crc", "ignoreCrc", "p");
				cf.parse(args);
				setWriteChecksum(cf.getOpt("crc"));
				setVerifyChecksum(!cf.getOpt("ignoreCrc"));
				setPreserve(cf.getOpt("p"));
				setRecursive(true);
				getLocalDestination(args);
			}
		}

		/// <summary>Copy local files to a remote filesystem</summary>
		public class Put : org.apache.hadoop.fs.shell.CommandWithDestination
		{
			public const string NAME = "put";

			public const string USAGE = "[-f] [-p] [-l] <localsrc> ... <dst>";

			public const string DESCRIPTION = "Copy files from the local file system " + "into fs. Copying fails if the file already "
				 + "exists, unless the -f flag is given.\n" + "Flags:\n" + "  -p : Preserves access and modification times, ownership and the mode.\n"
				 + "  -f : Overwrites the destination if it already exists.\n" + "  -l : Allow DataNode to lazily persist the file to disk. Forces\n"
				 + "       replication factor of 1. This flag will result in reduced\n" + "       durability. Use with care.\n";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(1, int.MaxValue, "f", "p", "l");
				cf.parse(args);
				setOverwrite(cf.getOpt("f"));
				setPreserve(cf.getOpt("p"));
				setLazyPersist(cf.getOpt("l"));
				getRemoteDestination(args);
				// should have a -r option
				setRecursive(true);
			}

			// commands operating on local paths have no need for glob expansion
			/// <exception cref="System.IO.IOException"/>
			protected internal override System.Collections.Generic.IList<org.apache.hadoop.fs.shell.PathData
				> expandArgument(string arg)
			{
				System.Collections.Generic.IList<org.apache.hadoop.fs.shell.PathData> items = new 
					System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData>();
				try
				{
					items.add(new org.apache.hadoop.fs.shell.PathData(new java.net.URI(arg), getConf(
						)));
				}
				catch (java.net.URISyntaxException e)
				{
					if (org.apache.hadoop.fs.Path.WINDOWS)
					{
						// Unlike URI, PathData knows how to parse Windows drive-letter paths.
						items.add(new org.apache.hadoop.fs.shell.PathData(arg, getConf()));
					}
					else
					{
						throw new System.IO.IOException("unexpected URISyntaxException", e);
					}
				}
				return items;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processArguments(System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.PathData> args)
			{
				// NOTE: this logic should be better, mimics previous implementation
				if (args.Count == 1 && args[0].ToString().Equals("-"))
				{
					copyStreamToTarget(Sharpen.Runtime.@in, getTargetPath(args[0]));
					return;
				}
				base.processArguments(args);
			}
		}

		public class CopyFromLocal : org.apache.hadoop.fs.shell.CopyCommands.Put
		{
			public const string NAME = "copyFromLocal";

			public const string USAGE = org.apache.hadoop.fs.shell.CopyCommands.Put.USAGE;

			public const string DESCRIPTION = "Identical to the -put command.";
		}

		public class CopyToLocal : org.apache.hadoop.fs.shell.CopyCommands.Get
		{
			public const string NAME = "copyToLocal";

			public const string USAGE = org.apache.hadoop.fs.shell.CopyCommands.Get.USAGE;

			public const string DESCRIPTION = "Identical to the -get command.";
		}

		/// <summary>
		/// Append the contents of one or more local files to a remote
		/// file.
		/// </summary>
		public class AppendToFile : org.apache.hadoop.fs.shell.CommandWithDestination
		{
			public const string NAME = "appendToFile";

			public const string USAGE = "<localsrc> ... <dst>";

			public const string DESCRIPTION = "Appends the contents of all the given local files to the "
				 + "given dst file. The dst file will be created if it does " + "not exist. If <localSrc> is -, then the input is read "
				 + "from stdin.";

			private const int DEFAULT_IO_LENGTH = 1024 * 1024;

			internal bool readStdin = false;

			// commands operating on local paths have no need for glob expansion
			/// <exception cref="System.IO.IOException"/>
			protected internal override System.Collections.Generic.IList<org.apache.hadoop.fs.shell.PathData
				> expandArgument(string arg)
			{
				System.Collections.Generic.IList<org.apache.hadoop.fs.shell.PathData> items = new 
					System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData>();
				if (arg.Equals("-"))
				{
					readStdin = true;
				}
				else
				{
					try
					{
						items.add(new org.apache.hadoop.fs.shell.PathData(new java.net.URI(arg), getConf(
							)));
					}
					catch (java.net.URISyntaxException e)
					{
						if (org.apache.hadoop.fs.Path.WINDOWS)
						{
							// Unlike URI, PathData knows how to parse Windows drive-letter paths.
							items.add(new org.apache.hadoop.fs.shell.PathData(arg, getConf()));
						}
						else
						{
							throw new System.IO.IOException("Unexpected URISyntaxException: " + e.ToString());
						}
					}
				}
				return items;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				if (args.Count < 2)
				{
					throw new System.IO.IOException("missing destination argument");
				}
				getRemoteDestination(args);
				base.processOptions(args);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processArguments(System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.PathData> args)
			{
				if (!dst.exists)
				{
					dst.fs.create(dst.path, false).close();
				}
				java.io.InputStream @is = null;
				org.apache.hadoop.fs.FSDataOutputStream fos = dst.fs.append(dst.path);
				try
				{
					if (readStdin)
					{
						if (args.Count == 0)
						{
							org.apache.hadoop.io.IOUtils.copyBytes(Sharpen.Runtime.@in, fos, DEFAULT_IO_LENGTH
								);
						}
						else
						{
							throw new System.IO.IOException("stdin (-) must be the sole input argument when present"
								);
						}
					}
					// Read in each input file and write to the target.
					foreach (org.apache.hadoop.fs.shell.PathData source in args)
					{
						@is = new java.io.FileInputStream(source.toFile());
						org.apache.hadoop.io.IOUtils.copyBytes(@is, fos, DEFAULT_IO_LENGTH);
						org.apache.hadoop.io.IOUtils.closeStream(@is);
						@is = null;
					}
				}
				finally
				{
					if (@is != null)
					{
						org.apache.hadoop.io.IOUtils.closeStream(@is);
					}
					if (fos != null)
					{
						org.apache.hadoop.io.IOUtils.closeStream(fos);
					}
				}
			}
		}
	}
}
