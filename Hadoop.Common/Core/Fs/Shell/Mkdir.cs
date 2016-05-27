using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Create the given dir</summary>
	internal class Mkdir : FsCommand
	{
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(Mkdir), "-mkdir");
		}

		public const string Name = "mkdir";

		public const string Usage = "[-p] <path> ...";

		public const string Description = "Create a directory in specified location.\n" +
			 "-p: Do not fail if the directory already exists";

		private bool createParents;

		protected internal override void ProcessOptions(List<string> args)
		{
			CommandFormat cf = new CommandFormat(1, int.MaxValue, "p");
			cf.Parse(args);
			createParents = cf.GetOpt("p");
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessPath(PathData item)
		{
			if (item.stat.IsDirectory())
			{
				if (!createParents)
				{
					throw new PathExistsException(item.ToString());
				}
			}
			else
			{
				throw new PathIsNotDirectoryException(item.ToString());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessNonexistentPath(PathData item)
		{
			// check if parent exists. this is complicated because getParent(a/b/c/) returns a/b/c, but
			// we want a/b
			if (!item.fs.Exists(new Path(item.path.ToString()).GetParent()) && !createParents)
			{
				throw new PathNotFoundException(item.ToString());
			}
			if (!item.fs.Mkdirs(item.path))
			{
				throw new PathIOException(item.ToString());
			}
		}
	}
}
