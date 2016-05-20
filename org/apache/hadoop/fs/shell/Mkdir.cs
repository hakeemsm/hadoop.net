using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Create the given dir</summary>
	internal class Mkdir : org.apache.hadoop.fs.shell.FsCommand
	{
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Mkdir
				)), "-mkdir");
		}

		public const string NAME = "mkdir";

		public const string USAGE = "[-p] <path> ...";

		public const string DESCRIPTION = "Create a directory in specified location.\n" +
			 "-p: Do not fail if the directory already exists";

		private bool createParents;

		protected internal override void processOptions(System.Collections.Generic.LinkedList
			<string> args)
		{
			org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
				(1, int.MaxValue, "p");
			cf.parse(args);
			createParents = cf.getOpt("p");
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
			item)
		{
			if (item.stat.isDirectory())
			{
				if (!createParents)
				{
					throw new org.apache.hadoop.fs.PathExistsException(item.ToString());
				}
			}
			else
			{
				throw new org.apache.hadoop.fs.PathIsNotDirectoryException(item.ToString());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processNonexistentPath(org.apache.hadoop.fs.shell.PathData
			 item)
		{
			// check if parent exists. this is complicated because getParent(a/b/c/) returns a/b/c, but
			// we want a/b
			if (!item.fs.exists(new org.apache.hadoop.fs.Path(item.path.ToString()).getParent
				()) && !createParents)
			{
				throw new org.apache.hadoop.fs.PathNotFoundException(item.ToString());
			}
			if (!item.fs.mkdirs(item.path))
			{
				throw new org.apache.hadoop.fs.PathIOException(item.ToString());
			}
		}
	}
}
