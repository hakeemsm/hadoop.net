using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Hadoop.Common.Core.Fs.Shell;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Snapshot related operations</summary>
	internal class SnapshotCommands : FsCommand
	{
		private const string CreateSnapshot = "createSnapshot";

		private const string DeleteSnapshot = "deleteSnapshot";

		private const string RenameSnapshot = "renameSnapshot";

		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(SnapshotCommands.CreateSnapshot), "-" + CreateSnapshot);
			factory.AddClass(typeof(SnapshotCommands.DeleteSnapshot), "-" + DeleteSnapshot);
			factory.AddClass(typeof(SnapshotCommands.RenameSnapshot), "-" + RenameSnapshot);
		}

		/// <summary>Create a snapshot</summary>
		public class CreateSnapshot : FsCommand
		{
			public const string Name = CreateSnapshot;

			public const string Usage = "<snapshotDir> [<snapshotName>]";

			public const string Description = "Create a snapshot on a directory";

			private string snapshotName = null;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				if (!item.stat.IsDirectory())
				{
					throw new PathIsNotDirectoryException(item.ToString());
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				if (args.Count == 0)
				{
					throw new ArgumentException("<snapshotDir> is missing.");
				}
				if (args.Count > 2)
				{
					throw new ArgumentException("Too many arguments.");
				}
				if (args.Count == 2)
				{
					snapshotName = args.RemoveLast();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessArguments(List<PathData> items)
			{
				base.ProcessArguments(items);
				if (numErrors != 0)
				{
					// check for error collecting paths
					return;
				}
				System.Diagnostics.Debug.Assert((items.Count == 1));
				PathData sroot = items.GetFirst();
				Path snapshotPath = sroot.fs.CreateSnapshot(sroot.path, snapshotName);
				@out.WriteLine("Created snapshot " + snapshotPath);
			}
		}

		/// <summary>Delete a snapshot</summary>
		public class DeleteSnapshot : FsCommand
		{
			public const string Name = DeleteSnapshot;

			public const string Usage = "<snapshotDir> <snapshotName>";

			public const string Description = "Delete a snapshot from a directory";

			private string snapshotName;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				if (!item.stat.IsDirectory())
				{
					throw new PathIsNotDirectoryException(item.ToString());
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				if (args.Count != 2)
				{
					throw new ArgumentException("Incorrect number of arguments.");
				}
				snapshotName = args.RemoveLast();
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessArguments(List<PathData> items)
			{
				base.ProcessArguments(items);
				if (numErrors != 0)
				{
					// check for error collecting paths
					return;
				}
				System.Diagnostics.Debug.Assert((items.Count == 1));
				PathData sroot = items.GetFirst();
				sroot.fs.DeleteSnapshot(sroot.path, snapshotName);
			}
		}

		/// <summary>Rename a snapshot</summary>
		public class RenameSnapshot : FsCommand
		{
			public const string Name = RenameSnapshot;

			public const string Usage = "<snapshotDir> <oldName> <newName>";

			public const string Description = "Rename a snapshot from oldName to newName";

			private string oldName;

			private string newName;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				if (!item.stat.IsDirectory())
				{
					throw new PathIsNotDirectoryException(item.ToString());
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				if (args.Count != 3)
				{
					throw new ArgumentException("Incorrect number of arguments.");
				}
				newName = args.RemoveLast();
				oldName = args.RemoveLast();
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessArguments(List<PathData> items)
			{
				base.ProcessArguments(items);
				if (numErrors != 0)
				{
					// check for error collecting paths
					return;
				}
				Preconditions.CheckArgument(items.Count == 1);
				PathData sroot = items.GetFirst();
				sroot.fs.RenameSnapshot(sroot.path, oldName, newName);
			}
		}
	}
}
