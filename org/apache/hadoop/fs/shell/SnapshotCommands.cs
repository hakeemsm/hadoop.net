using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Snapshot related operations</summary>
	internal class SnapshotCommands : org.apache.hadoop.fs.shell.FsCommand
	{
		private const string CREATE_SNAPSHOT = "createSnapshot";

		private const string DELETE_SNAPSHOT = "deleteSnapshot";

		private const string RENAME_SNAPSHOT = "renameSnapshot";

		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.SnapshotCommands.CreateSnapshot
				)), "-" + CREATE_SNAPSHOT);
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.SnapshotCommands.DeleteSnapshot
				)), "-" + DELETE_SNAPSHOT);
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.SnapshotCommands.RenameSnapshot
				)), "-" + RENAME_SNAPSHOT);
		}

		/// <summary>Create a snapshot</summary>
		public class CreateSnapshot : org.apache.hadoop.fs.shell.FsCommand
		{
			public const string NAME = CREATE_SNAPSHOT;

			public const string USAGE = "<snapshotDir> [<snapshotName>]";

			public const string DESCRIPTION = "Create a snapshot on a directory";

			private string snapshotName = null;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				if (!item.stat.isDirectory())
				{
					throw new org.apache.hadoop.fs.PathIsNotDirectoryException(item.ToString());
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				if (args.Count == 0)
				{
					throw new System.ArgumentException("<snapshotDir> is missing.");
				}
				if (args.Count > 2)
				{
					throw new System.ArgumentException("Too many arguments.");
				}
				if (args.Count == 2)
				{
					snapshotName = args.removeLast();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processArguments(System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.PathData> items)
			{
				base.processArguments(items);
				if (numErrors != 0)
				{
					// check for error collecting paths
					return;
				}
				System.Diagnostics.Debug.Assert((items.Count == 1));
				org.apache.hadoop.fs.shell.PathData sroot = items.getFirst();
				org.apache.hadoop.fs.Path snapshotPath = sroot.fs.createSnapshot(sroot.path, snapshotName
					);
				@out.WriteLine("Created snapshot " + snapshotPath);
			}
		}

		/// <summary>Delete a snapshot</summary>
		public class DeleteSnapshot : org.apache.hadoop.fs.shell.FsCommand
		{
			public const string NAME = DELETE_SNAPSHOT;

			public const string USAGE = "<snapshotDir> <snapshotName>";

			public const string DESCRIPTION = "Delete a snapshot from a directory";

			private string snapshotName;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				if (!item.stat.isDirectory())
				{
					throw new org.apache.hadoop.fs.PathIsNotDirectoryException(item.ToString());
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				if (args.Count != 2)
				{
					throw new System.ArgumentException("Incorrect number of arguments.");
				}
				snapshotName = args.removeLast();
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processArguments(System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.PathData> items)
			{
				base.processArguments(items);
				if (numErrors != 0)
				{
					// check for error collecting paths
					return;
				}
				System.Diagnostics.Debug.Assert((items.Count == 1));
				org.apache.hadoop.fs.shell.PathData sroot = items.getFirst();
				sroot.fs.deleteSnapshot(sroot.path, snapshotName);
			}
		}

		/// <summary>Rename a snapshot</summary>
		public class RenameSnapshot : org.apache.hadoop.fs.shell.FsCommand
		{
			public const string NAME = RENAME_SNAPSHOT;

			public const string USAGE = "<snapshotDir> <oldName> <newName>";

			public const string DESCRIPTION = "Rename a snapshot from oldName to newName";

			private string oldName;

			private string newName;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				if (!item.stat.isDirectory())
				{
					throw new org.apache.hadoop.fs.PathIsNotDirectoryException(item.ToString());
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				if (args.Count != 3)
				{
					throw new System.ArgumentException("Incorrect number of arguments.");
				}
				newName = args.removeLast();
				oldName = args.removeLast();
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processArguments(System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.PathData> items)
			{
				base.processArguments(items);
				if (numErrors != 0)
				{
					// check for error collecting paths
					return;
				}
				com.google.common.@base.Preconditions.checkArgument(items.Count == 1);
				org.apache.hadoop.fs.shell.PathData sroot = items.getFirst();
				sroot.fs.renameSnapshot(sroot.path, oldName, newName);
			}
		}
	}
}
