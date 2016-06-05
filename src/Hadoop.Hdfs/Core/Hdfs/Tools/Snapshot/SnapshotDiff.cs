using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.Snapshot
{
	/// <summary>
	/// A tool used to get the difference report between two snapshots, or between
	/// a snapshot and the current status of a directory.
	/// </summary>
	/// <remarks>
	/// A tool used to get the difference report between two snapshots, or between
	/// a snapshot and the current status of a directory.
	/// <pre>
	/// Usage: SnapshotDiff snapshotDir from to
	/// For from/to, users can use "." to present the current status, and use
	/// ".snapshot/snapshot_name" to present a snapshot, where ".snapshot/" can be
	/// omitted.
	/// </pre>
	/// </remarks>
	public class SnapshotDiff : Configured, Tool
	{
		private static string GetSnapshotName(string name)
		{
			if (Path.CurDir.Equals(name))
			{
				// current directory
				return string.Empty;
			}
			int i;
			if (name.StartsWith(HdfsConstants.DotSnapshotDir + Path.Separator))
			{
				i = 0;
			}
			else
			{
				if (name.StartsWith(HdfsConstants.SeparatorDotSnapshotDir + Path.Separator))
				{
					i = 1;
				}
				else
				{
					return name;
				}
			}
			// get the snapshot name
			return Sharpen.Runtime.Substring(name, i + HdfsConstants.DotSnapshotDir.Length + 
				1);
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] argv)
		{
			string description = "hdfs snapshotDiff <snapshotDir> <from> <to>:\n" + "\tGet the difference between two snapshots, \n"
				 + "\tor between a snapshot and the current tree of a directory.\n" + "\tFor <from>/<to>, users can use \".\" to present the current status,\n"
				 + "\tand use \".snapshot/snapshot_name\" to present a snapshot,\n" + "\twhere \".snapshot/\" can be omitted\n";
			if (argv.Length != 3)
			{
				System.Console.Error.WriteLine("Usage: \n" + description);
				return 1;
			}
			FileSystem fs = FileSystem.Get(GetConf());
			if (!(fs is DistributedFileSystem))
			{
				System.Console.Error.WriteLine("SnapshotDiff can only be used in DistributedFileSystem"
					);
				return 1;
			}
			DistributedFileSystem dfs = (DistributedFileSystem)fs;
			Path snapshotRoot = new Path(argv[0]);
			string fromSnapshot = GetSnapshotName(argv[1]);
			string toSnapshot = GetSnapshotName(argv[2]);
			try
			{
				SnapshotDiffReport diffReport = dfs.GetSnapshotDiffReport(snapshotRoot, fromSnapshot
					, toSnapshot);
				System.Console.Out.WriteLine(diffReport.ToString());
			}
			catch (IOException e)
			{
				string[] content = e.GetLocalizedMessage().Split("\n");
				System.Console.Error.WriteLine("snapshotDiff: " + content[0]);
				return 1;
			}
			return 0;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			int rc = ToolRunner.Run(new SnapshotDiff(), argv);
			System.Environment.Exit(rc);
		}
	}
}
