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
	/// A tool used to list all snapshottable directories that are owned by the
	/// current user.
	/// </summary>
	/// <remarks>
	/// A tool used to list all snapshottable directories that are owned by the
	/// current user. The tool returns all the snapshottable directories if the user
	/// is a super user.
	/// </remarks>
	public class LsSnapshottableDir : Configured, Tool
	{
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] argv)
		{
			string description = "hdfs lsSnapshottableDir: \n" + "\tGet the list of snapshottable directories that are owned by the current user.\n"
				 + "\tReturn all the snapshottable directories if the current user is a super user.\n";
			if (argv.Length != 0)
			{
				System.Console.Error.WriteLine("Usage: \n" + description);
				return 1;
			}
			FileSystem fs = FileSystem.Get(GetConf());
			if (!(fs is DistributedFileSystem))
			{
				System.Console.Error.WriteLine("LsSnapshottableDir can only be used in DistributedFileSystem"
					);
				return 1;
			}
			DistributedFileSystem dfs = (DistributedFileSystem)fs;
			try
			{
				SnapshottableDirectoryStatus[] stats = dfs.GetSnapshottableDirListing();
				SnapshottableDirectoryStatus.Print(stats, System.Console.Out);
			}
			catch (IOException e)
			{
				string[] content = e.GetLocalizedMessage().Split("\n");
				System.Console.Error.WriteLine("lsSnapshottableDir: " + content[0]);
				return 1;
			}
			return 0;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			int rc = ToolRunner.Run(new LsSnapshottableDir(), argv);
			System.Environment.Exit(rc);
		}
	}
}
