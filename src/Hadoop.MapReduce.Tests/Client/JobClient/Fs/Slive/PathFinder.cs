using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Class which generates a file or directory path using a simple random
	/// generation algorithm stated in http://issues.apache.org/jira/browse/HDFS-708
	/// </summary>
	internal class PathFinder
	{
		private enum Type
		{
			File,
			Directory
		}

		private const string DirPrefix = "sl_dir_";

		private const string FilePrefix = "sl_file_";

		private Path basePath;

		private ConfigExtractor config;

		private Random rnd;

		internal PathFinder(ConfigExtractor cfg, Random rnd)
		{
			this.basePath = cfg.GetDataPath();
			this.config = cfg;
			this.rnd = rnd;
		}

		/// <summary>
		/// This function uses a simple recursive algorithm to generate a path name
		/// using the current id % limitPerDir and using current id / limitPerDir to
		/// form the rest of the tree segments
		/// </summary>
		/// <param name="curId">
		/// the current id to use for determining the current directory id %
		/// per directory limit and then used for determining the next segment
		/// of the path to use, if &lt;= zero this will return the base path
		/// </param>
		/// <param name="limitPerDir">
		/// the per directory file limit used in modulo and division
		/// operations to calculate the file name and path tree
		/// </param>
		/// <param name="type">directory or file enumeration</param>
		/// <returns>Path</returns>
		private Path GetPath(int curId, int limitPerDir, PathFinder.Type type)
		{
			if (curId <= 0)
			{
				return basePath;
			}
			string name = string.Empty;
			switch (type)
			{
				case PathFinder.Type.File:
				{
					name = FilePrefix + curId % limitPerDir.ToString();
					break;
				}

				case PathFinder.Type.Directory:
				{
					name = DirPrefix + curId % limitPerDir.ToString();
					break;
				}
			}
			Path @base = GetPath((curId / limitPerDir), limitPerDir, PathFinder.Type.Directory
				);
			return new Path(@base, name);
		}

		/// <summary>
		/// Gets a file path using the given configuration provided total files and
		/// files per directory
		/// </summary>
		/// <returns>path</returns>
		internal virtual Path GetFile()
		{
			int fileLimit = config.GetTotalFiles();
			int dirLimit = config.GetDirSize();
			int startPoint = 1 + rnd.Next(fileLimit);
			return GetPath(startPoint, dirLimit, PathFinder.Type.File);
		}

		/// <summary>
		/// Gets a directory path using the given configuration provided total files
		/// and files per directory
		/// </summary>
		/// <returns>path</returns>
		internal virtual Path GetDirectory()
		{
			int fileLimit = config.GetTotalFiles();
			int dirLimit = config.GetDirSize();
			int startPoint = rnd.Next(fileLimit);
			return GetPath(startPoint, dirLimit, PathFinder.Type.Directory);
		}
	}
}
