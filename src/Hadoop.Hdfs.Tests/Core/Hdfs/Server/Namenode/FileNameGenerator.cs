using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>File name generator.</summary>
	/// <remarks>
	/// File name generator.
	/// Each directory contains not more than a fixed number (filesPerDir)
	/// of files and directories.
	/// When the number of files in one directory reaches the maximum,
	/// the generator creates a new directory and proceeds generating files in it.
	/// The generated namespace tree is balanced that is any path to a leaf
	/// file is not less than the height of the tree minus one.
	/// </remarks>
	public class FileNameGenerator
	{
		private const int DefaultFilesPerDirectory = 32;

		private readonly int[] pathIndecies = new int[20];

		private readonly string baseDir;

		private string currentDir;

		private readonly int filesPerDirectory;

		private long fileCount;

		internal FileNameGenerator(string baseDir)
			: this(baseDir, DefaultFilesPerDirectory)
		{
		}

		internal FileNameGenerator(string baseDir, int filesPerDir)
		{
			// this will support up to 32**20 = 2**100 = 10**30 files
			this.baseDir = baseDir;
			this.filesPerDirectory = filesPerDir;
			Reset();
		}

		internal virtual string GetNextDirName(string prefix)
		{
			int depth = 0;
			while (pathIndecies[depth] >= 0)
			{
				depth++;
			}
			int level;
			for (level = depth - 1; level >= 0 && pathIndecies[level] == filesPerDirectory - 
				1; level--)
			{
				pathIndecies[level] = 0;
			}
			if (level < 0)
			{
				pathIndecies[depth] = 0;
			}
			else
			{
				pathIndecies[level]++;
			}
			level = 0;
			string next = baseDir;
			while (pathIndecies[level] >= 0)
			{
				next = next + "/" + prefix + pathIndecies[level++];
			}
			return next;
		}

		internal virtual string GetNextFileName(string fileNamePrefix)
		{
			lock (this)
			{
				long fNum = fileCount % filesPerDirectory;
				if (fNum == 0)
				{
					currentDir = GetNextDirName(fileNamePrefix + "Dir");
				}
				string fn = currentDir + "/" + fileNamePrefix + fileCount;
				fileCount++;
				return fn;
			}
		}

		private void Reset()
		{
			lock (this)
			{
				Arrays.Fill(pathIndecies, -1);
				fileCount = 0L;
				currentDir = string.Empty;
			}
		}

		internal virtual int GetFilesPerDirectory()
		{
			lock (this)
			{
				return filesPerDirectory;
			}
		}

		internal virtual string GetCurrentDir()
		{
			lock (this)
			{
				return currentDir;
			}
		}
	}
}
