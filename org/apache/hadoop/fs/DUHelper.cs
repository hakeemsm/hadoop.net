using Sharpen;

namespace org.apache.hadoop.fs
{
	public class DUHelper
	{
		private int folderCount = 0;

		private int fileCount = 0;

		private double usage = 0;

		private long folderSize = -1;

		private DUHelper()
		{
		}

		public static long getFolderUsage(string folder)
		{
			return new org.apache.hadoop.fs.DUHelper().calculateFolderSize(folder);
		}

		private long calculateFolderSize(string folder)
		{
			if (folder == null)
			{
				throw new System.ArgumentException("folder");
			}
			java.io.File f = new java.io.File(folder);
			return folderSize = getFileSize(f);
		}

		public virtual string check(string folder)
		{
			if (folder == null)
			{
				throw new System.ArgumentException("folder");
			}
			java.io.File f = new java.io.File(folder);
			folderSize = getFileSize(f);
			usage = 1.0 * (f.getTotalSpace() - f.getFreeSpace()) / f.getTotalSpace();
			return string.format("used %d files %d disk in use %f", folderSize, fileCount, usage
				);
		}

		public virtual long getFileCount()
		{
			return fileCount;
		}

		public virtual double getUsage()
		{
			return usage;
		}

		private long getFileSize(java.io.File folder)
		{
			folderCount++;
			//Counting the total folders
			long foldersize = 0;
			if (folder.isFile())
			{
				return folder.length();
			}
			java.io.File[] filelist = folder.listFiles();
			if (filelist == null)
			{
				return 0;
			}
			for (int i = 0; i < filelist.Length; i++)
			{
				if (filelist[i].isDirectory())
				{
					foldersize += getFileSize(filelist[i]);
				}
				else
				{
					fileCount++;
					//Counting the total files
					foldersize += filelist[i].length();
				}
			}
			return foldersize;
		}

		public static void Main(string[] args)
		{
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				System.Console.Out.WriteLine("Windows: " + org.apache.hadoop.fs.DUHelper.getFolderUsage
					(args[0]));
			}
			else
			{
				System.Console.Out.WriteLine("Other: " + org.apache.hadoop.fs.DUHelper.getFolderUsage
					(args[0]));
			}
		}
	}
}
