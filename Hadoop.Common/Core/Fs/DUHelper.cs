using System;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS
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

		public static long GetFolderUsage(string folder)
		{
			return new Org.Apache.Hadoop.FS.DUHelper().CalculateFolderSize(folder);
		}

		private long CalculateFolderSize(string folder)
		{
			if (folder == null)
			{
				throw new ArgumentException("folder");
			}
			FilePath f = new FilePath(folder);
			return folderSize = GetFileSize(f);
		}

		public virtual string Check(string folder)
		{
			if (folder == null)
			{
				throw new ArgumentException("folder");
			}
			FilePath f = new FilePath(folder);
			folderSize = GetFileSize(f);
			usage = 1.0 * (f.GetTotalSpace() - f.GetFreeSpace()) / f.GetTotalSpace();
			return string.Format("used %d files %d disk in use %f", folderSize, fileCount, usage
				);
		}

		public virtual long GetFileCount()
		{
			return fileCount;
		}

		public virtual double GetUsage()
		{
			return usage;
		}

		private long GetFileSize(FilePath folder)
		{
			folderCount++;
			//Counting the total folders
			long foldersize = 0;
			if (folder.IsFile())
			{
				return folder.Length();
			}
			FilePath[] filelist = folder.ListFiles();
			if (filelist == null)
			{
				return 0;
			}
			for (int i = 0; i < filelist.Length; i++)
			{
				if (filelist[i].IsDirectory())
				{
					foldersize += GetFileSize(filelist[i]);
				}
				else
				{
					fileCount++;
					//Counting the total files
					foldersize += filelist[i].Length();
				}
			}
			return foldersize;
		}

		public static void Main(string[] args)
		{
			if (Shell.Windows)
			{
				System.Console.Out.WriteLine("Windows: " + Org.Apache.Hadoop.FS.DUHelper.GetFolderUsage
					(args[0]));
			}
			else
			{
				System.Console.Out.WriteLine("Other: " + Org.Apache.Hadoop.FS.DUHelper.GetFolderUsage
					(args[0]));
			}
		}
	}
}
