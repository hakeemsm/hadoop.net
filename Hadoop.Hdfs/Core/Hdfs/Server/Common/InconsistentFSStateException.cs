using System;
using System.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Common
{
	/// <summary>
	/// The exception is thrown when file system state is inconsistent
	/// and is not recoverable.
	/// </summary>
	[System.Serializable]
	public class InconsistentFSStateException : IOException
	{
		private const long serialVersionUID = 1L;

		public InconsistentFSStateException(FilePath dir, string descr)
			: base("Directory " + GetFilePath(dir) + " is in an inconsistent state: " + descr
				)
		{
		}

		public InconsistentFSStateException(FilePath dir, string descr, Exception ex)
			: this(dir, descr + "\n" + StringUtils.StringifyException(ex))
		{
		}

		private static string GetFilePath(FilePath dir)
		{
			try
			{
				return dir.GetCanonicalPath();
			}
			catch (IOException)
			{
			}
			return dir.GetPath();
		}
	}
}
