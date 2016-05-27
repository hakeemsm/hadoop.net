using Sharpen;

namespace Org.Apache.Hadoop.Nfs
{
	/// <summary>Class encapsulates different types of files</summary>
	[System.Serializable]
	public sealed class NfsFileType
	{
		public static readonly Org.Apache.Hadoop.Nfs.NfsFileType Nfsreg = new Org.Apache.Hadoop.Nfs.NfsFileType
			(1);

		public static readonly Org.Apache.Hadoop.Nfs.NfsFileType Nfsdir = new Org.Apache.Hadoop.Nfs.NfsFileType
			(2);

		public static readonly Org.Apache.Hadoop.Nfs.NfsFileType Nfsblk = new Org.Apache.Hadoop.Nfs.NfsFileType
			(3);

		public static readonly Org.Apache.Hadoop.Nfs.NfsFileType Nfschr = new Org.Apache.Hadoop.Nfs.NfsFileType
			(4);

		public static readonly Org.Apache.Hadoop.Nfs.NfsFileType Nfslnk = new Org.Apache.Hadoop.Nfs.NfsFileType
			(5);

		public static readonly Org.Apache.Hadoop.Nfs.NfsFileType Nfssock = new Org.Apache.Hadoop.Nfs.NfsFileType
			(6);

		public static readonly Org.Apache.Hadoop.Nfs.NfsFileType Nfsfifo = new Org.Apache.Hadoop.Nfs.NfsFileType
			(7);

		private readonly int value;

		internal NfsFileType(int val)
		{
			// a regular file
			// a directory
			// a block special device file
			// a character special device
			// a symbolic link
			// a socket
			// a named pipe
			Org.Apache.Hadoop.Nfs.NfsFileType.value = val;
		}

		public int ToValue()
		{
			return Org.Apache.Hadoop.Nfs.NfsFileType.value;
		}
	}
}
