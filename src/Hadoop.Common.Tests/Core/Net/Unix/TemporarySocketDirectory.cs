using System;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.Net.Unix
{
	/// <summary>Create a temporary directory in which sockets can be created.</summary>
	/// <remarks>
	/// Create a temporary directory in which sockets can be created.
	/// When creating a UNIX domain socket, the name
	/// must be fairly short (around 110 bytes on most platforms).
	/// </remarks>
	public class TemporarySocketDirectory : IDisposable
	{
		private FilePath dir;

		public TemporarySocketDirectory()
		{
			string tmp = Runtime.GetProperty("java.io.tmpdir", "/tmp");
			dir = new FilePath(tmp, "socks." + (Runtime.CurrentTimeMillis() + "." + (new Random
				().Next())));
			dir.Mkdirs();
			FileUtil.SetWritable(dir, true);
		}

		public virtual FilePath GetDir()
		{
			return dir;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			if (dir != null)
			{
				FileUtils.DeleteDirectory(dir);
				dir = null;
			}
		}

		~TemporarySocketDirectory()
		{
			Close();
		}
	}
}
