using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>
	/// Class that represents a file on disk which persistently stores
	/// a single <code>long</code> value.
	/// </summary>
	/// <remarks>
	/// Class that represents a file on disk which persistently stores
	/// a single <code>long</code> value. The file is updated atomically
	/// and durably (i.e fsynced).
	/// </remarks>
	public class PersistentLongFile
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Util.PersistentLongFile
			));

		private readonly FilePath file;

		private readonly long defaultVal;

		private long value;

		private bool loaded = false;

		public PersistentLongFile(FilePath file, long defaultVal)
		{
			this.file = file;
			this.defaultVal = defaultVal;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long Get()
		{
			if (!loaded)
			{
				value = ReadFile(file, defaultVal);
				loaded = true;
			}
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Set(long newVal)
		{
			if (value != newVal || !loaded)
			{
				WriteFile(file, newVal);
			}
			value = newVal;
			loaded = true;
		}

		/// <summary>Atomically write the given value to the given file, including fsyncing.</summary>
		/// <param name="file">destination file</param>
		/// <param name="val">value to write</param>
		/// <exception cref="System.IO.IOException">if the file cannot be written</exception>
		public static void WriteFile(FilePath file, long val)
		{
			AtomicFileOutputStream fos = new AtomicFileOutputStream(file);
			try
			{
				fos.Write(Sharpen.Runtime.GetBytesForString(val.ToString(), Charsets.Utf8));
				fos.Write('\n');
				fos.Close();
				fos = null;
			}
			finally
			{
				if (fos != null)
				{
					fos.Abort();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static long ReadFile(FilePath file, long defaultVal)
		{
			long val = defaultVal;
			if (file.Exists())
			{
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
					file), Charsets.Utf8));
				try
				{
					val = long.Parse(br.ReadLine());
					br.Close();
					br = null;
				}
				finally
				{
					IOUtils.Cleanup(Log, br);
				}
			}
			return val;
		}
	}
}
