using System;
using System.IO;
using Com.Google.Common.IO;
using Com.Google.Common.Primitives;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>
	/// Class that represents a file on disk which stores a single <code>long</code>
	/// value, but does not make any effort to make it truly durable.
	/// </summary>
	/// <remarks>
	/// Class that represents a file on disk which stores a single <code>long</code>
	/// value, but does not make any effort to make it truly durable. This is in
	/// contrast to
	/// <see cref="PersistentLongFile"/>
	/// which fsync()s the value on every
	/// change.
	/// This should be used for values which are updated frequently (such that
	/// performance is important) and not required to be up-to-date for correctness.
	/// This class also differs in that it stores the value as binary data instead
	/// of a textual string.
	/// </remarks>
	public class BestEffortLongFile : IDisposable
	{
		private readonly FilePath file;

		private readonly long defaultVal;

		private long value;

		private FileChannel ch = null;

		private readonly ByteBuffer buf = ByteBuffer.Allocate(long.Size / 8);

		public BestEffortLongFile(FilePath file, long defaultVal)
		{
			this.file = file;
			this.defaultVal = defaultVal;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long Get()
		{
			LazyOpen();
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Set(long newVal)
		{
			LazyOpen();
			buf.Clear();
			buf.PutLong(newVal);
			buf.Flip();
			IOUtils.WriteFully(ch, buf, 0);
			value = newVal;
		}

		/// <exception cref="System.IO.IOException"/>
		private void LazyOpen()
		{
			if (ch != null)
			{
				return;
			}
			// Load current value.
			byte[] data = null;
			try
			{
				data = Files.ToByteArray(file);
			}
			catch (FileNotFoundException)
			{
			}
			// Expected - this will use default value.
			if (data != null && data.Length != 0)
			{
				if (data.Length != Longs.Bytes)
				{
					throw new IOException("File " + file + " had invalid length: " + data.Length);
				}
				value = Longs.FromByteArray(data);
			}
			else
			{
				value = defaultVal;
			}
			// Now open file for future writes.
			RandomAccessFile raf = new RandomAccessFile(file, "rw");
			try
			{
				ch = raf.GetChannel();
			}
			finally
			{
				if (ch == null)
				{
					IOUtils.CloseStream(raf);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			if (ch != null)
			{
				ch.Close();
				ch = null;
			}
		}
	}
}
