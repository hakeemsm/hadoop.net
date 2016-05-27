using System;
using Org.Apache.Avro.File;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Adapts an
	/// <see cref="FSDataInputStream"/>
	/// to Avro's SeekableInput interface.
	/// </summary>
	public class AvroFSInput : IDisposable, SeekableInput
	{
		private readonly FSDataInputStream stream;

		private readonly long len;

		/// <summary>
		/// Construct given an
		/// <see cref="FSDataInputStream"/>
		/// and its length.
		/// </summary>
		public AvroFSInput(FSDataInputStream @in, long len)
		{
			this.stream = @in;
			this.len = len;
		}

		/// <summary>
		/// Construct given a
		/// <see cref="FileContext"/>
		/// and a
		/// <see cref="Path"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public AvroFSInput(FileContext fc, Path p)
		{
			FileStatus status = fc.GetFileStatus(p);
			this.len = status.GetLen();
			this.stream = fc.Open(p);
		}

		public virtual long Length()
		{
			return len;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(byte[] b, int off, int len)
		{
			return stream.Read(b, off, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Seek(long p)
		{
			stream.Seek(p);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long Tell()
		{
			return stream.GetPos();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			stream.Close();
		}
	}
}
