using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Adapts an
	/// <see cref="FSDataInputStream"/>
	/// to Avro's SeekableInput interface.
	/// </summary>
	public class AvroFSInput : java.io.Closeable, org.apache.avro.file.SeekableInput
	{
		private readonly org.apache.hadoop.fs.FSDataInputStream stream;

		private readonly long len;

		/// <summary>
		/// Construct given an
		/// <see cref="FSDataInputStream"/>
		/// and its length.
		/// </summary>
		public AvroFSInput(org.apache.hadoop.fs.FSDataInputStream @in, long len)
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
		public AvroFSInput(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 p)
		{
			org.apache.hadoop.fs.FileStatus status = fc.getFileStatus(p);
			this.len = status.getLen();
			this.stream = fc.open(p);
		}

		public virtual long length()
		{
			return len;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int read(byte[] b, int off, int len)
		{
			return stream.read(b, off, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void seek(long p)
		{
			stream.seek(p);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long tell()
		{
			return stream.getPos();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void close()
		{
			stream.close();
		}
	}
}
