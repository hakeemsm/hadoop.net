using System.IO;
using Hadoop.Common.Core.Fs;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// This class is used to represent the capacity, free and used space on a
	/// <see cref="FileSystem"/>
	/// .
	/// </summary>
	public class FsStatus : IWritable
	{
		private long capacity;

		private long used;

		private long remaining;

		/// <summary>Construct a FsStatus object, using the specified statistics</summary>
		public FsStatus(long capacity, long used, long remaining)
		{
			this.capacity = capacity;
			this.used = used;
			this.remaining = remaining;
		}

		/// <summary>Return the capacity in bytes of the file system</summary>
		public virtual long GetCapacity()
		{
			return capacity;
		}

		/// <summary>Return the number of bytes used on the file system</summary>
		public virtual long GetUsed()
		{
			return used;
		}

		/// <summary>Return the number of remaining bytes on the file system</summary>
		public virtual long GetRemaining()
		{
			return remaining;
		}

		//////////////////////////////////////////////////
		// Writable
		//////////////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteLong(capacity);
			@out.WriteLong(used);
			@out.WriteLong(remaining);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			capacity = @in.ReadLong();
			used = @in.ReadLong();
			remaining = @in.ReadLong();
		}
	}
}
