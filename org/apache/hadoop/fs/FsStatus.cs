using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// This class is used to represent the capacity, free and used space on a
	/// <see cref="FileSystem"/>
	/// .
	/// </summary>
	public class FsStatus : org.apache.hadoop.io.Writable
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
		public virtual long getCapacity()
		{
			return capacity;
		}

		/// <summary>Return the number of bytes used on the file system</summary>
		public virtual long getUsed()
		{
			return used;
		}

		/// <summary>Return the number of remaining bytes on the file system</summary>
		public virtual long getRemaining()
		{
			return remaining;
		}

		//////////////////////////////////////////////////
		// Writable
		//////////////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeLong(capacity);
			@out.writeLong(used);
			@out.writeLong(remaining);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			capacity = @in.readLong();
			used = @in.readLong();
			remaining = @in.readLong();
		}
	}
}
