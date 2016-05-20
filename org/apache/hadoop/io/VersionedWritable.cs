using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A base class for Writables that provides version checking.</summary>
	/// <remarks>
	/// A base class for Writables that provides version checking.
	/// <p>This is useful when a class may evolve, so that instances written by the
	/// old version of the class may still be processed by the new version.  To
	/// handle this situation,
	/// <see cref="readFields(java.io.DataInput)"/>
	/// implementations should catch
	/// <see cref="VersionMismatchException"/>
	/// .
	/// </remarks>
	public abstract class VersionedWritable : org.apache.hadoop.io.Writable
	{
		/// <summary>Return the version number of the current implementation.</summary>
		public abstract byte getVersion();

		// javadoc from Writable
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeByte(getVersion());
		}

		// store version
		// javadoc from Writable
		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			byte version = @in.readByte();
			// read version
			if (version != getVersion())
			{
				throw new org.apache.hadoop.io.VersionMismatchException(getVersion(), version);
			}
		}
	}
}
