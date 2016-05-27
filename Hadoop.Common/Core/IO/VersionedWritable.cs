using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A base class for Writables that provides version checking.</summary>
	/// <remarks>
	/// A base class for Writables that provides version checking.
	/// <p>This is useful when a class may evolve, so that instances written by the
	/// old version of the class may still be processed by the new version.  To
	/// handle this situation,
	/// <see cref="ReadFields(System.IO.DataInput)"/>
	/// implementations should catch
	/// <see cref="VersionMismatchException"/>
	/// .
	/// </remarks>
	public abstract class VersionedWritable : Writable
	{
		/// <summary>Return the version number of the current implementation.</summary>
		public abstract byte GetVersion();

		// javadoc from Writable
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteByte(GetVersion());
		}

		// store version
		// javadoc from Writable
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			byte version = @in.ReadByte();
			// read version
			if (version != GetVersion())
			{
				throw new VersionMismatchException(GetVersion(), version);
			}
		}
	}
}
