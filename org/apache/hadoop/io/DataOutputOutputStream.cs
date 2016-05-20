using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>OutputStream implementation that wraps a DataOutput.</summary>
	public class DataOutputOutputStream : java.io.OutputStream
	{
		private readonly java.io.DataOutput @out;

		/// <summary>Construct an OutputStream from the given DataOutput.</summary>
		/// <remarks>
		/// Construct an OutputStream from the given DataOutput. If 'out'
		/// is already an OutputStream, simply returns it. Otherwise, wraps
		/// it in an OutputStream.
		/// </remarks>
		/// <param name="out">the DataOutput to wrap</param>
		/// <returns>an OutputStream instance that outputs to 'out'</returns>
		public static java.io.OutputStream constructOutputStream(java.io.DataOutput @out)
		{
			if (@out is java.io.OutputStream)
			{
				return (java.io.OutputStream)@out;
			}
			else
			{
				return new org.apache.hadoop.io.DataOutputOutputStream(@out);
			}
		}

		private DataOutputOutputStream(java.io.DataOutput @out)
		{
			this.@out = @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void write(int b)
		{
			@out.writeByte(b);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void write(byte[] b, int off, int len)
		{
			@out.write(b, off, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void write(byte[] b)
		{
			@out.write(b);
		}
	}
}
