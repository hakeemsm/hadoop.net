using System.IO;

namespace Hadoop.Common.Core.IO
{
	/// <summary>OutputStream implementation that wraps a BinaryWriter.</summary>
	public class DataOutputOutputStream : OutputStream
	{
		private readonly BinaryWriter writer;

		/// <summary>Construct an OutputStream from the given BinaryWriter.</summary>
		/// <remarks>
		/// Construct an OutputStream from the given BinaryWriter. If 'out'
		/// is already an OutputStream, simply returns it. Otherwise, wraps
		/// it in an OutputStream.
		/// </remarks>
		/// <param name="out">the BinaryWriter to wrap</param>
		/// <returns>an OutputStream instance that outputs to 'out'</returns>
		public static OutputStream ConstructOutputStream(BinaryWriter writer)
		{
			if (@out is OutputStream)
			{
				return (OutputStream)@out;
			}
			else
			{
				return new DataOutputOutputStream(@out);
			}
		}

		private DataOutputOutputStream(BinaryWriter writer)
		{
			this.@out = @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(int b)
		{
			@out.WriteByte(b);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(byte[] b, int off, int len)
		{
			@out.Write(b, off, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(byte[] b)
		{
			@out.Write(b);
		}
	}
}
