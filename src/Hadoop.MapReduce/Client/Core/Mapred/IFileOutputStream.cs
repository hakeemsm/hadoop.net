using System.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A Checksum output stream.</summary>
	/// <remarks>
	/// A Checksum output stream.
	/// Checksum for the contents of the file is calculated and
	/// appended to the end of the file on close of the stream.
	/// Used for IFiles
	/// </remarks>
	public class IFileOutputStream : FilterOutputStream
	{
		/// <summary>The output stream to be checksummed.</summary>
		private readonly DataChecksum sum;

		private byte[] barray;

		private bool closed = false;

		private bool finished = false;

		/// <summary>
		/// Create a checksum output stream that writes
		/// the bytes to the given stream.
		/// </summary>
		/// <param name="out"/>
		public IFileOutputStream(OutputStream @out)
			: base(@out)
		{
			sum = DataChecksum.NewDataChecksum(DataChecksum.Type.Crc32, int.MaxValue);
			barray = new byte[sum.GetChecksumSize()];
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			if (closed)
			{
				return;
			}
			closed = true;
			Finish();
			@out.Close();
		}

		/// <summary>
		/// Finishes writing data to the output stream, by writing
		/// the checksum bytes to the end.
		/// </summary>
		/// <remarks>
		/// Finishes writing data to the output stream, by writing
		/// the checksum bytes to the end. The underlying stream is not closed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Finish()
		{
			if (finished)
			{
				return;
			}
			finished = true;
			sum.WriteValue(barray, 0, false);
			@out.Write(barray, 0, sum.GetChecksumSize());
			@out.Flush();
		}

		/// <summary>Write bytes to the stream.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Write(byte[] b, int off, int len)
		{
			sum.Update(b, off, len);
			@out.Write(b, off, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(int b)
		{
			barray[0] = unchecked((byte)(b & unchecked((int)(0xFF))));
			Write(barray, 0, 1);
		}
	}
}
