using System.IO;
using Hadoop.Common.Core.IO;
using ICSharpCode.SharpZipLib.Zip.Compression;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// A base-class for Writables which store themselves compressed and lazily
	/// inflate on field access.
	/// </summary>
	/// <remarks>
	/// A base-class for Writables which store themselves compressed and lazily
	/// inflate on field access.  This is useful for large objects whose fields are
	/// not be altered during a map or reduce operation: leaving the field data
	/// compressed makes copying the instance from one file to another much
	/// faster.
	/// </remarks>
	public abstract class CompressedWritable : Writable
	{
		private byte[] compressed;

		public CompressedWritable()
		{
		}

		// if non-null, the compressed field data of this instance.
		/// <exception cref="System.IO.IOException"/>
		public void ReadFields(DataInput @in)
		{
			compressed = new byte[@in.ReadInt()];
			@in.ReadFully(compressed, 0, compressed.Length);
		}

		/// <summary>
		/// Must be called by all methods which access fields to ensure that the data
		/// has been uncompressed.
		/// </summary>
		protected internal virtual void EnsureInflated()
		{
			if (compressed != null)
			{
				try
				{
					ByteArrayInputStream deflated = new ByteArrayInputStream(compressed);
					DataInput inflater = new DataInputStream(new InflaterInputStream(deflated));
					ReadFieldsCompressed(inflater);
					compressed = null;
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}
		}

		/// <summary>
		/// Subclasses implement this instead of
		/// <see cref="ReadFields(System.IO.DataInput)"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void ReadFieldsCompressed(DataInput @in);

		/// <exception cref="System.IO.IOException"/>
		public void Write(DataOutput @out)
		{
			if (compressed == null)
			{
				ByteArrayOutputStream deflated = new ByteArrayOutputStream();
				Deflater deflater = new Deflater(Deflater.BestSpeed);
				DataOutputStream dout = new DataOutputStream(new DeflaterOutputStream(deflated, deflater
					));
				WriteCompressed(dout);
				dout.Close();
				deflater.Finish();
				compressed = deflated.ToByteArray();
			}
			@out.WriteInt(compressed.Length);
			@out.Write(compressed);
		}

		/// <summary>
		/// Subclasses implement this instead of
		/// <see cref="Write(System.IO.DataOutput)"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void WriteCompressed(DataOutput @out);
	}
}
