using Sharpen;

namespace org.apache.hadoop.io
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
	public abstract class CompressedWritable : org.apache.hadoop.io.Writable
	{
		private byte[] compressed;

		public CompressedWritable()
		{
		}

		// if non-null, the compressed field data of this instance.
		/// <exception cref="System.IO.IOException"/>
		public void readFields(java.io.DataInput @in)
		{
			compressed = new byte[@in.readInt()];
			@in.readFully(compressed, 0, compressed.Length);
		}

		/// <summary>
		/// Must be called by all methods which access fields to ensure that the data
		/// has been uncompressed.
		/// </summary>
		protected internal virtual void ensureInflated()
		{
			if (compressed != null)
			{
				try
				{
					java.io.ByteArrayInputStream deflated = new java.io.ByteArrayInputStream(compressed
						);
					java.io.DataInput inflater = new java.io.DataInputStream(new java.util.zip.InflaterInputStream
						(deflated));
					readFieldsCompressed(inflater);
					compressed = null;
				}
				catch (System.IO.IOException e)
				{
					throw new System.Exception(e);
				}
			}
		}

		/// <summary>
		/// Subclasses implement this instead of
		/// <see cref="readFields(java.io.DataInput)"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void readFieldsCompressed(java.io.DataInput @in);

		/// <exception cref="System.IO.IOException"/>
		public void write(java.io.DataOutput @out)
		{
			if (compressed == null)
			{
				java.io.ByteArrayOutputStream deflated = new java.io.ByteArrayOutputStream();
				java.util.zip.Deflater deflater = new java.util.zip.Deflater(java.util.zip.Deflater
					.BEST_SPEED);
				java.io.DataOutputStream dout = new java.io.DataOutputStream(new java.util.zip.DeflaterOutputStream
					(deflated, deflater));
				writeCompressed(dout);
				dout.close();
				deflater.end();
				compressed = deflated.toByteArray();
			}
			@out.writeInt(compressed.Length);
			@out.write(compressed);
		}

		/// <summary>
		/// Subclasses implement this instead of
		/// <see cref="write(java.io.DataOutput)"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void writeCompressed(java.io.DataOutput @out);
	}
}
