using System.IO;

namespace Hadoop.Common.Core.IO
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
	public abstract class CompressedWritable : IWritable
	{
		private byte[] _compressed;

	    // if non-null, the compressed field data of this instance.
		/// <exception cref="System.IO.IOException"/>
		public void ReadFields(BinaryReader reader)
		{
			_compressed = new byte[reader.Read()];
			_compressed = reader.ReadBytes(_compressed.Length);
		}

		/// <summary>
		/// Must be called by all methods which access fields to ensure that the data
		/// has been uncompressed.
		/// </summary>
		protected internal virtual void EnsureInflated()
		{
			if (_compressed != null)
			{
				try
				{
					ByteArrayInputStream deflated = new ByteArrayInputStream(_compressed);
					BinaryReader inflater = new DataInputStream(new InflaterInputStream(deflated));
					ReadFieldsCompressed(inflater);
					_compressed = null;
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}
		}

		/// <summary>
		/// Subclasses implement this instead of
		/// <see cref="ReadFields(System.IO.BinaryReader)"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void ReadFieldsCompressed(BinaryReader @in);

		/// <exception cref="System.IO.IOException"/>
		public void Write(BinaryWriter @out)
		{
			if (_compressed == null)
			{
				ByteArrayOutputStream deflated = new ByteArrayOutputStream();
				Deflater deflater = new Deflater(Deflater.BestSpeed);
				DataOutputStream dout = new DataOutputStream(new DeflaterOutputStream(deflated, deflater
					));
				WriteCompressed(dout);
				dout.Close();
				deflater.Finish();
				_compressed = deflated.ToByteArray();
			}
			@out.WriteInt(_compressed.Length);
			@out.Write(_compressed);
		}

		/// <summary>
		/// Subclasses implement this instead of
		/// <see cref="Write(System.IO.BinaryWriter)"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void WriteCompressed(BinaryWriter @out);
	}
}
