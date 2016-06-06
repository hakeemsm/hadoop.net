using System.IO;
using System.IO.Compression;

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
			    var deflated = new MemoryStream(_compressed);
			    BinaryReader inflater = new BinaryReader(new DeflateStream(deflated, CompressionMode.Decompress));
			    ReadFieldsCompressed(inflater);
			    _compressed = null;
			}
		}

		/// <summary>
		/// Subclasses implement this instead of
		/// <see cref="ReadFields(System.IO.BinaryReader)"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void ReadFieldsCompressed(BinaryReader reader);

		/// <exception cref="System.IO.IOException"/>
		public void Write(BinaryWriter writer)
		{
			if (_compressed == null)
			{
                
			    MemoryStream deflated = new MemoryStream();
			    var deflateStream = new DeflateStream(deflated, CompressionMode.Decompress);
			    
			    BinaryWriter dout = new BinaryWriter(deflateStream);
			    WriteCompressed(dout);
				dout.Close();

			    _compressed = deflated.ToArray();
			}
			writer.Write(_compressed.Length);
			writer.Write(_compressed);
		}

		/// <summary>
		/// Subclasses implement this instead of
		/// <see cref="Write(System.IO.BinaryWriter)"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void WriteCompressed(BinaryWriter writer);
	}
}
