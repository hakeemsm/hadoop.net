using System.IO;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>
	/// An NFS request that uses
	/// <see cref="Org.Apache.Hadoop.Nfs.Nfs3.FileHandle"/>
	/// to identify a file.
	/// </summary>
	public abstract class NFS3Request
	{
		/// <summary>Deserialize a handle from an XDR object</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static FileHandle ReadHandle(XDR xdr)
		{
			FileHandle handle = new FileHandle();
			if (!handle.Deserialize(xdr))
			{
				throw new IOException("can't deserialize file handle");
			}
			return handle;
		}

		/// <summary>Subclass should implement.</summary>
		/// <remarks>Subclass should implement. Usually handle is the first to be serialized</remarks>
		public abstract void Serialize(XDR xdr);
	}
}
