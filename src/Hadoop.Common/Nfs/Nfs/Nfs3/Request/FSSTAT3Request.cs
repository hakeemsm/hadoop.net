using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>FSSTAT3 Request</summary>
	public class FSSTAT3Request : RequestWithHandle
	{
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.FSSTAT3Request Deserialize(XDR xdr
			)
		{
			FileHandle handle = ReadHandle(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.FSSTAT3Request(handle);
		}

		public FSSTAT3Request(FileHandle handle)
			: base(handle)
		{
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
		}
	}
}
