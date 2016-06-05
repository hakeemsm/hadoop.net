using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>READLINK3 Request</summary>
	public class READLINK3Request : RequestWithHandle
	{
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.READLINK3Request Deserialize(XDR
			 xdr)
		{
			FileHandle handle = ReadHandle(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.READLINK3Request(handle);
		}

		public READLINK3Request(FileHandle handle)
			: base(handle)
		{
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
		}
	}
}
