using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>GETATTR3 Request</summary>
	public class GETATTR3Request : RequestWithHandle
	{
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.GETATTR3Request Deserialize(XDR 
			xdr)
		{
			FileHandle handle = ReadHandle(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.GETATTR3Request(handle);
		}

		public GETATTR3Request(FileHandle handle)
			: base(handle)
		{
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
		}
	}
}
