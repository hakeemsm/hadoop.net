using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>ACCESS3 Request</summary>
	public class ACCESS3Request : RequestWithHandle
	{
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.ACCESS3Request Deserialize(XDR xdr
			)
		{
			FileHandle handle = ReadHandle(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.ACCESS3Request(handle);
		}

		public ACCESS3Request(FileHandle handle)
			: base(handle)
		{
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
		}
	}
}
