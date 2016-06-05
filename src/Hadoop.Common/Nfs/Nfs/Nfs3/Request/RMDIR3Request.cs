using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>RMDIR3 Request</summary>
	public class RMDIR3Request : RequestWithHandle
	{
		private readonly string name;

		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.RMDIR3Request Deserialize(XDR xdr
			)
		{
			FileHandle handle = ReadHandle(xdr);
			string name = xdr.ReadString();
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.RMDIR3Request(handle, name);
		}

		public RMDIR3Request(FileHandle handle, string name)
			: base(handle)
		{
			this.name = name;
		}

		public virtual string GetName()
		{
			return this.name;
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
			xdr.WriteInt(Runtime.GetBytesForString(name, Charsets.Utf8).Length);
			xdr.WriteFixedOpaque(Runtime.GetBytesForString(name, Charsets.Utf8));
		}
	}
}
