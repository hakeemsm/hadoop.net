using Com.Google.Common.Annotations;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>LOOKUP3 Request</summary>
	public class LOOKUP3Request : RequestWithHandle
	{
		private string name;

		public LOOKUP3Request(FileHandle handle, string name)
			: base(handle)
		{
			this.name = name;
		}

		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.LOOKUP3Request Deserialize(XDR xdr
			)
		{
			FileHandle handle = ReadHandle(xdr);
			string name = xdr.ReadString();
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.LOOKUP3Request(handle, name);
		}

		public virtual string GetName()
		{
			return this.name;
		}

		public virtual void SetName(string name)
		{
			this.name = name;
		}

		[VisibleForTesting]
		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
			xdr.WriteInt(Sharpen.Runtime.GetBytesForString(name, Charsets.Utf8).Length);
			xdr.WriteFixedOpaque(Sharpen.Runtime.GetBytesForString(name, Charsets.Utf8));
		}
	}
}
