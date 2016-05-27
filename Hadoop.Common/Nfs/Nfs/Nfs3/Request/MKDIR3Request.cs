using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>MKDIR3 Request</summary>
	public class MKDIR3Request : RequestWithHandle
	{
		private readonly string name;

		private readonly SetAttr3 objAttr;

		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.MKDIR3Request Deserialize(XDR xdr
			)
		{
			FileHandle handle = ReadHandle(xdr);
			string name = xdr.ReadString();
			SetAttr3 objAttr = new SetAttr3();
			objAttr.Deserialize(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.MKDIR3Request(handle, name, objAttr
				);
		}

		public MKDIR3Request(FileHandle handle, string name, SetAttr3 objAttr)
			: base(handle)
		{
			this.name = name;
			this.objAttr = objAttr;
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual SetAttr3 GetObjAttr()
		{
			return objAttr;
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
			xdr.WriteInt(Sharpen.Runtime.GetBytesForString(name, Charsets.Utf8).Length);
			xdr.WriteFixedOpaque(Sharpen.Runtime.GetBytesForString(name, Charsets.Utf8));
			objAttr.Serialize(xdr);
		}
	}
}
