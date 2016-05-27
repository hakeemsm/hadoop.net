using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>RENAME3 Request</summary>
	public class RENAME3Request : NFS3Request
	{
		private readonly FileHandle fromDirHandle;

		private readonly string fromName;

		private readonly FileHandle toDirHandle;

		private readonly string toName;

		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.RENAME3Request Deserialize(XDR xdr
			)
		{
			FileHandle fromDirHandle = ReadHandle(xdr);
			string fromName = xdr.ReadString();
			FileHandle toDirHandle = ReadHandle(xdr);
			string toName = xdr.ReadString();
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.RENAME3Request(fromDirHandle, fromName
				, toDirHandle, toName);
		}

		public RENAME3Request(FileHandle fromDirHandle, string fromName, FileHandle toDirHandle
			, string toName)
		{
			this.fromDirHandle = fromDirHandle;
			this.fromName = fromName;
			this.toDirHandle = toDirHandle;
			this.toName = toName;
		}

		public virtual FileHandle GetFromDirHandle()
		{
			return fromDirHandle;
		}

		public virtual string GetFromName()
		{
			return fromName;
		}

		public virtual FileHandle GetToDirHandle()
		{
			return toDirHandle;
		}

		public virtual string GetToName()
		{
			return toName;
		}

		public override void Serialize(XDR xdr)
		{
			fromDirHandle.Serialize(xdr);
			xdr.WriteInt(Sharpen.Runtime.GetBytesForString(fromName, Charsets.Utf8).Length);
			xdr.WriteFixedOpaque(Sharpen.Runtime.GetBytesForString(fromName, Charsets.Utf8));
			toDirHandle.Serialize(xdr);
			xdr.WriteInt(Sharpen.Runtime.GetBytesForString(toName, Charsets.Utf8).Length);
			xdr.WriteFixedOpaque(Sharpen.Runtime.GetBytesForString(toName, Charsets.Utf8));
		}
	}
}
