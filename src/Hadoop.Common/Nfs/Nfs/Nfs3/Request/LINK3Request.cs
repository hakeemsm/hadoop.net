using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>LINK3 Request</summary>
	public class LINK3Request : RequestWithHandle
	{
		private readonly FileHandle fromDirHandle;

		private readonly string fromName;

		public LINK3Request(FileHandle handle, FileHandle fromDirHandle, string fromName)
			: base(handle)
		{
			this.fromDirHandle = fromDirHandle;
			this.fromName = fromName;
		}

		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.LINK3Request Deserialize(XDR xdr
			)
		{
			FileHandle handle = ReadHandle(xdr);
			FileHandle fromDirHandle = ReadHandle(xdr);
			string fromName = xdr.ReadString();
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.LINK3Request(handle, fromDirHandle, 
				fromName);
		}

		public virtual FileHandle GetFromDirHandle()
		{
			return fromDirHandle;
		}

		public virtual string GetFromName()
		{
			return fromName;
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
			fromDirHandle.Serialize(xdr);
			xdr.WriteInt(fromName.Length);
			xdr.WriteFixedOpaque(Runtime.GetBytesForString(fromName, Charsets.Utf8), 
				fromName.Length);
		}
	}
}
