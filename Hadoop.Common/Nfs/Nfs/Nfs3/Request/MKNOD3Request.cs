using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Nfs;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>MKNOD3 Request</summary>
	public class MKNOD3Request : RequestWithHandle
	{
		private readonly string name;

		private int type;

		private SetAttr3 objAttr = null;

		private Nfs3FileAttributes.Specdata3 spec = null;

		public MKNOD3Request(FileHandle handle, string name, int type, SetAttr3 objAttr, 
			Nfs3FileAttributes.Specdata3 spec)
			: base(handle)
		{
			this.name = name;
			this.type = type;
			this.objAttr = objAttr;
			this.spec = spec;
		}

		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.MKNOD3Request Deserialize(XDR xdr
			)
		{
			FileHandle handle = ReadHandle(xdr);
			string name = xdr.ReadString();
			int type = xdr.ReadInt();
			SetAttr3 objAttr = new SetAttr3();
			Nfs3FileAttributes.Specdata3 spec = null;
			if (type == NfsFileType.Nfschr.ToValue() || type == NfsFileType.Nfsblk.ToValue())
			{
				objAttr.Deserialize(xdr);
				spec = new Nfs3FileAttributes.Specdata3(xdr.ReadInt(), xdr.ReadInt());
			}
			else
			{
				if (type == NfsFileType.Nfssock.ToValue() || type == NfsFileType.Nfsfifo.ToValue(
					))
				{
					objAttr.Deserialize(xdr);
				}
			}
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.MKNOD3Request(handle, name, type, objAttr
				, spec);
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual int GetType()
		{
			return type;
		}

		public virtual SetAttr3 GetObjAttr()
		{
			return objAttr;
		}

		public virtual Nfs3FileAttributes.Specdata3 GetSpec()
		{
			return spec;
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
			xdr.WriteInt(name.Length);
			xdr.WriteFixedOpaque(Sharpen.Runtime.GetBytesForString(name, Charsets.Utf8), name
				.Length);
			objAttr.Serialize(xdr);
			if (spec != null)
			{
				xdr.WriteInt(spec.GetSpecdata1());
				xdr.WriteInt(spec.GetSpecdata2());
			}
		}
	}
}
