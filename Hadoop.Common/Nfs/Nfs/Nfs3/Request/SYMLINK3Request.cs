using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>SYMLINK3 Request</summary>
	public class SYMLINK3Request : RequestWithHandle
	{
		private readonly string name;

		private readonly SetAttr3 symAttr;

		private readonly string symData;

		// The name of the link
		// It contains the target
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.SYMLINK3Request Deserialize(XDR 
			xdr)
		{
			FileHandle handle = ReadHandle(xdr);
			string name = xdr.ReadString();
			SetAttr3 symAttr = new SetAttr3();
			symAttr.Deserialize(xdr);
			string symData = xdr.ReadString();
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.SYMLINK3Request(handle, name, symAttr
				, symData);
		}

		public SYMLINK3Request(FileHandle handle, string name, SetAttr3 symAttr, string symData
			)
			: base(handle)
		{
			this.name = name;
			this.symAttr = symAttr;
			this.symData = symData;
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual SetAttr3 GetSymAttr()
		{
			return symAttr;
		}

		public virtual string GetSymData()
		{
			return symData;
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
			xdr.WriteInt(Runtime.GetBytesForString(name, Charsets.Utf8).Length);
			xdr.WriteFixedOpaque(Runtime.GetBytesForString(name, Charsets.Utf8));
			symAttr.Serialize(xdr);
			xdr.WriteInt(Runtime.GetBytesForString(symData, Charsets.Utf8).Length);
			xdr.WriteFixedOpaque(Runtime.GetBytesForString(symData, Charsets.Utf8));
		}
	}
}
