using System.IO;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>CREATE3 Request</summary>
	public class CREATE3Request : RequestWithHandle
	{
		private readonly string name;

		private readonly int mode;

		private readonly SetAttr3 objAttr;

		private long verf = 0;

		public CREATE3Request(FileHandle handle, string name, int mode, SetAttr3 objAttr, 
			long verf)
			: base(handle)
		{
			this.name = name;
			this.mode = mode;
			this.objAttr = objAttr;
			this.verf = verf;
		}

		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.CREATE3Request Deserialize(XDR xdr
			)
		{
			FileHandle handle = ReadHandle(xdr);
			string name = xdr.ReadString();
			int mode = xdr.ReadInt();
			SetAttr3 objAttr = new SetAttr3();
			long verf = 0;
			if ((mode == Nfs3Constant.CreateUnchecked) || (mode == Nfs3Constant.CreateGuarded
				))
			{
				objAttr.Deserialize(xdr);
			}
			else
			{
				if (mode == Nfs3Constant.CreateExclusive)
				{
					verf = xdr.ReadHyper();
				}
				else
				{
					throw new IOException("Wrong create mode:" + mode);
				}
			}
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.CREATE3Request(handle, name, mode, 
				objAttr, verf);
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual int GetMode()
		{
			return mode;
		}

		public virtual SetAttr3 GetObjAttr()
		{
			return objAttr;
		}

		public virtual long GetVerf()
		{
			return verf;
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
			xdr.WriteInt(name.Length);
			xdr.WriteFixedOpaque(Sharpen.Runtime.GetBytesForString(name, Charsets.Utf8), name
				.Length);
			xdr.WriteInt(mode);
			objAttr.Serialize(xdr);
		}
	}
}
