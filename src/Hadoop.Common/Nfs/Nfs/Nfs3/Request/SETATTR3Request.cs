using Org.Apache.Hadoop.Nfs;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>SETATTR3 Request</summary>
	public class SETATTR3Request : RequestWithHandle
	{
		private readonly SetAttr3 attr;

		private readonly bool check;

		private readonly NfsTime ctime;

		/* A client may request that the server check that the object is in an
		* expected state before performing the SETATTR operation. If guard.check is
		* TRUE, the server must compare the value of ctime to the current ctime of
		* the object. If the values are different, the server must preserve the
		* object attributes and must return a status of NFS3ERR_NOT_SYNC. If check is
		* FALSE, the server will not perform this check.
		*/
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.SETATTR3Request Deserialize(XDR 
			xdr)
		{
			FileHandle handle = ReadHandle(xdr);
			SetAttr3 attr = new SetAttr3();
			attr.Deserialize(xdr);
			bool check = xdr.ReadBoolean();
			NfsTime ctime;
			if (check)
			{
				ctime = NfsTime.Deserialize(xdr);
			}
			else
			{
				ctime = null;
			}
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.SETATTR3Request(handle, attr, check
				, ctime);
		}

		public SETATTR3Request(FileHandle handle, SetAttr3 attr, bool check, NfsTime ctime
			)
			: base(handle)
		{
			this.attr = attr;
			this.check = check;
			this.ctime = ctime;
		}

		public virtual SetAttr3 GetAttr()
		{
			return attr;
		}

		public virtual bool IsCheck()
		{
			return check;
		}

		public virtual NfsTime GetCtime()
		{
			return ctime;
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
			attr.Serialize(xdr);
			xdr.WriteBoolean(check);
			if (check)
			{
				ctime.Serialize(xdr);
			}
		}
	}
}
