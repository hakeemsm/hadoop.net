using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>WccData saved information used by client for weak cache consistency</summary>
	public class WccData
	{
		private WccAttr preOpAttr;

		private Nfs3FileAttributes postOpAttr;

		public virtual WccAttr GetPreOpAttr()
		{
			return preOpAttr;
		}

		public virtual void SetPreOpAttr(WccAttr preOpAttr)
		{
			this.preOpAttr = preOpAttr;
		}

		public virtual Nfs3FileAttributes GetPostOpAttr()
		{
			return postOpAttr;
		}

		public virtual void SetPostOpAttr(Nfs3FileAttributes postOpAttr)
		{
			this.postOpAttr = postOpAttr;
		}

		public WccData(WccAttr preOpAttr, Nfs3FileAttributes postOpAttr)
		{
			this.preOpAttr = (preOpAttr == null) ? new WccAttr() : preOpAttr;
			this.postOpAttr = (postOpAttr == null) ? new Nfs3FileAttributes() : postOpAttr;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.WccData Deserialize(XDR xdr)
		{
			xdr.ReadBoolean();
			WccAttr preOpAttr = WccAttr.Deserialize(xdr);
			xdr.ReadBoolean();
			Nfs3FileAttributes postOpAttr = Nfs3FileAttributes.Deserialize(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.WccData(preOpAttr, postOpAttr);
		}

		public virtual void Serialize(XDR @out)
		{
			@out.WriteBoolean(true);
			// attributes follow
			preOpAttr.Serialize(@out);
			@out.WriteBoolean(true);
			// attributes follow
			postOpAttr.Serialize(@out);
		}
	}
}
