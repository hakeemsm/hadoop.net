using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>Base class for a NFSv3 response.</summary>
	/// <remarks>
	/// Base class for a NFSv3 response. This class and its subclasses contain
	/// the response from NFSv3 handlers.
	/// </remarks>
	public class NFS3Response
	{
		protected internal int status;

		public NFS3Response(int status)
		{
			this.status = status;
		}

		public virtual int GetStatus()
		{
			return this.status;
		}

		public virtual void SetStatus(int status)
		{
			this.status = status;
		}

		/// <summary>
		/// Write the response, along with the rpc header (including verifier), to the
		/// XDR.
		/// </summary>
		public virtual XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			RpcAcceptedReply reply = RpcAcceptedReply.GetAcceptInstance(xid, verifier);
			reply.Write(@out);
			@out.WriteInt(this.GetStatus());
			return @out;
		}
	}
}
