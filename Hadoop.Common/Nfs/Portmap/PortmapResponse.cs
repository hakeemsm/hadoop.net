using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Portmap
{
	/// <summary>Helper utility for sending portmap response.</summary>
	public class PortmapResponse
	{
		public static XDR VoidReply(XDR xdr, int xid)
		{
			RpcAcceptedReply.GetAcceptInstance(xid, new VerifierNone()).Write(xdr);
			return xdr;
		}

		public static XDR IntReply(XDR xdr, int xid, int value)
		{
			RpcAcceptedReply.GetAcceptInstance(xid, new VerifierNone()).Write(xdr);
			xdr.WriteInt(value);
			return xdr;
		}

		public static XDR BooleanReply(XDR xdr, int xid, bool value)
		{
			RpcAcceptedReply.GetAcceptInstance(xid, new VerifierNone()).Write(xdr);
			xdr.WriteBoolean(value);
			return xdr;
		}

		public static XDR PmapList(XDR xdr, int xid, PortmapMapping[] list)
		{
			RpcAcceptedReply.GetAcceptInstance(xid, new VerifierNone()).Write(xdr);
			foreach (PortmapMapping mapping in list)
			{
				xdr.WriteBoolean(true);
				// Value follows
				mapping.Serialize(xdr);
			}
			xdr.WriteBoolean(false);
			// No value follows
			return xdr;
		}
	}
}
