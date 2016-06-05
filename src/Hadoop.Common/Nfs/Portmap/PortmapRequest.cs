using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Portmap
{
	/// <summary>Helper utility for building portmap request</summary>
	public class PortmapRequest
	{
		public static PortmapMapping Mapping(XDR xdr)
		{
			return PortmapMapping.Deserialize(xdr);
		}

		public static XDR Create(PortmapMapping mapping, bool set)
		{
			XDR request = new XDR();
			int procedure = set ? RpcProgramPortmap.PmapprocSet : RpcProgramPortmap.PmapprocUnset;
			RpcCall call = RpcCall.GetInstance(RpcUtil.GetNewXid(RpcProgramPortmap.Program.ToString
				()), RpcProgramPortmap.Program, RpcProgramPortmap.Version, procedure, new CredentialsNone
				(), new VerifierNone());
			call.Write(request);
			return mapping.Serialize(request);
		}
	}
}
