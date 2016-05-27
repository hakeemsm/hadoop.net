using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Portmap
{
	/// <summary>
	/// Represents a mapping entry for in the Portmap service for binding RPC
	/// protocols.
	/// </summary>
	/// <remarks>
	/// Represents a mapping entry for in the Portmap service for binding RPC
	/// protocols. See RFC 1833 for details.
	/// This maps a program to a port number.
	/// </remarks>
	public class PortmapMapping
	{
		public const int TransportTcp = 6;

		public const int TransportUdp = 17;

		private readonly int program;

		private readonly int version;

		private readonly int transport;

		private readonly int port;

		public PortmapMapping(int program, int version, int transport, int port)
		{
			this.program = program;
			this.version = version;
			this.transport = transport;
			this.port = port;
		}

		public virtual XDR Serialize(XDR xdr)
		{
			xdr.WriteInt(program);
			xdr.WriteInt(version);
			xdr.WriteInt(transport);
			xdr.WriteInt(port);
			return xdr;
		}

		public static Org.Apache.Hadoop.Portmap.PortmapMapping Deserialize(XDR xdr)
		{
			return new Org.Apache.Hadoop.Portmap.PortmapMapping(xdr.ReadInt(), xdr.ReadInt(), 
				xdr.ReadInt(), xdr.ReadInt());
		}

		public virtual int GetPort()
		{
			return port;
		}

		public static string Key(Org.Apache.Hadoop.Portmap.PortmapMapping mapping)
		{
			return mapping.program + " " + mapping.version + " " + mapping.transport;
		}

		public override string ToString()
		{
			return string.Format("(PortmapMapping-%d:%d:%d:%d)", program, version, transport, 
				port);
		}
	}
}
