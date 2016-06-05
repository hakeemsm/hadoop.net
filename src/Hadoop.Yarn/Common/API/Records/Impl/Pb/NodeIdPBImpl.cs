using Com.Google.Common.Base;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class NodeIdPBImpl : NodeId
	{
		internal YarnProtos.NodeIdProto proto = null;

		internal YarnProtos.NodeIdProto.Builder builder = null;

		public NodeIdPBImpl()
		{
			builder = YarnProtos.NodeIdProto.NewBuilder();
		}

		public NodeIdPBImpl(YarnProtos.NodeIdProto proto)
		{
			this.proto = proto;
		}

		public virtual YarnProtos.NodeIdProto GetProto()
		{
			return proto;
		}

		public override string GetHost()
		{
			Preconditions.CheckNotNull(proto);
			return proto.GetHost();
		}

		protected override void SetHost(string host)
		{
			Preconditions.CheckNotNull(builder);
			builder.SetHost(host);
		}

		public override int GetPort()
		{
			Preconditions.CheckNotNull(proto);
			return proto.GetPort();
		}

		protected override void SetPort(int port)
		{
			Preconditions.CheckNotNull(builder);
			builder.SetPort(port);
		}

		protected override void Build()
		{
			proto = ((YarnProtos.NodeIdProto)builder.Build());
			builder = null;
		}
	}
}
