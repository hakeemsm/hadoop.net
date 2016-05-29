using Com.Google.Common.Base;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ApplicationIdPBImpl : ApplicationId
	{
		internal YarnProtos.ApplicationIdProto proto = null;

		internal YarnProtos.ApplicationIdProto.Builder builder = null;

		public ApplicationIdPBImpl()
		{
			builder = YarnProtos.ApplicationIdProto.NewBuilder();
		}

		public ApplicationIdPBImpl(YarnProtos.ApplicationIdProto proto)
		{
			this.proto = proto;
		}

		public virtual YarnProtos.ApplicationIdProto GetProto()
		{
			return proto;
		}

		public override int GetId()
		{
			Preconditions.CheckNotNull(proto);
			return proto.GetId();
		}

		protected override void SetId(int id)
		{
			Preconditions.CheckNotNull(builder);
			builder.SetId(id);
		}

		public override long GetClusterTimestamp()
		{
			Preconditions.CheckNotNull(proto);
			return proto.GetClusterTimestamp();
		}

		protected override void SetClusterTimestamp(long clusterTimestamp)
		{
			Preconditions.CheckNotNull(builder);
			builder.SetClusterTimestamp((clusterTimestamp));
		}

		protected override void Build()
		{
			proto = ((YarnProtos.ApplicationIdProto)builder.Build());
			builder = null;
		}
	}
}
