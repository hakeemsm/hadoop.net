using Com.Google.Common.Base;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ReservationIdPBImpl : ReservationId
	{
		internal YarnProtos.ReservationIdProto proto = null;

		internal YarnProtos.ReservationIdProto.Builder builder = null;

		public ReservationIdPBImpl()
		{
			builder = YarnProtos.ReservationIdProto.NewBuilder();
		}

		public ReservationIdPBImpl(YarnProtos.ReservationIdProto proto)
		{
			this.proto = proto;
		}

		public virtual YarnProtos.ReservationIdProto GetProto()
		{
			return proto;
		}

		public override long GetId()
		{
			Preconditions.CheckNotNull(proto);
			return proto.GetId();
		}

		protected override void SetId(long id)
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
			proto = ((YarnProtos.ReservationIdProto)builder.Build());
			builder = null;
		}
	}
}
