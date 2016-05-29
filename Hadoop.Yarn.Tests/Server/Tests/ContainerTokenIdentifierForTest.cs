using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server
{
	public class ContainerTokenIdentifierForTest : ContainerTokenIdentifier
	{
		private static Log Log = LogFactory.GetLog(typeof(ContainerTokenIdentifier));

		public static readonly Text Kind = new Text("ContainerToken");

		private YarnSecurityTestTokenProtos.ContainerTokenIdentifierForTestProto proto;

		public ContainerTokenIdentifierForTest(ContainerId containerID, string hostName, 
			string appSubmitter, Resource r, long expiryTimeStamp, int masterKeyId, long rmIdentifier
			, Priority priority, long creationTime, LogAggregationContext logAggregationContext
			)
		{
			YarnSecurityTestTokenProtos.ContainerTokenIdentifierForTestProto.Builder builder = 
				YarnSecurityTestTokenProtos.ContainerTokenIdentifierForTestProto.NewBuilder();
			if (containerID != null)
			{
				builder.SetContainerId(((ContainerIdPBImpl)containerID).GetProto());
			}
			builder.SetNmHostAddr(hostName);
			builder.SetAppSubmitter(appSubmitter);
			if (r != null)
			{
				builder.SetResource(((ResourcePBImpl)r).GetProto());
			}
			builder.SetExpiryTimeStamp(expiryTimeStamp);
			builder.SetMasterKeyId(masterKeyId);
			builder.SetRmIdentifier(rmIdentifier);
			if (priority != null)
			{
				builder.SetPriority(((PriorityPBImpl)priority).GetProto());
			}
			builder.SetCreationTime(creationTime);
			if (logAggregationContext != null)
			{
				builder.SetLogAggregationContext(((LogAggregationContextPBImpl)logAggregationContext
					).GetProto());
			}
			proto = ((YarnSecurityTestTokenProtos.ContainerTokenIdentifierForTestProto)builder
				.Build());
		}

		public ContainerTokenIdentifierForTest(ContainerTokenIdentifier identifier, string
			 message)
		{
			YarnSecurityTestTokenProtos.ContainerTokenIdentifierForTestProto.Builder builder = 
				YarnSecurityTestTokenProtos.ContainerTokenIdentifierForTestProto.NewBuilder();
			ContainerIdPBImpl containerID = (ContainerIdPBImpl)identifier.GetContainerID();
			if (containerID != null)
			{
				builder.SetContainerId(containerID.GetProto());
			}
			builder.SetNmHostAddr(identifier.GetNmHostAddress());
			builder.SetAppSubmitter(identifier.GetApplicationSubmitter());
			ResourcePBImpl resource = (ResourcePBImpl)identifier.GetResource();
			if (resource != null)
			{
				builder.SetResource(resource.GetProto());
			}
			builder.SetExpiryTimeStamp(identifier.GetExpiryTimeStamp());
			builder.SetMasterKeyId(identifier.GetMasterKeyId());
			builder.SetRmIdentifier(identifier.GetRMIdentifier());
			PriorityPBImpl priority = (PriorityPBImpl)identifier.GetPriority();
			if (priority != null)
			{
				builder.SetPriority(priority.GetProto());
			}
			builder.SetCreationTime(identifier.GetCreationTime());
			builder.SetMessage(message);
			LogAggregationContextPBImpl logAggregationContext = (LogAggregationContextPBImpl)
				identifier.GetLogAggregationContext();
			if (logAggregationContext != null)
			{
				builder.SetLogAggregationContext(logAggregationContext.GetProto());
			}
			proto = ((YarnSecurityTestTokenProtos.ContainerTokenIdentifierForTestProto)builder
				.Build());
		}

		public override ContainerId GetContainerID()
		{
			return new ContainerIdPBImpl(proto.GetContainerId());
		}

		public override string GetApplicationSubmitter()
		{
			return proto.GetAppSubmitter();
		}

		public override string GetNmHostAddress()
		{
			return proto.GetNmHostAddr();
		}

		public override Resource GetResource()
		{
			return new ResourcePBImpl(proto.GetResource());
		}

		public override long GetExpiryTimeStamp()
		{
			return proto.GetExpiryTimeStamp();
		}

		public override int GetMasterKeyId()
		{
			return proto.GetMasterKeyId();
		}

		public override Priority GetPriority()
		{
			return new PriorityPBImpl(proto.GetPriority());
		}

		public override long GetCreationTime()
		{
			return proto.GetCreationTime();
		}

		/// <summary>Get the RMIdentifier of RM in which containers are allocated</summary>
		/// <returns>RMIdentifier</returns>
		public override long GetRMIdentifier()
		{
			return proto.GetRmIdentifier();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			DataInputStream dis = (DataInputStream)@in;
			byte[] buffer = IOUtils.ToByteArray(dis);
			proto = YarnSecurityTestTokenProtos.ContainerTokenIdentifierForTestProto.ParseFrom
				(buffer);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			Log.Debug("Writing ContainerTokenIdentifierForTest to RPC layer: " + this);
			@out.Write(proto.ToByteArray());
		}

		internal virtual YarnSecurityTestTokenProtos.ContainerTokenIdentifierForTestProto
			 GetNewProto()
		{
			return this.proto;
		}

		public override int GetHashCode()
		{
			return this.proto.GetHashCode();
		}

		public override bool Equals(object other)
		{
			if (other == null)
			{
				return false;
			}
			if (other.GetType().IsAssignableFrom(this.GetType()))
			{
				return this.GetNewProto().Equals(this.GetType().Cast(other).GetNewProto());
			}
			return false;
		}

		public override string ToString()
		{
			return TextFormat.ShortDebugString(this.proto);
		}
	}
}
