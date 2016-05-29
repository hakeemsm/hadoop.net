using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security
{
	/// <summary>TokenIdentifier for a container.</summary>
	/// <remarks>
	/// TokenIdentifier for a container. Encodes
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
	/// ,
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
	/// needed by the container and the target NMs host-address.
	/// </remarks>
	public class ContainerTokenIdentifier : TokenIdentifier
	{
		private static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Security.ContainerTokenIdentifier
			));

		public static readonly Text Kind = new Text("ContainerToken");

		private YarnSecurityTokenProtos.ContainerTokenIdentifierProto proto;

		public ContainerTokenIdentifier(ContainerId containerID, string hostName, string 
			appSubmitter, Resource r, long expiryTimeStamp, int masterKeyId, long rmIdentifier
			, Priority priority, long creationTime)
			: this(containerID, hostName, appSubmitter, r, expiryTimeStamp, masterKeyId, rmIdentifier
				, priority, creationTime, null)
		{
		}

		public ContainerTokenIdentifier(ContainerId containerID, string hostName, string 
			appSubmitter, Resource r, long expiryTimeStamp, int masterKeyId, long rmIdentifier
			, Priority priority, long creationTime, LogAggregationContext logAggregationContext
			)
		{
			YarnSecurityTokenProtos.ContainerTokenIdentifierProto.Builder builder = YarnSecurityTokenProtos.ContainerTokenIdentifierProto
				.NewBuilder();
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
			proto = ((YarnSecurityTokenProtos.ContainerTokenIdentifierProto)builder.Build());
		}

		/// <summary>Default constructor needed by RPC layer/SecretManager.</summary>
		public ContainerTokenIdentifier()
		{
		}

		public virtual ContainerId GetContainerID()
		{
			if (!proto.HasContainerId())
			{
				return null;
			}
			return new ContainerIdPBImpl(proto.GetContainerId());
		}

		public virtual string GetApplicationSubmitter()
		{
			return proto.GetAppSubmitter();
		}

		public virtual string GetNmHostAddress()
		{
			return proto.GetNmHostAddr();
		}

		public virtual Resource GetResource()
		{
			if (!proto.HasResource())
			{
				return null;
			}
			return new ResourcePBImpl(proto.GetResource());
		}

		public virtual long GetExpiryTimeStamp()
		{
			return proto.GetExpiryTimeStamp();
		}

		public virtual int GetMasterKeyId()
		{
			return proto.GetMasterKeyId();
		}

		public virtual Priority GetPriority()
		{
			if (!proto.HasPriority())
			{
				return null;
			}
			return new PriorityPBImpl(proto.GetPriority());
		}

		public virtual long GetCreationTime()
		{
			return proto.GetCreationTime();
		}

		/// <summary>Get the RMIdentifier of RM in which containers are allocated</summary>
		/// <returns>RMIdentifier</returns>
		public virtual long GetRMIdentifier()
		{
			return proto.GetRmIdentifier();
		}

		public virtual YarnSecurityTokenProtos.ContainerTokenIdentifierProto GetProto()
		{
			return proto;
		}

		public virtual LogAggregationContext GetLogAggregationContext()
		{
			if (!proto.HasLogAggregationContext())
			{
				return null;
			}
			return new LogAggregationContextPBImpl(proto.GetLogAggregationContext());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			Log.Debug("Writing ContainerTokenIdentifier to RPC layer: " + this);
			@out.Write(proto.ToByteArray());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			proto = YarnSecurityTokenProtos.ContainerTokenIdentifierProto.ParseFrom((DataInputStream
				)@in);
		}

		public override Text GetKind()
		{
			return Kind;
		}

		public override UserGroupInformation GetUser()
		{
			string containerId = null;
			if (proto.HasContainerId())
			{
				containerId = new ContainerIdPBImpl(proto.GetContainerId()).ToString();
			}
			return UserGroupInformation.CreateRemoteUser(containerId);
		}

		public class Renewer : Token.TrivialRenewer
		{
			// TODO: Needed?
			protected override Text GetKind()
			{
				return Kind;
			}
		}

		public override int GetHashCode()
		{
			return GetProto().GetHashCode();
		}

		public override bool Equals(object other)
		{
			if (other == null)
			{
				return false;
			}
			if (other.GetType().IsAssignableFrom(this.GetType()))
			{
				return this.GetProto().Equals(this.GetType().Cast(other).GetProto());
			}
			return false;
		}

		public override string ToString()
		{
			return TextFormat.ShortDebugString(GetProto());
		}
	}
}
