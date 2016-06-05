using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ProtoUtils
	{
		private static string ContainerStatePrefix = "C_";

		/*
		* ContainerState
		*/
		public static YarnProtos.ContainerStateProto ConvertToProtoFormat(ContainerState 
			e)
		{
			return YarnProtos.ContainerStateProto.ValueOf(ContainerStatePrefix + e.ToString()
				);
		}

		public static ContainerState ConvertFromProtoFormat(YarnProtos.ContainerStateProto
			 e)
		{
			return ContainerState.ValueOf(e.ToString().Replace(ContainerStatePrefix, string.Empty
				));
		}

		private static string NodeStatePrefix = "NS_";

		/*
		* NodeState
		*/
		public static YarnProtos.NodeStateProto ConvertToProtoFormat(NodeState e)
		{
			return YarnProtos.NodeStateProto.ValueOf(NodeStatePrefix + e.ToString());
		}

		public static NodeState ConvertFromProtoFormat(YarnProtos.NodeStateProto e)
		{
			return NodeState.ValueOf(e.ToString().Replace(NodeStatePrefix, string.Empty));
		}

		/*
		* NodeId
		*/
		public static YarnProtos.NodeIdProto ConvertToProtoFormat(NodeId e)
		{
			return ((NodeIdPBImpl)e).GetProto();
		}

		public static NodeId ConvertFromProtoFormat(YarnProtos.NodeIdProto e)
		{
			return new NodeIdPBImpl(e);
		}

		/*
		* YarnApplicationState
		*/
		public static YarnProtos.YarnApplicationStateProto ConvertToProtoFormat(YarnApplicationState
			 e)
		{
			return YarnProtos.YarnApplicationStateProto.ValueOf(e.ToString());
		}

		public static YarnApplicationState ConvertFromProtoFormat(YarnProtos.YarnApplicationStateProto
			 e)
		{
			return YarnApplicationState.ValueOf(e.ToString());
		}

		private static string YarnApplicationAttemptStatePrefix = "APP_ATTEMPT_";

		/*
		* YarnApplicationAttemptState
		*/
		public static YarnProtos.YarnApplicationAttemptStateProto ConvertToProtoFormat(YarnApplicationAttemptState
			 e)
		{
			return YarnProtos.YarnApplicationAttemptStateProto.ValueOf(YarnApplicationAttemptStatePrefix
				 + e.ToString());
		}

		public static YarnApplicationAttemptState ConvertFromProtoFormat(YarnProtos.YarnApplicationAttemptStateProto
			 e)
		{
			return YarnApplicationAttemptState.ValueOf(e.ToString().Replace(YarnApplicationAttemptStatePrefix
				, string.Empty));
		}

		/*
		* ApplicationsRequestScope
		*/
		public static YarnServiceProtos.ApplicationsRequestScopeProto ConvertToProtoFormat
			(ApplicationsRequestScope e)
		{
			return YarnServiceProtos.ApplicationsRequestScopeProto.ValueOf(e.ToString());
		}

		public static ApplicationsRequestScope ConvertFromProtoFormat(YarnServiceProtos.ApplicationsRequestScopeProto
			 e)
		{
			return ApplicationsRequestScope.ValueOf(e.ToString());
		}

		/*
		* ApplicationResourceUsageReport
		*/
		public static YarnProtos.ApplicationResourceUsageReportProto ConvertToProtoFormat
			(ApplicationResourceUsageReport e)
		{
			return ((ApplicationResourceUsageReportPBImpl)e).GetProto();
		}

		public static ApplicationResourceUsageReport ConvertFromProtoFormat(YarnProtos.ApplicationResourceUsageReportProto
			 e)
		{
			return new ApplicationResourceUsageReportPBImpl(e);
		}

		private static string FinalApplicationStatusPrefix = "APP_";

		/*
		* FinalApplicationStatus
		*/
		public static YarnProtos.FinalApplicationStatusProto ConvertToProtoFormat(FinalApplicationStatus
			 e)
		{
			return YarnProtos.FinalApplicationStatusProto.ValueOf(FinalApplicationStatusPrefix
				 + e.ToString());
		}

		public static FinalApplicationStatus ConvertFromProtoFormat(YarnProtos.FinalApplicationStatusProto
			 e)
		{
			return FinalApplicationStatus.ValueOf(e.ToString().Replace(FinalApplicationStatusPrefix
				, string.Empty));
		}

		/*
		* LocalResourceType
		*/
		public static YarnProtos.LocalResourceTypeProto ConvertToProtoFormat(LocalResourceType
			 e)
		{
			return YarnProtos.LocalResourceTypeProto.ValueOf(e.ToString());
		}

		public static LocalResourceType ConvertFromProtoFormat(YarnProtos.LocalResourceTypeProto
			 e)
		{
			return LocalResourceType.ValueOf(e.ToString());
		}

		/*
		* LocalResourceVisibility
		*/
		public static YarnProtos.LocalResourceVisibilityProto ConvertToProtoFormat(LocalResourceVisibility
			 e)
		{
			return YarnProtos.LocalResourceVisibilityProto.ValueOf(e.ToString());
		}

		public static LocalResourceVisibility ConvertFromProtoFormat(YarnProtos.LocalResourceVisibilityProto
			 e)
		{
			return LocalResourceVisibility.ValueOf(e.ToString());
		}

		/*
		* AMCommand
		*/
		public static YarnProtos.AMCommandProto ConvertToProtoFormat(AMCommand e)
		{
			return YarnProtos.AMCommandProto.ValueOf(e.ToString());
		}

		public static AMCommand ConvertFromProtoFormat(YarnProtos.AMCommandProto e)
		{
			return AMCommand.ValueOf(e.ToString());
		}

		/*
		* ByteBuffer
		*/
		public static ByteBuffer ConvertFromProtoFormat(ByteString byteString)
		{
			int capacity = byteString.AsReadOnlyByteBuffer().Rewind().Remaining();
			byte[] b = new byte[capacity];
			byteString.AsReadOnlyByteBuffer().Get(b, 0, capacity);
			return ByteBuffer.Wrap(b);
		}

		public static ByteString ConvertToProtoFormat(ByteBuffer byteBuffer)
		{
			//    return ByteString.copyFrom((ByteBuffer)byteBuffer.duplicate().rewind());
			int oldPos = byteBuffer.Position();
			byteBuffer.Rewind();
			ByteString bs = ByteString.CopyFrom(byteBuffer);
			byteBuffer.Position(oldPos);
			return bs;
		}

		private static string QueueStatePrefix = "Q_";

		/*
		* QueueState
		*/
		public static YarnProtos.QueueStateProto ConvertToProtoFormat(QueueState e)
		{
			return YarnProtos.QueueStateProto.ValueOf(QueueStatePrefix + e.ToString());
		}

		public static QueueState ConvertFromProtoFormat(YarnProtos.QueueStateProto e)
		{
			return QueueState.ValueOf(e.ToString().Replace(QueueStatePrefix, string.Empty));
		}

		private static string QueueAclPrefix = "QACL_";

		/*
		* QueueACL
		*/
		public static YarnProtos.QueueACLProto ConvertToProtoFormat(QueueACL e)
		{
			return YarnProtos.QueueACLProto.ValueOf(QueueAclPrefix + e.ToString());
		}

		public static QueueACL ConvertFromProtoFormat(YarnProtos.QueueACLProto e)
		{
			return QueueACL.ValueOf(e.ToString().Replace(QueueAclPrefix, string.Empty));
		}

		private static string AppAccessTypePrefix = "APPACCESS_";

		/*
		* ApplicationAccessType
		*/
		public static YarnProtos.ApplicationAccessTypeProto ConvertToProtoFormat(ApplicationAccessType
			 e)
		{
			return YarnProtos.ApplicationAccessTypeProto.ValueOf(AppAccessTypePrefix + e.ToString
				());
		}

		public static ApplicationAccessType ConvertFromProtoFormat(YarnProtos.ApplicationAccessTypeProto
			 e)
		{
			return ApplicationAccessType.ValueOf(e.ToString().Replace(AppAccessTypePrefix, string.Empty
				));
		}

		/*
		* Reservation Request interpreter type
		*/
		public static YarnProtos.ReservationRequestInterpreterProto ConvertToProtoFormat(
			ReservationRequestInterpreter e)
		{
			return YarnProtos.ReservationRequestInterpreterProto.ValueOf(e.ToString());
		}

		public static ReservationRequestInterpreter ConvertFromProtoFormat(YarnProtos.ReservationRequestInterpreterProto
			 e)
		{
			return ReservationRequestInterpreter.ValueOf(e.ToString());
		}
	}
}
