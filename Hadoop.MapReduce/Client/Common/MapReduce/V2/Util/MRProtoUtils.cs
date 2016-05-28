using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Util
{
	public class MRProtoUtils
	{
		private static string JobStatePrefix = "J_";

		/*
		* JobState
		*/
		public static MRProtos.JobStateProto ConvertToProtoFormat(JobState e)
		{
			return MRProtos.JobStateProto.ValueOf(JobStatePrefix + e.ToString());
		}

		public static JobState ConvertFromProtoFormat(MRProtos.JobStateProto e)
		{
			return JobState.ValueOf(e.ToString().Replace(JobStatePrefix, string.Empty));
		}

		private static string PhasePrefix = "P_";

		/*
		* Phase
		*/
		public static MRProtos.PhaseProto ConvertToProtoFormat(Phase e)
		{
			return MRProtos.PhaseProto.ValueOf(PhasePrefix + e.ToString());
		}

		public static Phase ConvertFromProtoFormat(MRProtos.PhaseProto e)
		{
			return Phase.ValueOf(e.ToString().Replace(PhasePrefix, string.Empty));
		}

		private static string TacePrefix = "TACE_";

		/*
		* TaskAttemptCompletionEventStatus
		*/
		public static MRProtos.TaskAttemptCompletionEventStatusProto ConvertToProtoFormat
			(TaskAttemptCompletionEventStatus e)
		{
			return MRProtos.TaskAttemptCompletionEventStatusProto.ValueOf(TacePrefix + e.ToString
				());
		}

		public static TaskAttemptCompletionEventStatus ConvertFromProtoFormat(MRProtos.TaskAttemptCompletionEventStatusProto
			 e)
		{
			return TaskAttemptCompletionEventStatus.ValueOf(e.ToString().Replace(TacePrefix, 
				string.Empty));
		}

		private static string TaskAttemptStatePrefix = "TA_";

		/*
		* TaskAttemptState
		*/
		public static MRProtos.TaskAttemptStateProto ConvertToProtoFormat(TaskAttemptState
			 e)
		{
			return MRProtos.TaskAttemptStateProto.ValueOf(TaskAttemptStatePrefix + e.ToString
				());
		}

		public static TaskAttemptState ConvertFromProtoFormat(MRProtos.TaskAttemptStateProto
			 e)
		{
			return TaskAttemptState.ValueOf(e.ToString().Replace(TaskAttemptStatePrefix, string.Empty
				));
		}

		private static string TaskStatePrefix = "TS_";

		/*
		* TaskState
		*/
		public static MRProtos.TaskStateProto ConvertToProtoFormat(TaskState e)
		{
			return MRProtos.TaskStateProto.ValueOf(TaskStatePrefix + e.ToString());
		}

		public static TaskState ConvertFromProtoFormat(MRProtos.TaskStateProto e)
		{
			return TaskState.ValueOf(e.ToString().Replace(TaskStatePrefix, string.Empty));
		}

		/*
		* TaskType
		*/
		public static MRProtos.TaskTypeProto ConvertToProtoFormat(TaskType e)
		{
			return MRProtos.TaskTypeProto.ValueOf(e.ToString());
		}

		public static TaskType ConvertFromProtoFormat(MRProtos.TaskTypeProto e)
		{
			return TaskType.ValueOf(e.ToString());
		}
	}
}
