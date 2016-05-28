using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords
{
	public interface GetTaskReportsResponse
	{
		IList<TaskReport> GetTaskReportList();

		TaskReport GetTaskReport(int index);

		int GetTaskReportCount();

		void AddAllTaskReports(IList<TaskReport> taskReports);

		void AddTaskReport(TaskReport taskReport);

		void RemoveTaskReport(int index);

		void ClearTaskReports();
	}
}
