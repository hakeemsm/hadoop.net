using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords
{
	public interface GetJobReportResponse
	{
		JobReport GetJobReport();

		void SetJobReport(JobReport jobReport);
	}
}
