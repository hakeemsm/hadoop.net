using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	public interface ApplicationHistoryReader
	{
		/// <summary>
		/// This method returns Application
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationHistoryData
		/// 	"/>
		/// for the
		/// specified
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// .
		/// </summary>
		/// <param name="appId"/>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationHistoryData
		/// 	"/>
		/// for the ApplicationId.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		ApplicationHistoryData GetApplication(ApplicationId appId);

		/// <summary>
		/// This method returns all Application
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationHistoryData
		/// 	"/>
		/// s
		/// </summary>
		/// <returns>
		/// map of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// to
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationHistoryData
		/// 	"/>
		/// s.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		IDictionary<ApplicationId, ApplicationHistoryData> GetAllApplications();

		/// <summary>
		/// Application can have multiple application attempts
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationAttemptHistoryData
		/// 	"/>
		/// . This method returns the all
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationAttemptHistoryData
		/// 	"/>
		/// s for the Application.
		/// </summary>
		/// <param name="appId"/>
		/// <returns>
		/// all
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationAttemptHistoryData
		/// 	"/>
		/// s for the Application.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		IDictionary<ApplicationAttemptId, ApplicationAttemptHistoryData> GetApplicationAttempts
			(ApplicationId appId);

		/// <summary>
		/// This method returns
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationAttemptHistoryData
		/// 	"/>
		/// for specified
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// .
		/// </summary>
		/// <param name="appAttemptId"><see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId
		/// 	"/></param>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationAttemptHistoryData
		/// 	"/>
		/// for ApplicationAttemptId
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		ApplicationAttemptHistoryData GetApplicationAttempt(ApplicationAttemptId appAttemptId
			);

		/// <summary>
		/// This method returns
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ContainerHistoryData
		/// 	"/>
		/// for specified
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// .
		/// </summary>
		/// <param name="containerId"><see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId
		/// 	"/></param>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ContainerHistoryData
		/// 	"/>
		/// for ContainerId
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		ContainerHistoryData GetContainer(ContainerId containerId);

		/// <summary>
		/// This method returns
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ContainerHistoryData
		/// 	"/>
		/// for specified
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId"/>
		/// .
		/// </summary>
		/// <param name="appAttemptId"><see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId
		/// 	"/></param>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ContainerHistoryData
		/// 	"/>
		/// for ApplicationAttemptId
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		ContainerHistoryData GetAMContainer(ApplicationAttemptId appAttemptId);

		/// <summary>
		/// This method returns Map
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// to
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ContainerHistoryData
		/// 	"/>
		/// for specified
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId"/>
		/// .
		/// </summary>
		/// <param name="appAttemptId"><see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId
		/// 	"/></param>
		/// <returns>
		/// Map
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// to
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ContainerHistoryData
		/// 	"/>
		/// for
		/// ApplicationAttemptId
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		IDictionary<ContainerId, ContainerHistoryData> GetContainers(ApplicationAttemptId
			 appAttemptId);
	}
}
