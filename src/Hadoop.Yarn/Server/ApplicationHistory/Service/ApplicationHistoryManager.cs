using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	public interface ApplicationHistoryManager
	{
		/// <summary>
		/// This method returns Application
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationReport"/>
		/// for the specified
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// .
		/// </summary>
		/// <param name="appId"/>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationReport"/>
		/// for the ApplicationId.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		ApplicationReport GetApplication(ApplicationId appId);

		/// <summary>
		/// This method returns the given number of Application
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationReport"/>
		/// s.
		/// </summary>
		/// <param name="appsNum"/>
		/// <returns>
		/// map of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// to
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationReport"/>
		/// s.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		IDictionary<ApplicationId, ApplicationReport> GetApplications(long appsNum);

		/// <summary>
		/// Application can have multiple application attempts
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
		/// . This method returns the all
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
		/// s for the Application.
		/// </summary>
		/// <param name="appId"/>
		/// <returns>
		/// all
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
		/// s for the Application.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		IDictionary<ApplicationAttemptId, ApplicationAttemptReport> GetApplicationAttempts
			(ApplicationId appId);

		/// <summary>
		/// This method returns
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
		/// for specified
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// .
		/// </summary>
		/// <param name="appAttemptId"><see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId
		/// 	"/></param>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
		/// for ApplicationAttemptId
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		ApplicationAttemptReport GetApplicationAttempt(ApplicationAttemptId appAttemptId);

		/// <summary>
		/// This method returns
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerReport"/>
		/// for specified
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// .
		/// </summary>
		/// <param name="containerId"><see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId
		/// 	"/></param>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerReport"/>
		/// for ContainerId
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		ContainerReport GetContainer(ContainerId containerId);

		/// <summary>
		/// This method returns
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerReport"/>
		/// for specified
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId"/>
		/// .
		/// </summary>
		/// <param name="appAttemptId"><see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId
		/// 	"/></param>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerReport"/>
		/// for ApplicationAttemptId
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		ContainerReport GetAMContainer(ApplicationAttemptId appAttemptId);

		/// <summary>
		/// This method returns Map of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// to
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerReport"/>
		/// for specified
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId"/>
		/// .
		/// </summary>
		/// <param name="appAttemptId"><see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId
		/// 	"/></param>
		/// <returns>
		/// Map of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// to
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerReport"/>
		/// for
		/// ApplicationAttemptId
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		IDictionary<ContainerId, ContainerReport> GetContainers(ApplicationAttemptId appAttemptId
			);
	}
}
