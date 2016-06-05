using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api
{
	public abstract class AHSClient : AbstractService
	{
		/// <summary>Create a new instance of AHSClient.</summary>
		[InterfaceAudience.Public]
		public static Org.Apache.Hadoop.Yarn.Client.Api.AHSClient CreateAHSClient()
		{
			Org.Apache.Hadoop.Yarn.Client.Api.AHSClient client = new AHSClientImpl();
			return client;
		}

		[InterfaceAudience.Private]
		public AHSClient(string name)
			: base(name)
		{
		}

		/// <summary>Get a report of the given Application.</summary>
		/// <remarks>
		/// Get a report of the given Application.
		/// <p>
		/// In secure mode, <code>YARN</code> verifies access to the application, queue
		/// etc. before accepting the request.
		/// <p>
		/// If the user does not have <code>VIEW_APP</code> access then the following
		/// fields in the report will be set to stubbed values:
		/// <ul>
		/// <li>host - set to "N/A"</li>
		/// <li>RPC port - set to -1</li>
		/// <li>client token - set to "N/A"</li>
		/// <li>diagnostics - set to "N/A"</li>
		/// <li>tracking URL - set to "N/A"</li>
		/// <li>original tracking URL - set to "N/A"</li>
		/// <li>resource usage report - all values are -1</li>
		/// </ul>
		/// </remarks>
		/// <param name="appId">
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// of the application that needs a report
		/// </param>
		/// <returns>application report</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract ApplicationReport GetApplicationReport(ApplicationId appId);

		/// <summary>
		/// <p>
		/// Get a report (ApplicationReport) of all Applications in the cluster.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report (ApplicationReport) of all Applications in the cluster.
		/// </p>
		/// <p>
		/// If the user does not have <code>VIEW_APP</code> access for an application
		/// then the corresponding report will be filtered as described in
		/// <see cref="GetApplicationReport(Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId)
		/// 	"/>
		/// .
		/// </p>
		/// </remarks>
		/// <returns>a list of reports for all applications</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<ApplicationReport> GetApplications();

		/// <summary>
		/// <p>
		/// Get a report of the given ApplicationAttempt.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report of the given ApplicationAttempt.
		/// </p>
		/// <p>
		/// In secure mode, <code>YARN</code> verifies access to the application, queue
		/// etc. before accepting the request.
		/// </p>
		/// </remarks>
		/// <param name="applicationAttemptId">
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId"/>
		/// of the application attempt that needs
		/// a report
		/// </param>
		/// <returns>application attempt report</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.ApplicationAttemptNotFoundException
		/// 	">
		/// if application attempt
		/// not found
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public abstract ApplicationAttemptReport GetApplicationAttemptReport(ApplicationAttemptId
			 applicationAttemptId);

		/// <summary>
		/// <p>
		/// Get a report of all (ApplicationAttempts) of Application in the cluster.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report of all (ApplicationAttempts) of Application in the cluster.
		/// </p>
		/// </remarks>
		/// <param name="applicationId"/>
		/// <returns>
		/// a list of reports for all application attempts for specified
		/// application
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<ApplicationAttemptReport> GetApplicationAttempts(ApplicationId
			 applicationId);

		/// <summary>
		/// <p>
		/// Get a report of the given Container.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report of the given Container.
		/// </p>
		/// <p>
		/// In secure mode, <code>YARN</code> verifies access to the application, queue
		/// etc. before accepting the request.
		/// </p>
		/// </remarks>
		/// <param name="containerId">
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// of the container that needs a report
		/// </param>
		/// <returns>container report</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.ContainerNotFoundException">if container not found
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		public abstract ContainerReport GetContainerReport(ContainerId containerId);

		/// <summary>
		/// <p>
		/// Get a report of all (Containers) of ApplicationAttempt in the cluster.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report of all (Containers) of ApplicationAttempt in the cluster.
		/// </p>
		/// </remarks>
		/// <param name="applicationAttemptId"/>
		/// <returns>
		/// a list of reports of all containers for specified application
		/// attempt
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<ContainerReport> GetContainers(ApplicationAttemptId applicationAttemptId
			);
	}
}
