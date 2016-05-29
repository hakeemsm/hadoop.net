using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <c>ApplicationSubmissionContext</c>
	/// represents all of the
	/// information needed by the
	/// <c>ResourceManager</c>
	/// to launch
	/// the
	/// <c>ApplicationMaster</c>
	/// for an application.
	/// <p>
	/// It includes details such as:
	/// <ul>
	/// <li>
	/// <see cref="ApplicationId"/>
	/// of the application.</li>
	/// <li>Application user.</li>
	/// <li>Application name.</li>
	/// <li>
	/// <see cref="Priority"/>
	/// of the application.</li>
	/// <li>
	/// <see cref="ContainerLaunchContext"/>
	/// of the container in which the
	/// <code>ApplicationMaster</code> is executed.
	/// </li>
	/// <li>
	/// maxAppAttempts. The maximum number of application attempts.
	/// It should be no larger than the global number of max attempts in the
	/// Yarn configuration.
	/// </li>
	/// <li>
	/// attemptFailuresValidityInterval. The default value is -1.
	/// when attemptFailuresValidityInterval in milliseconds is set to
	/// <literal>&gt;</literal>
	/// 0, the failure number will no take failures which happen
	/// out of the validityInterval into failure count. If failure count
	/// reaches to maxAppAttempts, the application will be failed.
	/// </li>
	/// <li>Optional, application-specific
	/// <see cref="LogAggregationContext"/>
	/// </li>
	/// </ul>
	/// </summary>
	/// <seealso cref="ContainerLaunchContext"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.SubmitApplication(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.SubmitApplicationRequest)
	/// 	"/>
	public abstract class ApplicationSubmissionContext
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static ApplicationSubmissionContext NewInstance(ApplicationId applicationId
			, string applicationName, string queue, Priority priority, ContainerLaunchContext
			 amContainer, bool isUnmanagedAM, bool cancelTokensWhenComplete, int maxAppAttempts
			, Resource resource, string applicationType, bool keepContainers, string appLabelExpression
			, string amContainerLabelExpression)
		{
			ApplicationSubmissionContext context = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ApplicationSubmissionContext>();
			context.SetApplicationId(applicationId);
			context.SetApplicationName(applicationName);
			context.SetQueue(queue);
			context.SetPriority(priority);
			context.SetAMContainerSpec(amContainer);
			context.SetUnmanagedAM(isUnmanagedAM);
			context.SetCancelTokensWhenComplete(cancelTokensWhenComplete);
			context.SetMaxAppAttempts(maxAppAttempts);
			context.SetApplicationType(applicationType);
			context.SetKeepContainersAcrossApplicationAttempts(keepContainers);
			context.SetNodeLabelExpression(appLabelExpression);
			context.SetResource(resource);
			ResourceRequest amReq = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ResourceRequest
				>();
			amReq.SetResourceName(ResourceRequest.Any);
			amReq.SetCapability(resource);
			amReq.SetNumContainers(1);
			amReq.SetRelaxLocality(true);
			amReq.SetNodeLabelExpression(amContainerLabelExpression);
			context.SetAMContainerResourceRequest(amReq);
			return context;
		}

		public static ApplicationSubmissionContext NewInstance(ApplicationId applicationId
			, string applicationName, string queue, Priority priority, ContainerLaunchContext
			 amContainer, bool isUnmanagedAM, bool cancelTokensWhenComplete, int maxAppAttempts
			, Resource resource, string applicationType, bool keepContainers)
		{
			return NewInstance(applicationId, applicationName, queue, priority, amContainer, 
				isUnmanagedAM, cancelTokensWhenComplete, maxAppAttempts, resource, applicationType
				, keepContainers, null, null);
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static ApplicationSubmissionContext NewInstance(ApplicationId applicationId
			, string applicationName, string queue, Priority priority, ContainerLaunchContext
			 amContainer, bool isUnmanagedAM, bool cancelTokensWhenComplete, int maxAppAttempts
			, Resource resource, string applicationType)
		{
			return NewInstance(applicationId, applicationName, queue, priority, amContainer, 
				isUnmanagedAM, cancelTokensWhenComplete, maxAppAttempts, resource, applicationType
				, false, null, null);
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static ApplicationSubmissionContext NewInstance(ApplicationId applicationId
			, string applicationName, string queue, Priority priority, ContainerLaunchContext
			 amContainer, bool isUnmanagedAM, bool cancelTokensWhenComplete, int maxAppAttempts
			, Resource resource)
		{
			return NewInstance(applicationId, applicationName, queue, priority, amContainer, 
				isUnmanagedAM, cancelTokensWhenComplete, maxAppAttempts, resource, null);
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static ApplicationSubmissionContext NewInstance(ApplicationId applicationId
			, string applicationName, string queue, ContainerLaunchContext amContainer, bool
			 isUnmanagedAM, bool cancelTokensWhenComplete, int maxAppAttempts, string applicationType
			, bool keepContainers, string appLabelExpression, ResourceRequest resourceRequest
			)
		{
			ApplicationSubmissionContext context = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ApplicationSubmissionContext>();
			context.SetApplicationId(applicationId);
			context.SetApplicationName(applicationName);
			context.SetQueue(queue);
			context.SetAMContainerSpec(amContainer);
			context.SetUnmanagedAM(isUnmanagedAM);
			context.SetCancelTokensWhenComplete(cancelTokensWhenComplete);
			context.SetMaxAppAttempts(maxAppAttempts);
			context.SetApplicationType(applicationType);
			context.SetKeepContainersAcrossApplicationAttempts(keepContainers);
			context.SetNodeLabelExpression(appLabelExpression);
			context.SetAMContainerResourceRequest(resourceRequest);
			return context;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static ApplicationSubmissionContext NewInstance(ApplicationId applicationId
			, string applicationName, string queue, Priority priority, ContainerLaunchContext
			 amContainer, bool isUnmanagedAM, bool cancelTokensWhenComplete, int maxAppAttempts
			, Resource resource, string applicationType, bool keepContainers, long attemptFailuresValidityInterval
			)
		{
			ApplicationSubmissionContext context = NewInstance(applicationId, applicationName
				, queue, priority, amContainer, isUnmanagedAM, cancelTokensWhenComplete, maxAppAttempts
				, resource, applicationType, keepContainers);
			context.SetAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
			return context;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static ApplicationSubmissionContext NewInstance(ApplicationId applicationId
			, string applicationName, string queue, Priority priority, ContainerLaunchContext
			 amContainer, bool isUnmanagedAM, bool cancelTokensWhenComplete, int maxAppAttempts
			, Resource resource, string applicationType, bool keepContainers, LogAggregationContext
			 logAggregationContext)
		{
			ApplicationSubmissionContext context = NewInstance(applicationId, applicationName
				, queue, priority, amContainer, isUnmanagedAM, cancelTokensWhenComplete, maxAppAttempts
				, resource, applicationType, keepContainers);
			context.SetLogAggregationContext(logAggregationContext);
			return context;
		}

		/// <summary>Get the <code>ApplicationId</code> of the submitted application.</summary>
		/// <returns><code>ApplicationId</code> of the submitted application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ApplicationId GetApplicationId();

		/// <summary>Set the <code>ApplicationId</code> of the submitted application.</summary>
		/// <param name="applicationId">
		/// <code>ApplicationId</code> of the submitted
		/// application
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetApplicationId(ApplicationId applicationId);

		/// <summary>Get the application <em>name</em>.</summary>
		/// <returns>application name</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetApplicationName();

		/// <summary>Set the application <em>name</em>.</summary>
		/// <param name="applicationName">application name</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetApplicationName(string applicationName);

		/// <summary>Get the <em>queue</em> to which the application is being submitted.</summary>
		/// <returns><em>queue</em> to which the application is being submitted</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetQueue();

		/// <summary>Set the <em>queue</em> to which the application is being submitted</summary>
		/// <param name="queue"><em>queue</em> to which the application is being submitted</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetQueue(string queue);

		/// <summary>Get the <code>Priority</code> of the application.</summary>
		/// <returns><code>Priority</code> of the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Priority GetPriority();

		/// <summary>Set the <code>Priority</code> of the application.</summary>
		/// <param name="priority"><code>Priority</code> of the application</param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetPriority(Priority priority);

		/// <summary>
		/// Get the <code>ContainerLaunchContext</code> to describe the
		/// <code>Container</code> with which the <code>ApplicationMaster</code> is
		/// launched.
		/// </summary>
		/// <returns>
		/// <code>ContainerLaunchContext</code> for the
		/// <code>ApplicationMaster</code> container
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ContainerLaunchContext GetAMContainerSpec();

		/// <summary>
		/// Set the <code>ContainerLaunchContext</code> to describe the
		/// <code>Container</code> with which the <code>ApplicationMaster</code> is
		/// launched.
		/// </summary>
		/// <param name="amContainer">
		/// <code>ContainerLaunchContext</code> for the
		/// <code>ApplicationMaster</code> container
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetAMContainerSpec(ContainerLaunchContext amContainer);

		/// <summary>Get if the RM should manage the execution of the AM.</summary>
		/// <remarks>
		/// Get if the RM should manage the execution of the AM.
		/// If true, then the RM
		/// will not allocate a container for the AM and start it. It will expect the
		/// AM to be launched and connect to the RM within the AM liveliness period and
		/// fail the app otherwise. The client should launch the AM only after the RM
		/// has ACCEPTED the application and changed the <code>YarnApplicationState</code>.
		/// Such apps will not be retried by the RM on app attempt failure.
		/// The default value is false.
		/// </remarks>
		/// <returns>true if the AM is not managed by the RM</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract bool GetUnmanagedAM();

		/// <param name="value">true if RM should not manage the AM</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetUnmanagedAM(bool value);

		/// <returns>true if tokens should be canceled when the app completes.</returns>
		[InterfaceStability.Unstable]
		public abstract bool GetCancelTokensWhenComplete();

		/// <summary>
		/// Set to false if tokens should not be canceled when the app finished else
		/// false.
		/// </summary>
		/// <remarks>
		/// Set to false if tokens should not be canceled when the app finished else
		/// false.  WARNING: this is not recommended unless you want your single job
		/// tokens to be reused by others jobs.
		/// </remarks>
		/// <param name="cancel">true if tokens should be canceled when the app finishes.</param>
		[InterfaceStability.Unstable]
		public abstract void SetCancelTokensWhenComplete(bool cancel);

		/// <returns>the number of max attempts of the application to be submitted</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract int GetMaxAppAttempts();

		/// <summary>Set the number of max attempts of the application to be submitted.</summary>
		/// <remarks>
		/// Set the number of max attempts of the application to be submitted. WARNING:
		/// it should be no larger than the global number of max attempts in the Yarn
		/// configuration.
		/// </remarks>
		/// <param name="maxAppAttempts">
		/// the number of max attempts of the application
		/// to be submitted.
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetMaxAppAttempts(int maxAppAttempts);

		/// <summary>
		/// Get the resource required by the <code>ApplicationMaster</code> for this
		/// application.
		/// </summary>
		/// <remarks>
		/// Get the resource required by the <code>ApplicationMaster</code> for this
		/// application. Please note this will be DEPRECATED, use <em>getResource</em>
		/// in <em>getAMContainerResourceRequest</em> instead.
		/// </remarks>
		/// <returns>
		/// the resource required by the <code>ApplicationMaster</code> for
		/// this application.
		/// </returns>
		[InterfaceAudience.Public]
		public abstract Resource GetResource();

		/// <summary>
		/// Set the resource required by the <code>ApplicationMaster</code> for this
		/// application.
		/// </summary>
		/// <param name="resource">
		/// the resource required by the <code>ApplicationMaster</code>
		/// for this application.
		/// </param>
		[InterfaceAudience.Public]
		public abstract void SetResource(Resource resource);

		/// <summary>Get the application type</summary>
		/// <returns>the application type</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetApplicationType();

		/// <summary>Set the application type</summary>
		/// <param name="applicationType">the application type</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetApplicationType(string applicationType);

		/// <summary>
		/// Get the flag which indicates whether to keep containers across application
		/// attempts or not.
		/// </summary>
		/// <returns>
		/// the flag which indicates whether to keep containers across
		/// application attempts or not.
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract bool GetKeepContainersAcrossApplicationAttempts();

		/// <summary>
		/// Set the flag which indicates whether to keep containers across application
		/// attempts.
		/// </summary>
		/// <remarks>
		/// Set the flag which indicates whether to keep containers across application
		/// attempts.
		/// <p>
		/// If the flag is true, running containers will not be killed when application
		/// attempt fails and these containers will be retrieved by the new application
		/// attempt on registration via
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.RegisterApplicationMaster(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.RegisterApplicationMasterRequest)
		/// 	"/>
		/// .
		/// </p>
		/// </remarks>
		/// <param name="keepContainers">
		/// the flag which indicates whether to keep containers across
		/// application attempts.
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetKeepContainersAcrossApplicationAttempts(bool keepContainers
			);

		/// <summary>Get tags for the application</summary>
		/// <returns>the application tags</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ICollection<string> GetApplicationTags();

		/// <summary>Set tags for the application.</summary>
		/// <remarks>
		/// Set tags for the application. A maximum of
		/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.ApplicationMaxTags"/>
		/// are allowed
		/// per application. Each tag can be at most
		/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.ApplicationMaxTagLength"
		/// 	/>
		/// characters, and can contain only ASCII characters.
		/// </remarks>
		/// <param name="tags">tags to set</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetApplicationTags(ICollection<string> tags);

		/// <summary>Get node-label-expression for this app.</summary>
		/// <remarks>
		/// Get node-label-expression for this app. If this is set, all containers of
		/// this application without setting node-label-expression in ResurceRequest
		/// will get allocated resources on only those nodes that satisfy this
		/// node-label-expression.
		/// If different node-label-expression of this app and ResourceRequest are set
		/// at the same time, the one set in ResourceRequest will be used when
		/// allocating container
		/// </remarks>
		/// <returns>node-label-expression for this app</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract string GetNodeLabelExpression();

		/// <summary>Set node-label-expression for this app</summary>
		/// <param name="nodeLabelExpression">node-label-expression of this app</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract void SetNodeLabelExpression(string nodeLabelExpression);

		/// <summary>
		/// Get ResourceRequest of AM container, if this is not null, scheduler will
		/// use this to acquire resource for AM container.
		/// </summary>
		/// <remarks>
		/// Get ResourceRequest of AM container, if this is not null, scheduler will
		/// use this to acquire resource for AM container.
		/// If this is null, scheduler will assemble a ResourceRequest by using
		/// <em>getResource</em> and <em>getPriority</em> of
		/// <em>ApplicationSubmissionContext</em>.
		/// Number of containers and Priority will be ignore.
		/// </remarks>
		/// <returns>ResourceRequest of AM container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract ResourceRequest GetAMContainerResourceRequest();

		/// <summary>Set ResourceRequest of AM container</summary>
		/// <param name="request">of AM container</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract void SetAMContainerResourceRequest(ResourceRequest request);

		/// <summary>Get the attemptFailuresValidityInterval in milliseconds for the application
		/// 	</summary>
		/// <returns>the attemptFailuresValidityInterval</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract long GetAttemptFailuresValidityInterval();

		/// <summary>Set the attemptFailuresValidityInterval in milliseconds for the application
		/// 	</summary>
		/// <param name="attemptFailuresValidityInterval"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetAttemptFailuresValidityInterval(long attemptFailuresValidityInterval
			);

		/// <summary>Get <code>LogAggregationContext</code> of the application</summary>
		/// <returns><code>LogAggregationContext</code> of the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract LogAggregationContext GetLogAggregationContext();

		/// <summary>Set <code>LogAggregationContext</code> for the application</summary>
		/// <param name="logAggregationContext">for the application</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetLogAggregationContext(LogAggregationContext logAggregationContext
			);

		/// <summary>
		/// Get the reservation id, that corresponds to a valid resource allocation in
		/// the scheduler (between start and end time of the corresponding reservation)
		/// </summary>
		/// <returns>
		/// the reservation id representing the unique id of the corresponding
		/// reserved resource allocation in the scheduler
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ReservationId GetReservationID();

		/// <summary>
		/// Set the reservation id, that correspond to a valid resource allocation in
		/// the scheduler (between start and end time of the corresponding reservation)
		/// </summary>
		/// <param name="reservationID">
		/// representing the unique id of the
		/// corresponding reserved resource allocation in the scheduler
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetReservationID(ReservationId reservationID);
	}
}
