using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	/// <summary>The interface to an Application in the ResourceManager.</summary>
	/// <remarks>
	/// The interface to an Application in the ResourceManager. Take a
	/// look at
	/// <see cref="RMAppImpl"/>
	/// for its implementation. This interface
	/// exposes methods to access various updates in application status/report.
	/// </remarks>
	public interface RMApp : EventHandler<RMAppEvent>
	{
		/// <summary>
		/// The application id for this
		/// <see cref="RMApp"/>
		/// .
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// for this
		/// <see cref="RMApp"/>
		/// .
		/// </returns>
		ApplicationId GetApplicationId();

		/// <summary>
		/// The application submission context for this
		/// <see cref="RMApp"/>
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
		/// for this
		/// <see cref="RMApp"/>
		/// </returns>
		ApplicationSubmissionContext GetApplicationSubmissionContext();

		/// <summary>
		/// The current state of the
		/// <see cref="RMApp"/>
		/// .
		/// </summary>
		/// <returns>
		/// the current state
		/// <see cref="RMAppState"/>
		/// for this application.
		/// </returns>
		RMAppState GetState();

		/// <summary>The user who submitted this application.</summary>
		/// <returns>the user who submitted the application.</returns>
		string GetUser();

		/// <summary>Progress of application.</summary>
		/// <returns>
		/// the progress of the
		/// <see cref="RMApp"/>
		/// .
		/// </returns>
		float GetProgress();

		/// <summary>
		/// <see cref="RMApp"/>
		/// can have multiple application attempts
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttempt
		/// 	"/>
		/// .
		/// This method returns the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttempt
		/// 	"/>
		/// corresponding to
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId"/>
		/// .
		/// </summary>
		/// <param name="appAttemptId">the application attempt id</param>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttempt
		/// 	"/>
		/// corresponding to the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId"/>
		/// .
		/// </returns>
		RMAppAttempt GetRMAppAttempt(ApplicationAttemptId appAttemptId);

		/// <summary>
		/// Each Application is submitted to a queue decided by
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext.SetQueue(string)
		/// 	"/>
		/// .
		/// This method returns the queue to which an application was submitted.
		/// </summary>
		/// <returns>the queue to which the application was submitted to.</returns>
		string GetQueue();

		/// <summary>
		/// Reflects a change in the application's queue from the one specified in the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
		/// .
		/// </summary>
		/// <param name="name">the new queue name</param>
		void SetQueue(string name);

		/// <summary>
		/// The name of the application as set in
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext.SetApplicationName(string)
		/// 	"/>
		/// .
		/// </summary>
		/// <returns>the name of the application.</returns>
		string GetName();

		/// <summary>
		/// <see cref="RMApp"/>
		/// can have multiple application attempts
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttempt
		/// 	"/>
		/// .
		/// This method returns the current
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttempt
		/// 	"/>
		/// .
		/// </summary>
		/// <returns>
		/// the current
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttempt
		/// 	"/>
		/// </returns>
		RMAppAttempt GetCurrentAppAttempt();

		/// <summary>
		/// <see cref="RMApp"/>
		/// can have multiple application attempts
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttempt
		/// 	"/>
		/// .
		/// This method returns the all
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttempt
		/// 	"/>
		/// s for the RMApp.
		/// </summary>
		/// <returns>
		/// all
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttempt
		/// 	"/>
		/// s for the RMApp.
		/// </returns>
		IDictionary<ApplicationAttemptId, RMAppAttempt> GetAppAttempts();

		/// <summary>To get the status of an application in the RM, this method can be used.</summary>
		/// <remarks>
		/// To get the status of an application in the RM, this method can be used.
		/// If full access is not allowed then the following fields in the report
		/// will be stubbed:
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
		/// <param name="clientUserName">the user name of the client requesting the report</param>
		/// <param name="allowAccess">whether to allow full access to the report</param>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationReport"/>
		/// detailing the status of the application.
		/// </returns>
		ApplicationReport CreateAndGetApplicationReport(string clientUserName, bool allowAccess
			);

		/// <summary>
		/// To receive the collection of all
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode.RMNode"/>
		/// s whose updates have been
		/// received by the RMApp. Updates can be node becoming lost or becoming
		/// healthy etc. The method clears the information from the
		/// <see cref="RMApp"/>
		/// . So
		/// each call to this method gives the delta from the previous call.
		/// </summary>
		/// <param name="updatedNodes">Collection into which the updates are transferred</param>
		/// <returns>
		/// the number of nodes added to the
		/// <see cref="System.Collections.ICollection{E}"/>
		/// </returns>
		int PullRMNodeUpdates(ICollection<RMNode> updatedNodes);

		/// <summary>
		/// The finish time of the
		/// <see cref="RMApp"/>
		/// </summary>
		/// <returns>the finish time of the application.,</returns>
		long GetFinishTime();

		/// <summary>the start time of the application.</summary>
		/// <returns>the start time of the application.</returns>
		long GetStartTime();

		/// <summary>the submit time of the application.</summary>
		/// <returns>the submit time of the application.</returns>
		long GetSubmitTime();

		/// <summary>The tracking url for the application master.</summary>
		/// <returns>the tracking url for the application master.</returns>
		string GetTrackingUrl();

		/// <summary>The original tracking url for the application master.</summary>
		/// <returns>the original tracking url for the application master.</returns>
		string GetOriginalTrackingUrl();

		/// <summary>the diagnostics information for the application master.</summary>
		/// <returns>the diagnostics information for the application master.</returns>
		StringBuilder GetDiagnostics();

		/// <summary>
		/// The final finish state of the AM when unregistering as in
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.FinishApplicationMasterRequest.SetFinalApplicationStatus(Org.Apache.Hadoop.Yarn.Api.Records.FinalApplicationStatus)
		/// 	"/>
		/// .
		/// </summary>
		/// <returns>
		/// the final finish state of the AM as set in
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.FinishApplicationMasterRequest.SetFinalApplicationStatus(Org.Apache.Hadoop.Yarn.Api.Records.FinalApplicationStatus)
		/// 	"/>
		/// .
		/// </returns>
		FinalApplicationStatus GetFinalApplicationStatus();

		/// <summary>The number of max attempts of the application.</summary>
		/// <returns>the number of max attempts of the application.</returns>
		int GetMaxAppAttempts();

		/// <summary>Returns the application type</summary>
		/// <returns>the application type.</returns>
		string GetApplicationType();

		/// <summary>Get tags for the application</summary>
		/// <returns>tags corresponding to the application</returns>
		ICollection<string> GetApplicationTags();

		/// <summary>Check whether this application's state has been saved to the state store.
		/// 	</summary>
		/// <returns>the flag indicating whether the applications's state is stored.</returns>
		bool IsAppFinalStateStored();

		/// <summary>
		/// Nodes on which the containers for this
		/// <see cref="RMApp"/>
		/// ran.
		/// </summary>
		/// <returns>
		/// the set of nodes that ran any containers from this
		/// <see cref="RMApp"/>
		/// Add more node on which containers for this
		/// <see cref="RMApp"/>
		/// ran
		/// </returns>
		ICollection<NodeId> GetRanNodes();

		/// <summary>
		/// Create the external user-facing state of ApplicationMaster from the
		/// current state of the
		/// <see cref="RMApp"/>
		/// .
		/// </summary>
		/// <returns>the external user-facing state of ApplicationMaster.</returns>
		YarnApplicationState CreateApplicationState();

		/// <summary>
		/// Get RMAppMetrics of the
		/// <see cref="RMApp"/>
		/// .
		/// </summary>
		/// <returns>metrics</returns>
		RMAppMetrics GetRMAppMetrics();

		ReservationId GetReservationId();

		ResourceRequest GetAMResourceRequest();
	}
}
