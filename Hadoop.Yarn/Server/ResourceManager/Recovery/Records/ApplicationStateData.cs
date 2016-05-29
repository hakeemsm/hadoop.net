using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records
{
	/// <summary>
	/// Contains all the state data that needs to be stored persistently
	/// for an Application
	/// </summary>
	public abstract class ApplicationStateData
	{
		public IDictionary<ApplicationAttemptId, ApplicationAttemptStateData> attempts = 
			new Dictionary<ApplicationAttemptId, ApplicationAttemptStateData>();

		public static ApplicationStateData NewInstance(long submitTime, long startTime, string
			 user, ApplicationSubmissionContext submissionContext, RMAppState state, string 
			diagnostics, long finishTime)
		{
			ApplicationStateData appState = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ApplicationStateData
				>();
			appState.SetSubmitTime(submitTime);
			appState.SetStartTime(startTime);
			appState.SetUser(user);
			appState.SetApplicationSubmissionContext(submissionContext);
			appState.SetState(state);
			appState.SetDiagnostics(diagnostics);
			appState.SetFinishTime(finishTime);
			return appState;
		}

		public static ApplicationStateData NewInstance(long submitTime, long startTime, ApplicationSubmissionContext
			 context, string user)
		{
			return NewInstance(submitTime, startTime, user, context, null, string.Empty, 0);
		}

		public virtual int GetAttemptCount()
		{
			return attempts.Count;
		}

		public virtual ApplicationAttemptStateData GetAttempt(ApplicationAttemptId attemptId
			)
		{
			return attempts[attemptId];
		}

		public abstract YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto
			 GetProto();

		/// <summary>The time at which the application was received by the Resource Manager</summary>
		/// <returns>submitTime</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetSubmitTime();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetSubmitTime(long submitTime);

		/// <summary>Get the <em>start time</em> of the application.</summary>
		/// <returns><em>start time</em> of the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract long GetStartTime();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetStartTime(long startTime);

		/// <summary>The application submitter</summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetUser(string user);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetUser();

		/// <summary>
		/// The
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
		/// for the application
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// can be obtained from the this
		/// </summary>
		/// <returns>ApplicationSubmissionContext</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ApplicationSubmissionContext GetApplicationSubmissionContext();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationSubmissionContext(ApplicationSubmissionContext
			 context);

		/// <summary>Get the final state of the application.</summary>
		/// <returns>the final state of the application.</returns>
		public abstract RMAppState GetState();

		public abstract void SetState(RMAppState state);

		/// <summary>Get the diagnostics information for the application master.</summary>
		/// <returns>the diagnostics information for the application master.</returns>
		public abstract string GetDiagnostics();

		public abstract void SetDiagnostics(string diagnostics);

		/// <summary>The finish time of the application.</summary>
		/// <returns>the finish time of the application.,</returns>
		public abstract long GetFinishTime();

		public abstract void SetFinishTime(long finishTime);
	}
}
