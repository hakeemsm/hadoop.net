using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records
{
	public abstract class ApplicationAttemptStateData
	{
		/*
		* Contains the state data that needs to be persisted for an ApplicationAttempt
		*/
		public static ApplicationAttemptStateData NewInstance(ApplicationAttemptId attemptId
			, Container container, Credentials attemptTokens, long startTime, RMAppAttemptState
			 finalState, string finalTrackingUrl, string diagnostics, FinalApplicationStatus
			 amUnregisteredFinalStatus, int exitStatus, long finishTime, long memorySeconds, 
			long vcoreSeconds)
		{
			ApplicationAttemptStateData attemptStateData = Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<ApplicationAttemptStateData>();
			attemptStateData.SetAttemptId(attemptId);
			attemptStateData.SetMasterContainer(container);
			attemptStateData.SetAppAttemptTokens(attemptTokens);
			attemptStateData.SetState(finalState);
			attemptStateData.SetFinalTrackingUrl(finalTrackingUrl);
			attemptStateData.SetDiagnostics(diagnostics == null ? string.Empty : diagnostics);
			attemptStateData.SetStartTime(startTime);
			attemptStateData.SetFinalApplicationStatus(amUnregisteredFinalStatus);
			attemptStateData.SetAMContainerExitStatus(exitStatus);
			attemptStateData.SetFinishTime(finishTime);
			attemptStateData.SetMemorySeconds(memorySeconds);
			attemptStateData.SetVcoreSeconds(vcoreSeconds);
			return attemptStateData;
		}

		public static ApplicationAttemptStateData NewInstance(ApplicationAttemptId attemptId
			, Container masterContainer, Credentials attemptTokens, long startTime, long memorySeconds
			, long vcoreSeconds)
		{
			return NewInstance(attemptId, masterContainer, attemptTokens, startTime, null, "N/A"
				, string.Empty, null, ContainerExitStatus.Invalid, 0, memorySeconds, vcoreSeconds
				);
		}

		public abstract YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto
			 GetProto();

		/// <summary>The ApplicationAttemptId for the application attempt</summary>
		/// <returns>ApplicationAttemptId for the application attempt</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ApplicationAttemptId GetAttemptId();

		public abstract void SetAttemptId(ApplicationAttemptId attemptId);

		/*
		* The master container running the application attempt
		* @return Container that hosts the attempt
		*/
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract Container GetMasterContainer();

		public abstract void SetMasterContainer(Container container);

		/// <summary>The application attempt tokens that belong to this attempt</summary>
		/// <returns>The application attempt tokens that belong to this attempt</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract Credentials GetAppAttemptTokens();

		public abstract void SetAppAttemptTokens(Credentials attemptTokens);

		/// <summary>Get the final state of the application attempt.</summary>
		/// <returns>the final state of the application attempt.</returns>
		public abstract RMAppAttemptState GetState();

		public abstract void SetState(RMAppAttemptState state);

		/// <summary>
		/// Get the original not-proxied <em>final tracking url</em> for the
		/// application.
		/// </summary>
		/// <remarks>
		/// Get the original not-proxied <em>final tracking url</em> for the
		/// application. This is intended to only be used by the proxy itself.
		/// </remarks>
		/// <returns>
		/// the original not-proxied <em>final tracking url</em> for the
		/// application
		/// </returns>
		public abstract string GetFinalTrackingUrl();

		/// <summary>Set the final tracking Url of the AM.</summary>
		/// <param name="url"/>
		public abstract void SetFinalTrackingUrl(string url);

		/// <summary>Get the <em>diagnositic information</em> of the attempt</summary>
		/// <returns><em>diagnositic information</em> of the attempt</returns>
		public abstract string GetDiagnostics();

		public abstract void SetDiagnostics(string diagnostics);

		/// <summary>Get the <em>start time</em> of the application.</summary>
		/// <returns><em>start time</em> of the application</returns>
		public abstract long GetStartTime();

		public abstract void SetStartTime(long startTime);

		/// <summary>Get the <em>final finish status</em> of the application.</summary>
		/// <returns><em>final finish status</em> of the application</returns>
		public abstract FinalApplicationStatus GetFinalApplicationStatus();

		public abstract void SetFinalApplicationStatus(FinalApplicationStatus finishState
			);

		public abstract int GetAMContainerExitStatus();

		public abstract void SetAMContainerExitStatus(int exitStatus);

		/// <summary>Get the <em>finish time</em> of the application attempt.</summary>
		/// <returns><em>finish time</em> of the application attempt</returns>
		public abstract long GetFinishTime();

		public abstract void SetFinishTime(long finishTime);

		/// <summary>Get the <em>memory seconds</em> (in MB seconds) of the application.</summary>
		/// <returns><em>memory seconds</em> (in MB seconds) of the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetMemorySeconds();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetMemorySeconds(long memorySeconds);

		/// <summary>Get the <em>vcore seconds</em> of the application.</summary>
		/// <returns><em>vcore seconds</em> of the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetVcoreSeconds();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetVcoreSeconds(long vcoreSeconds);
	}
}
