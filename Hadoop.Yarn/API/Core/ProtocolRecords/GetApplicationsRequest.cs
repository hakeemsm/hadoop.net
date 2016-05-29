using System.Collections.Generic;
using Org.Apache.Commons.Lang.Math;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The request from clients to get a report of Applications
	/// in the cluster from the <code>ResourceManager</code>.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplications(GetApplicationsRequest)
	/// 	"/>
	public abstract class GetApplicationsRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetApplicationsRequest NewInstance()
		{
			GetApplicationsRequest request = Records.NewRecord<GetApplicationsRequest>();
			return request;
		}

		/// <summary>
		/// <p>
		/// The request from clients to get a report of Applications matching the
		/// giving application types in the cluster from the
		/// <code>ResourceManager</code>.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The request from clients to get a report of Applications matching the
		/// giving application types in the cluster from the
		/// <code>ResourceManager</code>.
		/// </p>
		/// </remarks>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplications(GetApplicationsRequest)
		/// 	">
		/// <p>Setting any of the parameters to null, would just disable that
		/// filter</p>
		/// </seealso>
		/// <param name="scope">
		/// 
		/// <see cref="ApplicationsRequestScope"/>
		/// to filter by
		/// </param>
		/// <param name="users">list of users to filter by</param>
		/// <param name="queues">list of scheduler queues to filter by</param>
		/// <param name="applicationTypes">types of applications</param>
		/// <param name="applicationTags">application tags to filter by</param>
		/// <param name="applicationStates">application states to filter by</param>
		/// <param name="startRange">range of application start times to filter by</param>
		/// <param name="finishRange">range of application finish times to filter by</param>
		/// <param name="limit">number of applications to limit to</param>
		/// <returns>
		/// 
		/// <see cref="GetApplicationsRequest"/>
		/// to be used with
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplications(GetApplicationsRequest)
		/// 	"/>
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetApplicationsRequest NewInstance(ApplicationsRequestScope scope, 
			ICollection<string> users, ICollection<string> queues, ICollection<string> applicationTypes
			, ICollection<string> applicationTags, EnumSet<YarnApplicationState> applicationStates
			, LongRange startRange, LongRange finishRange, long limit)
		{
			GetApplicationsRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetApplicationsRequest
				>();
			if (scope != null)
			{
				request.SetScope(scope);
			}
			request.SetUsers(users);
			request.SetQueues(queues);
			request.SetApplicationTypes(applicationTypes);
			request.SetApplicationTags(applicationTags);
			request.SetApplicationStates(applicationStates);
			if (startRange != null)
			{
				request.SetStartRange(startRange.GetMinimumLong(), startRange.GetMaximumLong());
			}
			if (finishRange != null)
			{
				request.SetFinishRange(finishRange.GetMinimumLong(), finishRange.GetMaximumLong()
					);
			}
			if (limit != null)
			{
				request.SetLimit(limit);
			}
			return request;
		}

		/// <summary>
		/// <p>
		/// The request from clients to get a report of Applications matching the
		/// giving application types in the cluster from the
		/// <code>ResourceManager</code>.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The request from clients to get a report of Applications matching the
		/// giving application types in the cluster from the
		/// <code>ResourceManager</code>.
		/// </p>
		/// </remarks>
		/// <param name="scope">
		/// 
		/// <see cref="ApplicationsRequestScope"/>
		/// to filter by
		/// </param>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplications(GetApplicationsRequest)
		/// 	"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetApplicationsRequest NewInstance(ApplicationsRequestScope scope)
		{
			GetApplicationsRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetApplicationsRequest
				>();
			request.SetScope(scope);
			return request;
		}

		/// <summary>
		/// <p>
		/// The request from clients to get a report of Applications matching the
		/// giving application types in the cluster from the
		/// <code>ResourceManager</code>.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The request from clients to get a report of Applications matching the
		/// giving application types in the cluster from the
		/// <code>ResourceManager</code>.
		/// </p>
		/// </remarks>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplications(GetApplicationsRequest)
		/// 	"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetApplicationsRequest NewInstance(ICollection<string> applicationTypes
			)
		{
			GetApplicationsRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetApplicationsRequest
				>();
			request.SetApplicationTypes(applicationTypes);
			return request;
		}

		/// <summary>
		/// <p>
		/// The request from clients to get a report of Applications matching the
		/// giving application states in the cluster from the
		/// <code>ResourceManager</code>.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The request from clients to get a report of Applications matching the
		/// giving application states in the cluster from the
		/// <code>ResourceManager</code>.
		/// </p>
		/// </remarks>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplications(GetApplicationsRequest)
		/// 	"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetApplicationsRequest NewInstance(EnumSet<YarnApplicationState> applicationStates
			)
		{
			GetApplicationsRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetApplicationsRequest
				>();
			request.SetApplicationStates(applicationStates);
			return request;
		}

		/// <summary>
		/// <p>
		/// The request from clients to get a report of Applications matching the
		/// giving and application types and application types in the cluster from the
		/// <code>ResourceManager</code>.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The request from clients to get a report of Applications matching the
		/// giving and application types and application types in the cluster from the
		/// <code>ResourceManager</code>.
		/// </p>
		/// </remarks>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplications(GetApplicationsRequest)
		/// 	"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetApplicationsRequest NewInstance(ICollection<string> applicationTypes
			, EnumSet<YarnApplicationState> applicationStates)
		{
			GetApplicationsRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetApplicationsRequest
				>();
			request.SetApplicationTypes(applicationTypes);
			request.SetApplicationStates(applicationStates);
			return request;
		}

		/// <summary>Get the application types to filter applications on</summary>
		/// <returns>Set of Application Types to filter on</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ICollection<string> GetApplicationTypes();

		/// <summary>Set the application types to filter applications on</summary>
		/// <param name="applicationTypes">
		/// A Set of Application Types to filter on.
		/// If not defined, match all applications
		/// </param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationTypes(ICollection<string> applicationTypes);

		/// <summary>Get the application states to filter applications on</summary>
		/// <returns>Set of Application states to filter on</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract EnumSet<YarnApplicationState> GetApplicationStates();

		/// <summary>Set the application states to filter applications on</summary>
		/// <param name="applicationStates">
		/// A Set of Application states to filter on.
		/// If not defined, match all running applications
		/// </param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationStates(EnumSet<YarnApplicationState> applicationStates
			);

		/// <summary>Set the application states to filter applications on</summary>
		/// <param name="applicationStates">
		/// all lower-case string representation of the
		/// application states to filter on
		/// </param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationStates(ICollection<string> applicationStates);

		/// <summary>Get the users to filter applications on</summary>
		/// <returns>set of users to filter applications on</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract ICollection<string> GetUsers();

		/// <summary>Set the users to filter applications on</summary>
		/// <param name="users">set of users to filter applications on</param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetUsers(ICollection<string> users);

		/// <summary>Get the queues to filter applications on</summary>
		/// <returns>set of queues to filter applications on</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract ICollection<string> GetQueues();

		/// <summary>Set the queue to filter applications on</summary>
		/// <param name="queue">user to filter applications on</param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetQueues(ICollection<string> queue);

		/// <summary>Get the limit on the number applications to return</summary>
		/// <returns>number of applications to limit to</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract long GetLimit();

		/// <summary>Limit the number applications to return</summary>
		/// <param name="limit">number of applications to limit to</param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetLimit(long limit);

		/// <summary>Get the range of start times to filter applications on</summary>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Commons.Lang.Math.LongRange"/>
		/// of start times to filter applications on
		/// </returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract LongRange GetStartRange();

		/// <summary>Set the range of start times to filter applications on</summary>
		/// <param name="range"/>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetStartRange(LongRange range);

		/// <summary>Set the range of start times to filter applications on</summary>
		/// <param name="begin">beginning of the range</param>
		/// <param name="end">end of the range</param>
		/// <exception cref="System.ArgumentException"/>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetStartRange(long begin, long end);

		/// <summary>Get the range of finish times to filter applications on</summary>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Commons.Lang.Math.LongRange"/>
		/// of finish times to filter applications on
		/// </returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract LongRange GetFinishRange();

		/// <summary>Set the range of finish times to filter applications on</summary>
		/// <param name="range"/>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetFinishRange(LongRange range);

		/// <summary>Set the range of finish times to filter applications on</summary>
		/// <param name="begin">beginning of the range</param>
		/// <param name="end">end of the range</param>
		/// <exception cref="System.ArgumentException"/>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetFinishRange(long begin, long end);

		/// <summary>Get the tags to filter applications on</summary>
		/// <returns>list of tags to filter on</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract ICollection<string> GetApplicationTags();

		/// <summary>Set the list of tags to filter applications on</summary>
		/// <param name="tags">list of tags to filter on</param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationTags(ICollection<string> tags);

		/// <summary>
		/// Get the
		/// <see cref="ApplicationsRequestScope"/>
		/// of applications to be filtered.
		/// </summary>
		/// <returns>
		/// 
		/// <see cref="ApplicationsRequestScope"/>
		/// of applications to return.
		/// </returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract ApplicationsRequestScope GetScope();

		/// <summary>
		/// Set the
		/// <see cref="ApplicationsRequestScope"/>
		/// of applications to filter.
		/// </summary>
		/// <param name="scope">scope to use for filtering applications</param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetScope(ApplicationsRequestScope scope);
	}
}
