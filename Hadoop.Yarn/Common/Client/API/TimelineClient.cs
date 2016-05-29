using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api
{
	/// <summary>
	/// A client library that can be used to post some information in terms of a
	/// number of conceptual entities.
	/// </summary>
	public abstract class TimelineClient : AbstractService
	{
		/// <summary>Create a timeline client.</summary>
		/// <remarks>
		/// Create a timeline client. The current UGI when the user initialize the
		/// client will be used to do the put and the delegation token operations. The
		/// current user may use
		/// <see cref="Org.Apache.Hadoop.Security.UserGroupInformation.DoAs{T}(Sharpen.PrivilegedAction{T})
		/// 	"/>
		/// another user to
		/// construct and initialize a timeline client if the following operations are
		/// supposed to be conducted by that user.
		/// </remarks>
		/// <returns>a timeline client</returns>
		[InterfaceAudience.Public]
		public static Org.Apache.Hadoop.Yarn.Client.Api.TimelineClient CreateTimelineClient
			()
		{
			Org.Apache.Hadoop.Yarn.Client.Api.TimelineClient client = new TimelineClientImpl(
				);
			return client;
		}

		[InterfaceAudience.Private]
		protected internal TimelineClient(string name)
			: base(name)
		{
		}

		/// <summary>
		/// <p>
		/// Send the information of a number of conceptual entities to the timeline
		/// server.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Send the information of a number of conceptual entities to the timeline
		/// server. It is a blocking API. The method will not return until it gets the
		/// response from the timeline server.
		/// </p>
		/// </remarks>
		/// <param name="entities">
		/// the collection of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEntity"/>
		/// </param>
		/// <returns>the error information if the sent entities are not correctly stored</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Public]
		public abstract TimelinePutResponse PutEntities(params TimelineEntity[] entities);

		/// <summary>
		/// <p>
		/// Send the information of a domain to the timeline server.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Send the information of a domain to the timeline server. It is a
		/// blocking API. The method will not return until it gets the response from
		/// the timeline server.
		/// </p>
		/// </remarks>
		/// <param name="domain">
		/// an
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineDomain"/>
		/// object
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Public]
		public abstract void PutDomain(TimelineDomain domain);

		/// <summary>
		/// <p>
		/// Get a delegation token so as to be able to talk to the timeline server in a
		/// secure way.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a delegation token so as to be able to talk to the timeline server in a
		/// secure way.
		/// </p>
		/// </remarks>
		/// <param name="renewer">
		/// Address of the renewer who can renew these tokens when needed by
		/// securely talking to the timeline server
		/// </param>
		/// <returns>
		/// a delegation token (
		/// <see cref="Org.Apache.Hadoop.Security.Token.Token{T}"/>
		/// ) that can be used to talk to the
		/// timeline server
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Public]
		public abstract Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier
			> GetDelegationToken(string renewer);

		/// <summary>
		/// <p>
		/// Renew a timeline delegation token.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Renew a timeline delegation token.
		/// </p>
		/// </remarks>
		/// <param name="timelineDT">the delegation token to renew</param>
		/// <returns>the new expiration time</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Public]
		public abstract long RenewDelegationToken(Org.Apache.Hadoop.Security.Token.Token<
			TimelineDelegationTokenIdentifier> timelineDT);

		/// <summary>
		/// <p>
		/// Cancel a timeline delegation token.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Cancel a timeline delegation token.
		/// </p>
		/// </remarks>
		/// <param name="timelineDT">the delegation token to cancel</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Public]
		public abstract void CancelDelegationToken(Org.Apache.Hadoop.Security.Token.Token
			<TimelineDelegationTokenIdentifier> timelineDT);
	}
}
