using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The request sent by a client to <em>submit an application</em> to the
	/// <code>ResourceManager</code>.</p>
	/// <p>The request, via
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
	/// , contains
	/// details such as queue,
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
	/// required to run the
	/// <code>ApplicationMaster</code>, the equivalent of
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerLaunchContext"/>
	/// for launching the
	/// <code>ApplicationMaster</code> etc.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.SubmitApplication(SubmitApplicationRequest)
	/// 	"/>
	public abstract class SubmitApplicationRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static SubmitApplicationRequest NewInstance(ApplicationSubmissionContext context
			)
		{
			SubmitApplicationRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				SubmitApplicationRequest>();
			request.SetApplicationSubmissionContext(context);
			return request;
		}

		/// <summary>Get the <code>ApplicationSubmissionContext</code> for the application.</summary>
		/// <returns><code>ApplicationSubmissionContext</code> for the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ApplicationSubmissionContext GetApplicationSubmissionContext();

		/// <summary>Set the <code>ApplicationSubmissionContext</code> for the application.</summary>
		/// <param name="context">
		/// <code>ApplicationSubmissionContext</code> for the
		/// application
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetApplicationSubmissionContext(ApplicationSubmissionContext
			 context);
	}
}
