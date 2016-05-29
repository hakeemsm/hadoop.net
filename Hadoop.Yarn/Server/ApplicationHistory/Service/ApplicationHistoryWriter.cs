using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	/// <summary>
	/// It is the interface of writing the application history, exposing the methods
	/// of writing
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationStartData
	/// 	"/>
	/// ,
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationFinishData
	/// 	"/>
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationAttemptStartData
	/// 	"/>
	/// ,
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationAttemptFinishData
	/// 	"/>
	/// ,
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ContainerStartData
	/// 	"/>
	/// and
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ContainerFinishData
	/// 	"/>
	/// .
	/// </summary>
	public interface ApplicationHistoryWriter
	{
		/// <summary>
		/// This method writes the information of <code>RMApp</code> that is available
		/// when it starts.
		/// </summary>
		/// <param name="appStart">
		/// the record of the information of <code>RMApp</code> that is
		/// available when it starts
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		void ApplicationStarted(ApplicationStartData appStart);

		/// <summary>
		/// This method writes the information of <code>RMApp</code> that is available
		/// when it finishes.
		/// </summary>
		/// <param name="appFinish">
		/// the record of the information of <code>RMApp</code> that is
		/// available when it finishes
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		void ApplicationFinished(ApplicationFinishData appFinish);

		/// <summary>
		/// This method writes the information of <code>RMAppAttempt</code> that is
		/// available when it starts.
		/// </summary>
		/// <param name="appAttemptStart">
		/// the record of the information of <code>RMAppAttempt</code> that is
		/// available when it starts
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		void ApplicationAttemptStarted(ApplicationAttemptStartData appAttemptStart);

		/// <summary>
		/// This method writes the information of <code>RMAppAttempt</code> that is
		/// available when it finishes.
		/// </summary>
		/// <param name="appAttemptFinish">
		/// the record of the information of <code>RMAppAttempt</code> that is
		/// available when it finishes
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		void ApplicationAttemptFinished(ApplicationAttemptFinishData appAttemptFinish);

		/// <summary>
		/// This method writes the information of <code>RMContainer</code> that is
		/// available when it starts.
		/// </summary>
		/// <param name="containerStart">
		/// the record of the information of <code>RMContainer</code> that is
		/// available when it starts
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		void ContainerStarted(ContainerStartData containerStart);

		/// <summary>
		/// This method writes the information of <code>RMContainer</code> that is
		/// available when it finishes.
		/// </summary>
		/// <param name="containerFinish">
		/// the record of the information of <code>RMContainer</code> that is
		/// available when it finishes
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		void ContainerFinished(ContainerFinishData containerFinish);
	}
}
