using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	/// <summary>This class is the abstract of the storage of the application history data.
	/// 	</summary>
	/// <remarks>
	/// This class is the abstract of the storage of the application history data. It
	/// is a
	/// <see cref="Org.Apache.Hadoop.Service.Service"/>
	/// , such that the implementation of this class can make use
	/// of the service life cycle to initialize and cleanup the storage. Users can
	/// access the storage via
	/// <see cref="ApplicationHistoryReader"/>
	/// and
	/// <see cref="ApplicationHistoryWriter"/>
	/// interfaces.
	/// </remarks>
	public interface ApplicationHistoryStore : Org.Apache.Hadoop.Service.Service, ApplicationHistoryReader
		, ApplicationHistoryWriter
	{
	}
}
