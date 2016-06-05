using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Tools;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api
{
	public interface HSAdminProtocol : GetUserMappingsProtocol, RefreshUserMappingsProtocol
		, HSAdminRefreshProtocol
	{
	}
}
