using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api
{
	public interface HSAdminRefreshProtocol
	{
		/// <summary>Refresh admin acls.</summary>
		/// <exception cref="System.IO.IOException"/>
		void RefreshAdminAcls();

		/// <summary>Refresh loaded job cache</summary>
		/// <exception cref="System.IO.IOException"/>
		void RefreshLoadedJobCache();

		/// <summary>Refresh job retention settings.</summary>
		/// <exception cref="System.IO.IOException"/>
		void RefreshJobRetentionSettings();

		/// <summary>Refresh log retention settings.</summary>
		/// <exception cref="System.IO.IOException"/>
		void RefreshLogRetentionSettings();
	}
}
