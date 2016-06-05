

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Used to registry custom methods to refresh at runtime.</summary>
	public interface RefreshHandler
	{
		/// <summary>Implement this method to accept refresh requests from the administrator.
		/// 	</summary>
		/// <param name="identifier">is the identifier you registered earlier</param>
		/// <param name="args">contains a list of string args from the administrator</param>
		/// <exception cref="System.Exception">as a shorthand for a RefreshResponse(-1, message)
		/// 	</exception>
		/// <returns>a RefreshResponse</returns>
		RefreshResponse HandleRefresh(string identifier, string[] args);
	}
}
