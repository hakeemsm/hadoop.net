using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <p><code>URL</code> represents a serializable
	/// <see cref="System.Uri"/>
	/// .</p>
	/// </summary>
	public abstract class URL
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static URL NewInstance(string scheme, string host, int port, string file)
		{
			URL url = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<URL>();
			url.SetScheme(scheme);
			url.SetHost(host);
			url.SetPort(port);
			url.SetFile(file);
			return url;
		}

		/// <summary>Get the scheme of the URL.</summary>
		/// <returns>scheme of the URL</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetScheme();

		/// <summary>Set the scheme of the URL</summary>
		/// <param name="scheme">scheme of the URL</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetScheme(string scheme);

		/// <summary>Get the user info of the URL.</summary>
		/// <returns>user info of the URL</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetUserInfo();

		/// <summary>Set the user info of the URL.</summary>
		/// <param name="userInfo">user info of the URL</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetUserInfo(string userInfo);

		/// <summary>Get the host of the URL.</summary>
		/// <returns>host of the URL</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetHost();

		/// <summary>Set the host of the URL.</summary>
		/// <param name="host">host of the URL</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetHost(string host);

		/// <summary>Get the port of the URL.</summary>
		/// <returns>port of the URL</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract int GetPort();

		/// <summary>Set the port of the URL</summary>
		/// <param name="port">port of the URL</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetPort(int port);

		/// <summary>Get the file of the URL.</summary>
		/// <returns>file of the URL</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetFile();

		/// <summary>Set the file of the URL.</summary>
		/// <param name="file">file of the URL</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetFile(string file);
	}
}
